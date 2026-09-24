// Package pipeline 把配置、输入源、规则集内核和产物层接起来。
//
// 只有一条硬规则：**每个规则集是独立单元**。任何一个规则集失败都不影响其他的，
// 退出码只看总产出。旧实现是分阶段整体判定，某一类源全挂就直接返回失败，
// 连已经下载好的几十个规则集都不再编译，摘要也不会打印。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"

	"github.com/Sentsuki/srs-box/internal/config"
	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/report"
	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source"
	"github.com/Sentsuki/srs-box/internal/source/geosite"
	"golang.org/x/sync/errgroup"
)

// dropValuesContaining 是硬编码的水印过滤。
//
// 不值得开成配置项；但丢弃条数会出现在摘要里 —— 旧实现把这条藏在处理逻辑内部，
// 静默丢规则且没有任何提示。
//
// 用 skk.moe 而不是更精确的 ruleset.skk.moe：skk 的水印有两种形态，后者只盖住
// 一种，另一种（7h15.ru1353t.1s.m4d3.by.5ukk4w.skk.moe，leetspeak 的
// "this ruleset is made by sukkaw"）会漏进几乎每个产物。
//
// 代价是**连带删掉 sukka 自己的真实服务域名**（pic.skk.moe、img.skk.moe、
// latency-test.skk.moe、speedtest-net-servers.cdn.skk.moe 等）。这是刻意的取舍：
// 宁可少几条自用域名，也不要每个产物里都挂着别人的水印。
var dropValuesContaining = []string{"skk.moe"}

// Options 是一次运行的参数。
type Options struct {
	Only     []string
	DryRun   bool
	Progress func(format string, args ...any)
}

// Run 跑完一次完整流程，返回结果。
//
// 只有配置层面的问题（--only 指向不存在的规则集）才返回 error；源挂了、
// 解析失败、写盘失败都记进各自的 Result。
func Run(ctx context.Context, cfg *config.Config, opts Options) (*report.Run, error) {
	sources := newRegistry(cfg)

	// 顺序是被依赖关系定死的：geosite 先就位 → 展开 bulk → 选择 → 抓其余输入。
	//
	// bulk 生成的名字来自 dlc.dat 里的数据，而 --only 要能点名这些名字，
	// 所以选择只能发生在展开之后。反过来把选择放在最前面的话，--only 对
	// geosite- 开头的名字一律报"没有这些规则集"。
	sources.prepareGeosite(ctx, cfg, opts.Only, opts.Progress)
	bulk, bulkComplete := expandBulk(cfg, sources, opts)

	// 候选名单 = 配置里写的 + bulk 展开的，--only 对两者一视同仁。
	//
	// 名单**无条件**包含 bulk 的名字，哪怕这次一个都不构建：configured 是
	// "发布方眼里该存在的全部产物"，不是"这次跑了哪些"。漏掉一个名字等于告诉
	// 发布方那是孤儿 —— --only 跑一次再 publish，分支上上千个 geosite-*.srs
	// 会被当孤儿全部删掉，而摘要上看不出任何异常。本次不构建的那些会落成
	// skipped，发布方照旧保留上一次的文件。
	candidates := make([]*config.Ruleset, 0, len(cfg.Rulesets)+len(bulk))
	candidates = append(candidates, cfg.Rulesets...)
	candidates = append(candidates, bulk...)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })

	configured := make([]string, 0, len(candidates))
	for _, r := range candidates {
		configured = append(configured, r.Name)
	}

	selected, err := selectRulesets(candidates, opts.Only)
	if err != nil {
		return nil, err
	}
	selectedSet := map[string]bool{}
	for _, r := range selected {
		selectedSet[r.Name] = true
	}

	// 抓取只按选中的来 —— --only 不该把没选的地址也抓一遍。
	if err := sources.prepareInputs(ctx, selected, opts.Progress); err != nil {
		return nil, err
	}

	results := build(ctx, cfg, selected, sources, opts)

	run := &report.Run{
		Results:       results,
		Configured:    configured,
		Selected:      selectedSet,
		Authoritative: bulkComplete,
	}
	return run, nil
}

// expandBulk 把 geosite.bulk 展开成一批规则集。
//
// 返回的 complete 为 false 表示**规则集名单不完整** —— 发布方据此跳过孤儿清理。
// 只有两种情况会不完整：配了 bulk 但 dlc.dat 没加载成功，或者展开本身出错。
// 那时我们根本不知道 geosite- 前缀下本该有哪些名字，一旦按不完整的名单清孤儿，
// 一次 GitHub 抖动就会删光整个前缀。
func expandBulk(cfg *config.Config, sources *registry, opts Options) (out []*config.Ruleset, complete bool) {
	if cfg.Geosite == nil || cfg.Geosite.Bulk == nil {
		return nil, true // 没配 bulk，名单全部来自配置，天然完整
	}
	b := cfg.Geosite.Bulk
	if sources.geositeDown {
		return nil, false
	}
	codes, err := sources.geosite.ExpandBulk(b.Include, b.Exclude)
	if err != nil {
		if opts.Progress != nil {
			opts.Progress("geosite.bulk 展开失败: %v —— 本次跳过孤儿清理", err)
		}
		return nil, false
	}
	prefix := "geosite-"
	if b.Prefix != nil {
		prefix = *b.Prefix
	}
	// 撞名必须报错而不是静默覆盖：两条配置指向同一个输出文件名，
	// 少掉一个产出而摘要上完全看不出来，正是上一次重构要消灭的那类问题。
	taken := make(map[string]struct{}, len(cfg.Rulesets))
	for _, r := range cfg.Rulesets {
		taken[r.Name] = struct{}{}
	}

	out = make([]*config.Ruleset, 0, len(codes))
	var skipped, clashed []string
	for _, code := range codes {
		name := prefix + code
		if !config.ValidName(name) {
			skipped = append(skipped, name)
			continue
		}
		if _, dup := taken[name]; dup {
			clashed = append(clashed, name)
			continue
		}
		taken[name] = struct{}{}
		spec := &config.Ruleset{Name: name}
		spec.Geosite = []string{code}
		out = append(out, spec)
	}
	if len(clashed) > 0 {
		if opts.Progress != nil {
			opts.Progress("geosite.bulk 与配置里的规则集撞名，已跳过: %s —— "+
				"想让批量避开就写进 bulk.exclude", strings.Join(clashed, ", "))
		}
		// 撞名意味着名单不是我们以为的那份，孤儿清理不能照它来。
		return out, false
	}
	if len(skipped) > 0 && opts.Progress != nil {
		opts.Progress("geosite.bulk 跳过 %d 个名字非法的 code（如 %s）",
			len(skipped), skipped[0])
	}
	if opts.Progress != nil {
		opts.Progress("geosite.bulk 展开出 %d 个规则集", len(out))
	}
	return out, len(skipped) == 0
}

// selectRulesets 从候选名单里挑出 --only 点名的那些。
//
// 候选里既有配置写死的，也有 bulk 展开出来的 —— 两者在这里没有区别，
// --only geosite-cn 和 --only block-ads 一样能用。
func selectRulesets(candidates []*config.Ruleset, only []string) ([]*config.Ruleset, error) {
	if len(only) == 0 {
		return candidates, nil
	}
	want := map[string]bool{}
	for _, name := range only {
		want[name] = true
	}
	var chosen []*config.Ruleset
	for _, r := range candidates {
		if want[r.Name] {
			chosen = append(chosen, r)
			delete(want, r.Name)
		}
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for name := range want {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("没有这些规则集: %v —— 配置里的名字和 geosite.bulk "+
			"展开出来的名字都找过了（bulk 展开失败时它的名字自然也点不到）", missing)
	}
	return chosen, nil
}

// ---------------- 输入源 ----------------

type registry struct {
	http    *source.HTTP
	file    *source.File
	inline  *source.Inline
	geosite *geosite.Source
	// geositeDown 记录 geosite 整类输入是否不可用。它决定规则集名单还算不算完整。
	geositeDown bool
}

func newRegistry(cfg *config.Config) *registry {
	return &registry{
		http: source.NewHTTP(source.HTTPOptions{
			Concurrency: cfg.Fetch.Concurrency,
			Timeout:     cfg.Fetch.Timeout.Std(),
			Retries:     cfg.Fetch.Retries,
		}),
		file:   source.NewFile("."),
		inline: source.NewInline(),
		geosite: geosite.New(geosite.Options{
			Repo:      geositeRepo(cfg),
			File:      geositeFile(cfg),
			Normalize: geositeNormalize(cfg),
			Timeout:   cfg.Fetch.Timeout.Std() * 4, // dlc.dat 有几 MB，比单个列表宽松些
		}),
	}
}

func geositeRepo(cfg *config.Config) string {
	if cfg.Geosite != nil {
		return cfg.Geosite.Repo
	}
	return ""
}

func geositeFile(cfg *config.Config) string {
	if cfg.Geosite != nil {
		return cfg.Geosite.File
	}
	return ""
}

func geositeNormalize(cfg *config.Config) geosite.Normalize {
	if cfg.Geosite != nil {
		return geosite.Normalize(cfg.Geosite.Normalize)
	}
	return ""
}

// prepareGeosite 让 geosite 这一类输入就位。它必须先于 bulk 展开和 --only 选择。
//
// 这一步跑的时候选择还没做（选择要等 bulk 展开，展开又要等这一步），所以它自己
// 按 --only 过一遍配置里的名字 —— 否则 --only 一个纯 HTTP 的规则集也会白下
// 一份 2MB 的 dlc.dat。bulk 生成的名字在这里点不到，也不需要：配了 bulk 就
// 一定加载。
//
// 配了 bulk 一定加载，哪怕没有任何规则集引用 geosite code：规则集**名单本身**
// 来自 dlc.dat。以前这里只看"引用了哪些 code"，于是一份只有 bulk、没有显式
// geosite 输入的配置，bulk 永远展不开。
func (r *registry) prepareGeosite(ctx context.Context, cfg *config.Config, only []string, progress func(string, ...any)) {
	want := map[string]bool{}
	for _, name := range only {
		want[name] = true
	}
	var codes []string
	for _, spec := range cfg.Rulesets {
		if len(only) > 0 && !want[spec.Name] {
			continue
		}
		codes = append(codes, spec.Geosite...)
		if spec.Exclude != nil {
			codes = append(codes, spec.Exclude.Geosite...)
		}
	}
	needBulk := cfg.Geosite != nil && cfg.Geosite.Bulk != nil
	if len(codes) == 0 && !needBulk {
		return
	}

	var err error
	if len(codes) > 0 {
		err = r.geosite.Prepare(ctx, codes)
	} else {
		err = r.geosite.Load(ctx)
	}
	if err != nil {
		// dlc.dat 拿不到不该让整次运行失败 —— 引用它的规则集各自记账，
		// 其余几十个照常产出。这正是"每个规则集是独立单元"的延伸。
		r.geositeDown = true
		if progress != nil {
			progress("%v —— 引用 geosite 的规则集本次不会更新", err)
		}
	}
}

// prepareInputs 把选中规则集的 URL 与本地文件一次性备齐。
//
// 收齐之后再准备，是"同一个地址被多个规则集引用只抓一次"能成立的原因 ——
// 换成边构建边抓就做不到了。geosite 不在这里，它已经在 prepareGeosite 就位。
func (r *registry) prepareInputs(ctx context.Context, specs []*config.Ruleset, progress func(string, ...any)) error {
	var urls, files []string
	collect := func(in config.Inputs) {
		urls = append(urls, in.Sources...)
		files = append(files, in.Files...)
	}
	for _, spec := range specs {
		collect(spec.Inputs)
		if spec.Exclude != nil {
			collect(*spec.Exclude)
		}
	}

	if progress != nil && len(urls) > 0 {
		progress("抓取 %d 个地址（并发上限已设）", len(dedupe(urls)))
	}
	if err := r.http.Prepare(ctx, urls); err != nil {
		return err
	}
	return r.file.Prepare(ctx, files)
}

func dedupe(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// feedAll 把一组输入喂进规则集，返回失败的输入描述。
//
// 部分输入失败照常构建，全部失败才算这个规则集没救 —— 与"每个规则集是独立单元"
// 同一条原则的延伸：一个源挂了不该让整个规则集消失。
func (r *registry) feedAll(in config.Inputs, opts source.Options, set *ruleset.RuleSet) (failed []string, attempted int) {
	feed := func(src source.Source, keys []string) {
		for _, key := range keys {
			attempted++
			if err := src.Feed(key, opts, set); err != nil {
				failed = append(failed, err.Error())
			}
		}
	}
	feed(r.http, in.Sources)
	feed(r.file, in.Files)
	feed(r.inline, in.Inline)
	feed(r.geosite, in.Geosite)
	return failed, attempted
}

// ---------------- 构建 ----------------

func build(ctx context.Context, cfg *config.Config, specs []*config.Ruleset, sources *registry, opts Options) []*report.Result {
	results := make([]*report.Result, len(specs))

	// 构建阶段也并发：几十万条 CIDR 的聚合和收敛是 CPU 密集的，Python 版
	// 抓完之后是串行的。限流按核数，不按规则集数 —— 上千个 goroutine 同时
	// 持有大 map 会把内存推高。
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(runtime.GOMAXPROCS(0))
	var mu sync.Mutex

	for i, spec := range specs {
		group.Go(func() error {
			res := buildOne(gctx, cfg, spec, sources, opts)
			mu.Lock()
			results[i] = res
			mu.Unlock()
			return nil
		})
	}
	_ = group.Wait()
	return results
}

// panicProbe 是测试注入点。panic 隔离这件事只有真的 panic 一次才验得了，
// 而生产路径上没有任何能稳定触发 panic 的输入。nil 时只是一次判空。
var panicProbe func(name string)

func buildOne(ctx context.Context, cfg *config.Config, spec *config.Ruleset, sources *registry, opts Options) (res *report.Result) {
	res = &report.Result{Name: spec.Name}
	set := ruleset.New(spec.Name)
	feedOpts := source.Options{Format: spec.ParsedFormat()}

	// "每个规则集是独立单元"这条原则在 panic 面前也得成立。
	//
	// 没有这道防线的话，任意一个规则集里的 panic（sing-box 的 matcher 对空串
	// 直接取 domain[0]、srs 编码器遇到没想到的输入）会带走整个进程：几十个
	// 已经算好的规则集一个都不会落盘，摘要也不会打印 —— 与"某一类源全挂就
	// 整体失败"是同一种事故，而那正是这一层重构要消灭的东西。
	//
	// 栈打到 stderr（诊断），摘要和报告里只留一句话（结果）—— 日志级别只该
	// 影响诊断，不该影响结果。
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		fmt.Fprintf(os.Stderr, "规则集 %s 内部错误: %v\n%s\n", spec.Name, p, debug.Stack())
		res.OK = false
		res.Err = fmt.Errorf("内部错误（已隔离，其余规则集不受影响）: %v —— 这是 bug，栈见 stderr", p)
		res.Diag = set.Diag
	}()
	if panicProbe != nil {
		panicProbe(spec.Name)
	}

	failed, attempted := sources.feedAll(spec.Inputs, feedOpts, set)
	res.FailedSources = failed
	if len(failed) == attempted {
		res.Err = errors.New("所有输入都不可用")
		res.Diag = set.Diag
		return res
	}

	// 变换顺序是固定的：差集 → 收敛 → 水印过滤 → CIDR 聚合。
	// 差集必须在收敛之前 —— 收敛会把等价写法压成一种，压完之后待删项可能
	// 已经变成了另一种形态，再做差就删不掉了。
	if spec.Exclude != nil {
		drop := ruleset.New(spec.Name + " (exclude)")
		dropFailed, dropAttempted := sources.feedAll(*spec.Exclude, feedOpts, drop)
		if len(dropFailed) == dropAttempted && dropAttempted > 0 {
			// exclude 全挂时**不能**当没事发生：那会让本该删掉的东西留在产物里。
			res.Err = fmt.Errorf("exclude 的输入全部不可用，拒绝产出未经排除的规则集")
			res.FailedSources = append(res.FailedSources, dropFailed...)
			res.Diag = set.Diag
			return res
		}
		res.FailedSources = append(res.FailedSources, dropFailed...)
		set.Subtract(drop)
	}
	if spec.CollapseEnabled() {
		set.Collapse()
	}
	set.DropValuesContaining(dropValuesContaining)
	if spec.Aggregate {
		set.AggregateCIDR()
	}

	res.Diag = set.Diag
	res.Rules = set.Total()
	if set.Empty() {
		res.Err = errors.New("解析后没有任何规则")
		return res
	}
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}

	if err := write(cfg, spec, set, res, opts); err != nil {
		res.Err = err
		return res
	}
	res.OK = true
	return res
}

func write(cfg *config.Config, spec *config.Ruleset, set *ruleset.RuleSet, res *report.Result, opts Options) error {
	if opts.DryRun {
		return nil
	}
	if a := cfg.Output.JSON; a != nil {
		path := filepath.Join(a.Dir, spec.Name+".json")
		if _, err := emit.JSONFile(path, set, cfg.RulesetVersion); err != nil {
			return fmt.Errorf("写 JSON 失败: %w", err)
		}
		res.JSONPath = path
	}
	if a := cfg.Output.SRS; a != nil {
		path := filepath.Join(a.Dir, spec.Name+".srs")
		size, err := emit.SRSFile(path, set, cfg.RulesetVersion)
		if err != nil {
			// 编译失败时**保留**本次写出的 JSON：那正是 sing-box 拒绝的输入，
			// 是唯一的排错依据。
			return fmt.Errorf("写 .srs 失败: %w", err)
		}
		res.SRSPath = path
		res.SRSSize = size
	}
	return nil
}

// PruneStale 删掉与本次运行不符的旧产物。
//
// 上一次成功、这一次失败的规则集会在输出目录里留下上次的文件，而摘要标着 ✗
// —— 看目录的人无从分辨哪些是新的。发布流程靠运行报告区分（失败项保留上一次
// **已发布**的版本），但本地输出目录应当只反映本次运行。
//
// 只处理本次选中的规则集，--only 不会误删其余产物。
func PruneStale(cfg *config.Config, run *report.Run) {
	for _, res := range run.Results {
		if res.OK {
			continue
		}
		if res.JSONPath == "" && cfg.Output.JSON != nil {
			_ = removeIfExists(filepath.Join(cfg.Output.JSON.Dir, res.Name+".json"))
		}
		if res.SRSPath == "" && cfg.Output.SRS != nil {
			_ = removeIfExists(filepath.Join(cfg.Output.SRS.Dir, res.Name+".srs"))
		}
	}
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}
