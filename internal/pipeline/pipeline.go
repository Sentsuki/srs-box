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
	"sort"
	"sync"

	"github.com/Sentsuki/srs-box/internal/config"
	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/report"
	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source"
	"golang.org/x/sync/errgroup"
)

// dropValuesContaining 是硬编码的水印过滤。
//
// 只有一条，不值得开成配置项；但丢弃条数会出现在摘要里 —— 旧实现把这条藏在
// 处理逻辑内部，静默丢规则且没有任何提示。
var dropValuesContaining = []string{"ruleset.skk.moe"}

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
	selected, err := selectRulesets(cfg, opts.Only)
	if err != nil {
		return nil, err
	}

	configured := make([]string, 0, len(cfg.Rulesets))
	for _, r := range cfg.Rulesets {
		configured = append(configured, r.Name)
	}
	selectedSet := map[string]bool{}
	for _, r := range selected {
		selectedSet[r.Name] = true
	}

	sources := newRegistry(cfg)
	if err := sources.prepare(ctx, selected, opts.Progress); err != nil {
		return nil, err
	}

	results := build(ctx, cfg, selected, sources, opts)

	run := &report.Run{
		Results:    results,
		Configured: configured,
		Selected:   selectedSet,
		// 目前唯一会让名单不完整的是 geosite.bulk 的通配符，而 bulk 还没实现。
		Authoritative: cfg.Geosite == nil || cfg.Geosite.Bulk == nil,
	}
	return run, nil
}

func selectRulesets(cfg *config.Config, only []string) ([]*config.Ruleset, error) {
	if len(only) == 0 {
		return cfg.Rulesets, nil
	}
	want := map[string]bool{}
	for _, name := range only {
		want[name] = true
	}
	var chosen []*config.Ruleset
	for _, r := range cfg.Rulesets {
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
		return nil, fmt.Errorf("配置里没有这些规则集: %v", missing)
	}
	return chosen, nil
}

// ---------------- 输入源 ----------------

type registry struct {
	http   *source.HTTP
	file   *source.File
	inline *source.Inline
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
	}
}

// prepare 先把全部输入的 key 收齐，再让每种源各准备一次。
//
// 收齐之后再准备，是"同一个地址被多个规则集引用只抓一次"能成立的原因 ——
// 换成边构建边抓就做不到了。
func (r *registry) prepare(ctx context.Context, specs []*config.Ruleset, progress func(string, ...any)) error {
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
	// geosite 输入在第四阶段接进来。现在遇到就明确报错，不静默忽略。
	for _, code := range in.Geosite {
		attempted++
		failed = append(failed, "geosite "+code+": geosite 输入源尚未实现")
	}
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

func buildOne(ctx context.Context, cfg *config.Config, spec *config.Ruleset, sources *registry, opts Options) *report.Result {
	res := &report.Result{Name: spec.Name}
	set := ruleset.New(spec.Name)
	feedOpts := source.Options{Format: spec.ParsedFormat()}

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
