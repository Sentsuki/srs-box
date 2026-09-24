// Package geosite 把 v2fly 的 dlc.dat 变成规则集里的值。
//
// 它是一种**输入源**，和 HTTP、本地文件、内联规则平级 —— 不是第二条流水线。
// 一个规则集可以同时引用 URL 和 geosite code，两种混写，所以没有 type 判别
// 字段，也不需要 merge provider 和拓扑排序。
package geosite

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source"
)

// Normalize 决定取值怎么归一。
type Normalize string

const (
	// NormalizeLenient 走归一管道，非法值丢弃并计入诊断。默认。
	NormalizeLenient Normalize = "lenient"
	// NormalizeStrict 任何非法值都让该规则集失败。
	NormalizeStrict Normalize = "strict"
)

// Options 是 geosite 源的设置。
type Options struct {
	// Repo 是 dlc.dat 的上游，默认 v2fly/domain-list-community。
	Repo string
	// File 指定本地 dlc.dat，非空时不走网络。
	File string
	// Normalize 默认 lenient。
	Normalize Normalize
	// Timeout 是下载超时。
	Timeout time.Duration
	// Progress 可为 nil。
	Progress func(format string, args ...any)
}

// Source 是 geosite 输入源。
type Source struct {
	opts   Options
	client *http.Client

	once    sync.Once
	codes   codeMap
	odd     []string // 与自己名字矛盾的属性 code
	loadErr error
}

// New 建一个 geosite 源。dlc.dat 在第一次 Prepare 时才下载。
func New(opts Options) *Source {
	if opts.Normalize == "" {
		opts.Normalize = NormalizeLenient
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Minute
	}
	return &Source{
		opts:   opts,
		client: &http.Client{Timeout: opts.Timeout},
	}
}

func (s *Source) Kind() string { return "geosite" }

// Prepare 下载并解析 dlc.dat，然后校验全部 key 都能解析出 code。
//
// 只做一次：dlc.dat 是这一类输入的共享状态，被任意多个规则集引用都只拉一次 ——
// 和 URL 去重同理，是结构上保证的，不靠调用方自觉。
func (s *Source) Prepare(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	s.once.Do(func() { s.load(ctx) })
	if s.loadErr != nil {
		// dlc.dat 拿不到或校验失败是**整类输入**不可用，引用它的规则集各自记账。
		return s.loadErr
	}
	if len(s.odd) > 0 && s.opts.Progress != nil {
		s.opts.Progress("geosite: 发现 %d 个与自己名字矛盾的属性 code（如 %s）—— "+
			"未自动排除，需要的话写进 exclude", len(s.odd), strings.Join(s.odd[:min(3, len(s.odd))], ", "))
	}
	return nil
}

func (s *Source) load(ctx context.Context) {
	var data []byte
	var err error
	if s.opts.File != "" {
		if s.opts.Progress != nil {
			s.opts.Progress("geosite: 读本地 %s", s.opts.File)
		}
		data, err = readLocalDLC(s.opts.File)
	} else {
		if s.opts.Progress != nil {
			s.opts.Progress("geosite: 下载 dlc.dat 并校验 sha256")
		}
		data, err = fetchDLC(ctx, s.client, s.opts.Repo)
	}
	if err != nil {
		s.loadErr = fmt.Errorf("geosite: %w", err)
		return
	}
	sites, err := decodeDLC(data)
	if err != nil {
		s.loadErr = fmt.Errorf("geosite: 解析 dlc.dat 失败: %w", err)
		return
	}
	s.codes, s.odd = build(sites)
	if s.opts.Progress != nil {
		s.opts.Progress("geosite: %d 个 code（含属性变体）", len(s.codes))
	}
}

// Feed 把一个 code 或 glob 的内容并进规则集。
func (s *Source) Feed(key string, _ source.Options, into *ruleset.RuleSet) error {
	if s.loadErr != nil {
		return s.loadErr
	}
	if s.codes == nil {
		return fmt.Errorf("geosite %s: dlc.dat 尚未加载", key)
	}
	matched, err := s.codes.resolve(key)
	if err != nil {
		return fmt.Errorf("geosite %s: %w", key, err)
	}
	for _, code := range matched {
		for _, it := range s.codes[code] {
			if err := s.commit(it, into); err != nil {
				return fmt.Errorf("geosite %s: %w", code, err)
			}
		}
	}
	return nil
}

func (s *Source) commit(it item, into *ruleset.RuleSet) error {
	switch s.opts.Normalize {
	case NormalizeStrict:
		if err := into.Add(it.field, it.value); err != nil {
			return err
		}
	default:
		into.AddLenient(it.field, it.value)
	}
	return nil
}

// Codes 返回全部可寻址的 code，已排序。供 bulk 展开和诊断使用。
func (s *Source) Codes() []string {
	out := make([]string, 0, len(s.codes))
	for code := range s.codes {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// Contradictions 返回与自己名字矛盾的属性 code。
func (s *Source) Contradictions() []string { return s.odd }

// ExpandBulk 按 include / exclude 展开出一批 code。
//
// 这是唯一的"一条配置生成多个规则集"的口子。它待在这里而不是 rulesets 里，
// 因为它生成的名字来自运行时的数据（1540 个 code 手写不出来）。
func (s *Source) ExpandBulk(include, exclude []string) ([]string, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	if len(include) == 0 {
		return nil, fmt.Errorf("bulk.include 不能为空 —— 想要全量就显式写 [\"*\"]")
	}
	chosen := map[string]struct{}{}
	for _, pattern := range include {
		matched, err := s.codes.resolve(pattern)
		if err != nil {
			return nil, err
		}
		for _, code := range matched {
			chosen[code] = struct{}{}
		}
	}
	for _, pattern := range exclude {
		lowered := strings.ToLower(strings.TrimSpace(pattern))
		for code := range chosen {
			ok, err := matchGlob(lowered, code)
			if err != nil {
				return nil, fmt.Errorf("bulk.exclude 里的 glob %q 非法: %w", pattern, err)
			}
			if ok {
				delete(chosen, code)
			}
		}
	}
	if len(chosen) == 0 {
		return nil, fmt.Errorf("bulk 展开后一个 code 都不剩 —— include 与 exclude 互相抵消了")
	}
	out := make([]string, 0, len(chosen))
	for code := range chosen {
		out = append(out, code)
	}
	sort.Strings(out)
	return out, nil
}

// matchGlob 用 path.Match 匹配 code 名。
//
// code 名里没有 /，所以 path.Match 的 * 不跨分隔符这条限制在这里没有影响。
// 注意 ! 是**字面字符**不是取反：geolocation-!cn 就是个普通 code 名，
// glob 没有取反操作符 —— 要排除用 exclude。
func matchGlob(pattern, code string) (bool, error) {
	return path.Match(pattern, code)
}
