package ruleset

import (
	"net/netip"
	"strings"

	"go4.org/netipx"
)

// 变换的执行顺序是固定的，由调用方（pipeline）保证：
//
//	Subtract → Collapse → DropValuesContaining → AggregateCIDR
//
// 差集必须在收敛之前：收敛会把 {domain:a.com, suffix:.a.com} 压成无点的
// suffix:a.com，"只排掉 apex"这个本来能表达的操作就此消失。差集怎么对上两种
// 编码而又不依赖先收敛主集合，见 subtract.go。

// AggregateCIDR 合并相邻或包含的网段，返回减少的条数。
//
// netipx 自己会按地址族分开处理，不必像 Python 版那样手工分 v4/v6 两组。
func (s *RuleSet) AggregateCIDR() int {
	total := 0
	for _, f := range [...]Field{FieldIPCIDR, FieldSourceIPCIDR} {
		values := s.Values(f)
		if len(values) == 0 {
			continue
		}
		var b netipx.IPSetBuilder
		for _, v := range values {
			prefix, err := netip.ParsePrefix(v)
			if err != nil {
				// 进得来就一定归一过，这里不可能失败；真失败了宁可原样留着。
				continue
			}
			b.AddPrefix(prefix)
		}
		set, err := b.IPSet()
		if err != nil {
			continue
		}
		merged := set.Prefixes()
		if len(merged) >= len(values) {
			continue
		}
		next := make(map[string]struct{}, len(merged))
		for _, prefix := range merged {
			next[prefix.String()] = struct{}{}
		}
		total += len(values) - len(next)
		s.strs[f] = next
	}
	s.Diag.Aggregated += total
	return total
}

// DropValuesContaining 删除含指定子串的规则值（大小写不敏感），返回删除条数。
//
// 透传规则同样受这条过滤约束 —— 只扫 plain 的话，带 invert 或含未知字段的
// 规则里的水印域名能原样漏到产物里。
func (s *RuleSet) DropValuesContaining(needles []string) int {
	lowered := make([]string, 0, len(needles))
	for _, n := range needles {
		if n != "" {
			lowered = append(lowered, strings.ToLower(n))
		}
	}
	if len(lowered) == 0 {
		return 0
	}

	removed := 0
	for _, m := range s.strs {
		for v := range m {
			if hits(v, lowered) {
				delete(m, v)
				removed++
			}
		}
	}
	removed += s.dropVerbatim(lowered)
	s.Diag.Dropped += removed
	return removed
}

// dropVerbatim 对透传规则施加同一套过滤。
//
// 一律**整条**丢弃，不做逐值裁剪。理由：从 logical 规则的 AND/OR 里摘掉一个
// 子项会改变整条规则的语义；而单条 default 规则里的字段按 match group 组合，
// 把某个字段删空等于去掉一个必须满足的条件，规则会匹配到比原来更多的流量。
// 整条丢弃对两种情形都安全。
func (s *RuleSet) dropVerbatim(needles []string) int {
	if len(s.verbatim) == 0 {
		return 0
	}
	kept := s.verbatim[:0:0]
	removed := 0
	for _, rule := range s.verbatim {
		if ruleHits(rule, needles) {
			removed++
			continue
		}
		kept = append(kept, rule)
	}
	if removed > 0 {
		s.verbatim = kept
		s.seen = make(map[string]struct{}, len(kept))
		for _, rule := range kept {
			if key, err := marshalRule(rule); err == nil {
				s.seen[string(key)] = struct{}{}
			}
		}
	}
	return removed
}

func hits(value string, needles []string) bool {
	lower := strings.ToLower(value)
	for _, n := range needles {
		if strings.Contains(lower, n) {
			return true
		}
	}
	return false
}
