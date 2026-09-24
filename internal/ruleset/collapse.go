package ruleset

import (
	"strings"

	"github.com/sagernet/sing/common/domain"
)

// Collapse 把等价或被包含的写法压掉，返回删除的条数。
//
// 四条变换，全部**不改变匹配结果**，所以默认开启：
//
//  1. domain:x + domain_suffix:.x  →  domain_suffix:x
//  2. 有 domain_suffix:x           →  删 domain:x 与 domain_suffix:.x
//  3. 有后缀覆盖某个 domain/suffix  →  删掉被覆盖的那个
//  4. domain_keyword:k 覆盖某个值   →  删掉被覆盖的那个
//
// 为什么必须做：同一件事在不同上游有不同写法。Clash 的 DOMAIN-SUFFIX,example.com
// 落成无点形式，skk domainset 的 .example.com 落成带点形式，geosite 的 RootDomain
// 被上游拆成 domain + 带点 suffix 两条。三个源说同一件事，产出四个规则值，
// 而集合去重一个都抓不住 —— 不做收敛，"混合去重"名不副实。
//
// 不碰透传规则：跨 logical 规则谈"覆盖"没有良好定义。
func (s *RuleSet) Collapse() int {
	domains := s.strs[FieldDomain]
	suffixes := s.strs[FieldDomainSuffix]
	keywords := s.strs[FieldDomainKeyword]
	if len(domains) == 0 && len(suffixes) == 0 {
		return 0
	}

	removed := 0
	removed += collapseDottedPairs(domains, suffixes)
	removed += collapseSuffixes(suffixes)
	removed += collapseDomainsBySuffix(domains, suffixes)
	removed += collapseByKeyword(domains, suffixes, keywords)

	s.Diag.Collapsed += removed
	return removed
}

// collapseDottedPairs 是变换 1 与 2 里"合并"的那一半：
// domain:x 与 domain_suffix:.x 同时存在时，合成无点的 domain_suffix:x。
//
// 这不是我们的发明 —— sing-box 自己的 domain.Matcher.Dump 在反编译 .srs 时
// 做的就是这个变换。也就是说任何 .srs 走一遍 decompile 都会变成这个形态，
// 我们只是提前做掉。
//
// 净减一条：两个值变一个。
func collapseDottedPairs(domains, suffixes map[string]struct{}) int {
	if len(domains) == 0 || len(suffixes) == 0 {
		return 0
	}
	removed := 0
	for s := range suffixes {
		if !strings.HasPrefix(s, ".") {
			continue
		}
		bare := s[1:]
		if bare == "" {
			continue
		}
		if _, ok := domains[bare]; !ok {
			continue
		}
		delete(domains, bare)
		delete(suffixes, s)
		suffixes[bare] = struct{}{}
		removed++
	}
	return removed
}

// collapseSuffixes 删掉被同集合里另一条后缀覆盖的后缀（变换 2 的后一半 + 变换 3）。
//
// 不用 sing-box 的 matcher：把整个后缀集合塞进去之后每条都会命中自己，全集会被
// 删空。所以用显式谓词 suffixCovers，并由 TestSuffixCoversAgreesWithMatcher
// 逐对对着真 matcher 验证语义。
//
// 实现不是两层循环。照 suffixCovers 的定义展开，"存在 s 覆盖 t"只有两种可能：
//
//   - t 带前导点，且同 base 的无点形式也在集合里（无点形式包含自身，是超集）；
//   - t 的 base 在 label 边界上是集合里某条后缀 base 的**真子域**。
//
// 后者只需沿着点把 base 的祖先逐级列出来查表，层数就是 label 数（三五个），
// 于是整体是 O(n × label 数) 而不是 O(n²) —— geosite 那种几万条后缀的规则集
// 用两层循环会直接跑死。
//
// 一轮扫完即可，不必迭代到不动点：覆盖关系是传递的（set(t) ⊆ set(s) ⊆ set(r)），
// 所以拿原始集合判定的结果不会因为中途删掉了 s 而失效。
func collapseSuffixes(suffixes map[string]struct{}) int {
	if len(suffixes) < 2 {
		return 0
	}
	// bare 是无点后缀的 base，all 是全部后缀的 base（无点 + 带点）。
	bare := make(map[string]struct{}, len(suffixes))
	all := make(map[string]struct{}, len(suffixes))
	for s := range suffixes {
		base, includesSelf := suffixBase(s)
		if base == "" {
			continue
		}
		all[base] = struct{}{}
		if includesSelf {
			bare[base] = struct{}{}
		}
	}

	doomed := make([]string, 0, 16)
	for s := range suffixes {
		base, includesSelf := suffixBase(s)
		if base == "" {
			continue
		}
		if !includesSelf {
			if _, ok := bare[base]; ok {
				doomed = append(doomed, s)
				continue
			}
		}
		if hasAncestorIn(base, all) {
			doomed = append(doomed, s)
		}
	}
	for _, s := range doomed {
		delete(suffixes, s)
	}
	return len(doomed)
}

// hasAncestorIn 报告 base 的某个**真**祖先（按 label 边界）是否在集合里。
//
// 按 label 边界切，不是字符串后缀：HasSuffix("example.com", "ample.com") 成立，
// 但 example.com 并不是 ample.com 的子域 —— 这是这段代码最容易写错的地方。
func hasAncestorIn(base string, set map[string]struct{}) bool {
	rest := base
	for {
		i := strings.IndexByte(rest, '.')
		if i < 0 {
			return false
		}
		rest = rest[i+1:]
		if rest == "" {
			return false
		}
		if _, ok := set[rest]; ok {
			return true
		}
	}
}

// collapseDomainsBySuffix 删掉已被某条后缀覆盖的 domain（变换 3）。
//
// 这一步**用 sing-box 自己的 matcher 判覆盖**：domain 不在 matcher 里，
// 不存在自命中问题，于是"覆盖"的定义与 sing-box 的实际匹配行为逐字一致 ——
// 前导点那类坑不可能再踩第二次。
func collapseDomainsBySuffix(domains, suffixes map[string]struct{}) int {
	if len(domains) == 0 || len(suffixes) == 0 {
		return 0
	}
	// NewMatcher 对空串会 panic（它直接取 domain[0]）。归一层不放空值进来，
	// 这里再挡一道 —— 收敛是纯内部变换，不该有任何能让进程死掉的路径。
	list := nonEmpty(sortedKeys(suffixes))
	if len(list) == 0 {
		return 0
	}
	matcher := domain.NewMatcher(nil, list, false)
	removed := 0
	for d := range domains {
		if matcher.Match(d) {
			delete(domains, d)
			removed++
		}
	}
	return removed
}

// collapseByKeyword 删掉被某个 domain_keyword 覆盖的 domain / domain_suffix（变换 4）。
//
// keyword 匹配的是"名字里含这个子串"，匹配面严格更大，所以覆盖成立。
// 这条删得最狠：skk-reject 那类源带着大量关键词，会吃掉成片的域名条目。
//
// 判据对 domain 和 suffix 是同一个表达式 strings.Contains(v, k)：
//   - domain "a.com" 被匹配的只有它自己，含 k 即被覆盖。
//   - suffix "a.com"（无点）匹配 a.com 与 *.a.com，两者都含 "a.com" ⊇ k。
//   - suffix ".a.com"（带点）只匹配 *.a.com，它们都含 ".a.com" ⊇ k。
//
// 这个判据是保守的：k 若跨越了 base 的左边界（如 k=".ample" 之于 suffix "example.com"）
// 实际也覆盖，但这里判不出来。保守方向是"少删"，安全。
func collapseByKeyword(domains, suffixes, keywords map[string]struct{}) int {
	if len(keywords) == 0 {
		return 0
	}
	list := sortedKeys(keywords)
	removed := 0
	for _, m := range []map[string]struct{}{domains, suffixes} {
		for v := range m {
			for _, k := range list {
				if k != "" && strings.Contains(v, k) {
					delete(m, v)
					removed++
					break
				}
			}
		}
	}
	return removed
}

// suffixCovers 报告后缀 s 匹配的名字集合是否包含后缀 t 匹配的集合。
//
// sing-box 的语义（见 sing/common/domain/matcher.go 的 NewMatcher 与 has）：
//
//	domain_suffix "x"   匹配 x 本身及其全部子域
//	domain_suffix ".x"  只匹配子域，x 本身不命中
//
// 记 base(s) 为去掉前导点的部分、self(s) 为"是否包含自身"，则
//
//	set(s) = {base(s) | self(s)} ∪ {以 "."+base(s) 结尾的一切}
//
// s 覆盖 t 当且仅当 set(t) ⊆ set(s)。
func suffixCovers(s, t string) bool {
	bs, selfS := suffixBase(s)
	bt, selfT := suffixBase(t)
	if bs == "" || bt == "" {
		return false
	}
	if bs == bt {
		// 同一个 base：只有"s 不含自身而 t 含自身"这一种情况不成立。
		return selfS || !selfT
	}
	// t 的 base 落在 s 的子域里：set(t) 的每个成员都以 "."+bs 结尾。
	return strings.HasSuffix(bt, "."+bs)
}

func suffixBase(s string) (base string, includesSelf bool) {
	if strings.HasPrefix(s, ".") {
		return s[1:], false
	}
	return s, true
}

func nonEmpty(in []string) []string {
	out := in[:0:0]
	for _, v := range in {
		if v != "" && v != "." {
			out = append(out, v)
		}
	}
	return out
}
