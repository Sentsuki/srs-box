package ruleset

import (
	"net/netip"
	"strings"

	"github.com/sagernet/sing/common/domain"
	"go4.org/netipx"
)

// Subtract 从本集合里减去 other 匹配的一切，返回**整条删掉**的条数。
//
// 差集是语义的，不是字面的。字面差集（只删两边完全相同的值）在这个项目里注定
// 漏排除：同一个意图在不同上游是不同编码 —— Clash 的 +.example.com 落成无点
// domain_suffix，skk domainset 的 .example.com 落成带点形式，geosite 的
// RootDomain 落成 domain + 带点 suffix 两条。主集合和 exclude 来自不同的源时，
// 两边说的是同一件事却对不上字符串，本该删掉的域名于是原样留在产物里 ——
// 与"exclude 的源全挂"完全相同的后果，但没有任何信号。
//
// 判据就是收敛用的那一套覆盖谓词（见 collapse.go 的 suffixCovers），含义是
// "other 匹配的名字集合是否包含这条规则匹配的集合"。三种结局：
//
//   - 完全覆盖 → 删掉，计入 Diag.Subtracted
//   - 可以改窄 → 改写成更窄的等价规则，计入 Diag.Narrowed
//     （无点 suffix 去掉 apex 就是带点 suffix；CIDR 裁掉一段仍是一组 CIDR）
//   - 部分重叠 → headless rule 表达不出来，原样留下并计入 Diag.Unexpressible
//
// 第三类是这套设计的边界，必须报出来而不是假装没发生：domain_suffix 减去一个
// 更窄的 domain（a.com 减 x.a.com）写不成 headless rule，除非上 logical+invert，
// 而那会破坏整个拆字段去重的模型。
//
// 不改 other。判定前会在 other 的域名字段上做一次收敛 —— 收敛是匹配集合守恒的
// 变换，只统一表示不改语义 —— 所以用的是一份副本。
//
// **绝不能反过来收敛主集合**：{domain:a.com, suffix:.a.com} 收敛后是无点的
// suffix:a.com，"只排掉 apex"这个本来能表达的操作就此消失。这也是变换顺序里
// "差集必须在收敛之前"那条的真正理由。
func (s *RuleSet) Subtract(other *RuleSet) int {
	removed := 0
	removed += s.subtractDomains(other)
	removed += s.subtractCIDR(other)
	removed += s.subtractLiteral(other)
	removed += s.subtractVerbatim(other)
	s.Diag.Subtracted += removed
	return removed
}

// ---------------- 域名 ----------------

// subtractDomains 处理 domain / domain_suffix / domain_keyword 三个字段。
func (s *RuleSet) subtractDomains(other *RuleSet) int {
	view := collapsedDomainView(other)
	dropDomains := view.strs[FieldDomain]
	dropSuffixes := view.strs[FieldDomainSuffix]
	dropKeywords := sortedKeys(view.strs[FieldDomainKeyword])
	if len(dropDomains) == 0 && len(dropSuffixes) == 0 && len(dropKeywords) == 0 {
		return 0
	}

	// bare / all 与 collapseSuffixes 里那段是同一个建表：一次建好，之后每条
	// 后缀只查自己的祖先链（层数就是 label 数），整体 O(n × label 数)
	// 而不是 O(n×m) —— geosite 那种几万条后缀的 exclude 用两层循环会跑死。
	bare, all := suffixBaseSets(dropSuffixes)

	// domain 的覆盖判定交给 sing-box 自己的 matcher —— 主集合的 domain 不在
	// matcher 里，不存在收敛那边"每条都命中自己"的问题，于是判据与 sing-box
	// 的实际匹配行为逐字一致。
	var matcher *domain.Matcher
	if list := nonEmpty(sortedKeys(dropSuffixes)); len(list) > 0 {
		matcher = domain.NewMatcher(nil, list, false)
	}

	removed := 0

	// 1) domain：被 exclude 的 domain、suffix 或 keyword 命中即删。
	//    domain 只匹配它自己一个名字，所以"没被覆盖"等价于"完全不相交"，
	//    这个字段不可能产生表达不了的部分重叠。
	for v := range s.strs[FieldDomain] {
		_, exact := dropDomains[v]
		if exact || (matcher != nil && matcher.Match(v)) || coveredByKeyword(v, dropKeywords) {
			delete(s.strs[FieldDomain], v)
			removed++
		}
	}

	// 2) domain_suffix：覆盖即删；恰好只差一个 apex 或只差子域时改窄，
	//    而不是原样留着。
	suffixes := s.strs[FieldDomainSuffix]
	type rewrite struct {
		from string
		to   Field
		base string
	}
	var narrowed []rewrite
	for t := range suffixes {
		base, includesSelf := suffixBase(t)
		if base == "" {
			continue
		}
		if coveredBySuffixes(base, includesSelf, bare, all) || coveredByKeyword(t, dropKeywords) {
			delete(suffixes, t)
			removed++
			continue
		}
		if !includesSelf {
			continue
		}
		// 无点 suffix 是 {apex} ∪ {子域}，两半各自被排掉时剩下的都还写得出来：
		//   减 apex（exclude 有 domain:base）        → 带点 suffix
		//   减子域（exclude 有同 base 的带点 suffix）→ domain
		_, apexDropped := dropDomains[base]
		_, subdomainsDropped := all[base]
		switch {
		case apexDropped && subdomainsDropped:
			// 两半都被排掉 = 整条被覆盖。收敛过的 exclude 走不到这里
			// （domain:x 与 suffix:.x 会先被合成无点的 suffix:x），是道保险。
			delete(suffixes, t)
			removed++
		case subdomainsDropped:
			narrowed = append(narrowed, rewrite{from: t, to: FieldDomain, base: base})
		case apexDropped:
			narrowed = append(narrowed, rewrite{from: t, to: FieldDomainSuffix, base: base})
		}
	}
	for _, r := range narrowed {
		delete(suffixes, r.from)
		value := r.base
		if r.to == FieldDomainSuffix {
			value = "." + r.base
		}
		// 改窄之后的值要再过一遍关键词：带点形式含前导点，可能命中一个无点
		// 形式没命中的关键词。漏掉这一步就等于少排除了一条。
		if coveredByKeyword(value, dropKeywords) {
			removed++
			continue
		}
		s.addStr(r.to, value)
		s.Diag.Narrowed++
	}

	// 3) domain_keyword：只有更宽的 keyword 能覆盖 keyword ——
	//    k1 含 k2 时，k1 匹配的名字必然也含 k2，集合包含成立。
	for k := range s.strs[FieldDomainKeyword] {
		if coveredByKeyword(k, dropKeywords) {
			delete(s.strs[FieldDomainKeyword], k)
			removed++
		}
	}

	s.countUnexpressible(dropDomains, dropSuffixes)
	return removed
}

// collapsedDomainView 是 other 域名字段的收敛副本。
//
// 收敛是这套判据能对上两种编码的关键：exclude 里的 {domain:a.com,
// suffix:.a.com}（geosite RootDomain 的形态）收敛后就是无点的 suffix:a.com，
// 于是它能覆盖主集合里来自 +.a.com 的那条无点后缀。
func collapsedDomainView(other *RuleSet) *RuleSet {
	view := New(other.Name)
	for _, f := range [...]Field{FieldDomain, FieldDomainSuffix, FieldDomainKeyword} {
		m := other.strs[f]
		if len(m) == 0 {
			continue
		}
		cp := make(map[string]struct{}, len(m))
		for v := range m {
			cp[v] = struct{}{}
		}
		view.strs[f] = cp
	}
	view.Collapse()
	return view
}

// suffixBaseSets 把一组后缀拆成 bare（无点形式的 base，匹配集合含 apex 自身）
// 与 all（全部 base）。
func suffixBaseSets(suffixes map[string]struct{}) (bare, all map[string]struct{}) {
	bare = make(map[string]struct{}, len(suffixes))
	all = make(map[string]struct{}, len(suffixes))
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
	return bare, all
}

// coveredBySuffixes 报告 (base, includesSelf) 这条后缀是否被 bare/all 描述的
// 后缀集合完全覆盖。这是 suffixCovers 对着一整个集合展开后的形式。
func coveredBySuffixes(base string, includesSelf bool, bare, all map[string]struct{}) bool {
	if _, ok := bare[base]; ok {
		return true // 同 base 的无点后缀是超集（含 apex 与全部子域）
	}
	if !includesSelf {
		if _, ok := all[base]; ok {
			return true // 两边都带点、同 base，集合相等
		}
	}
	return hasAncestorIn(base, all)
}

// coveredByKeyword 报告值是否含任一关键词 —— 与 collapseByKeyword 同一个判据，
// 同样是保守的（判不出跨 base 左边界的命中），保守方向是少删。
func coveredByKeyword(value string, keywords []string) bool {
	for _, k := range keywords {
		if k != "" && strings.Contains(value, k) {
			return true
		}
	}
	return false
}

// countUnexpressible 数出"exclude 想排、但 headless rule 写不出差集"的条数。
//
// 只有一种形态会这样：本集合里一条后缀，exclude 里有个更窄的名字落在它下面
// （suffix:a.com 减 domain:x.a.com）。差集是"a.com 及其子域但不含 x.a.com"，
// 拆字段的规则表达不了。
//
// domain 不会落进这一类（没被覆盖就等于完全不相交）。domain_keyword 理论上会，
// 但关键词的匹配面是开放的，几乎任何 exclude 都与它部分重叠，报出来全是噪声，
// 所以不计 —— 这条限制写在文档里。
func (s *RuleSet) countUnexpressible(dropDomains, dropSuffixes map[string]struct{}) {
	suffixes := s.strs[FieldDomainSuffix]
	if len(suffixes) == 0 || (len(dropDomains) == 0 && len(dropSuffixes) == 0) {
		return
	}
	bases := make(map[string]int, len(suffixes))
	for t := range suffixes {
		if base, _ := suffixBase(t); base != "" {
			bases[base]++
		}
	}
	hit := map[string]struct{}{}
	// 沿 exclude 里那个名字的祖先链往上走，撞到本集合的后缀 base 就说明它是
	// 那条后缀的真子域。方向与 hasAncestorIn 相反，代价一样是 label 数。
	mark := func(name string) {
		rest := name
		for {
			i := strings.IndexByte(rest, '.')
			if i < 0 {
				return
			}
			rest = rest[i+1:]
			if rest == "" {
				return
			}
			if _, ok := bases[rest]; ok {
				hit[rest] = struct{}{}
			}
		}
	}
	for d := range dropDomains {
		mark(d)
	}
	for t := range dropSuffixes {
		if base, _ := suffixBase(t); base != "" {
			mark(base)
		}
	}
	n := 0
	for base := range hit {
		n += bases[base]
	}
	s.Diag.Unexpressible += n
}

// ---------------- CIDR ----------------

// subtractCIDR 对 ip_cidr / source_ip_cidr 做真正的集合差。
//
// IP 前缀是这些字段里唯一**没有表达边界**的：一组前缀减一组前缀，结果仍然是
// 一组前缀。所以这里不做覆盖近似，直接算差集，10.0.0.0/8 减 10.1.0.0/16
// 会被正确拆成若干前缀。
//
// 只重写与 exclude 真正相交的那些前缀，不相交的原样留着 —— 否则会顺带做一次
// 全量聚合，而 aggregate 是显式开关，不该被 exclude 悄悄打开。
func (s *RuleSet) subtractCIDR(other *RuleSet) int {
	removed := 0
	for _, f := range [...]Field{FieldIPCIDR, FieldSourceIPCIDR} {
		mine := s.strs[f]
		theirs := other.strs[f]
		if len(mine) == 0 || len(theirs) == 0 {
			continue
		}
		var db netipx.IPSetBuilder
		for _, v := range sortedKeys(theirs) {
			if p, err := netip.ParsePrefix(v); err == nil {
				db.AddPrefix(p)
			}
		}
		dropSet, err := db.IPSet()
		if err != nil {
			continue
		}

		kept := make(map[string]struct{}, len(mine))
		var touched []netip.Prefix
		gone, narrowed := 0, 0
		for _, v := range sortedKeys(mine) {
			p, err := netip.ParsePrefix(v)
			if err != nil {
				// 进得来就一定归一过，这里不可能失败；真失败了宁可原样留着。
				kept[v] = struct{}{}
				continue
			}
			switch {
			case dropSet.ContainsPrefix(p):
				gone++ // 整条被吃掉
			case dropSet.OverlapsPrefix(p):
				touched = append(touched, p)
				narrowed++
			default:
				kept[v] = struct{}{}
			}
		}
		if gone == 0 && len(touched) == 0 {
			continue
		}
		if len(touched) > 0 {
			var kb netipx.IPSetBuilder
			for _, p := range touched {
				kb.AddPrefix(p)
			}
			kb.RemoveSet(dropSet)
			rest, err := kb.IPSet()
			if err != nil {
				// 算不出来就把相交的那些原样留着，宁可少排除也不要乱删。
				for _, p := range touched {
					kept[p.String()] = struct{}{}
				}
				narrowed = 0
			} else {
				for _, p := range rest.Prefixes() {
					kept[p.String()] = struct{}{}
				}
			}
		}
		s.strs[f] = kept
		s.Diag.Narrowed += narrowed
		removed += gone
	}
	return removed
}

// ---------------- 其余字段与透传规则 ----------------

// subtractLiteral 处理没有覆盖关系可言的字段：值就是值，相等才删。
//
// domain_regex 也在这里 —— 判两个正则谁包含谁是不可判定的，不猜。
func (s *RuleSet) subtractLiteral(other *RuleSet) int {
	removed := 0
	for f, m := range s.strs {
		switch f {
		case FieldDomain, FieldDomainSuffix, FieldDomainKeyword, FieldIPCIDR, FieldSourceIPCIDR:
			continue // 上面按语义处理过了
		}
		drop := other.strs[f]
		if len(drop) == 0 {
			continue
		}
		for v := range m {
			if _, found := drop[v]; found {
				delete(m, v)
				removed++
			}
		}
	}
	for f, m := range s.ports {
		drop := other.ports[f]
		if len(drop) == 0 {
			continue
		}
		for v := range m {
			if _, found := drop[v]; found {
				delete(m, v)
				removed++
			}
		}
	}
	return removed
}

// subtractVerbatim 只按去重键完全相同删除。
//
// 从一条 logical 规则里"减掉"另一条不是有定义的操作，所以这里不做语义差集，
// 而是在两边都带着透传规则时记一条诊断说明这件事。
func (s *RuleSet) subtractVerbatim(other *RuleSet) int {
	removed := 0
	if len(other.seen) > 0 && len(s.verbatim) > 0 {
		kept := s.verbatim[:0:0]
		for _, rule := range s.verbatim {
			key, err := marshalRule(rule)
			if err != nil {
				kept = append(kept, rule)
				continue
			}
			if _, found := other.seen[string(key)]; found {
				delete(s.seen, string(key))
				removed++
				continue
			}
			kept = append(kept, rule)
		}
		s.verbatim = kept
	}
	if len(other.verbatim) > 0 && len(s.verbatim) > 0 {
		s.Diag.BadValue("verbatim", "exclude 里的透传规则只按完全相同匹配，未做语义差集")
	}
	return removed
}
