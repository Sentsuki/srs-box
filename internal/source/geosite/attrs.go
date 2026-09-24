package geosite

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// item 是一条已经映射到 sing-box 字段的规则值。
type item struct {
	field ruleset.Field
	value string
}

// codeMap 是 code → items，键已小写。
type codeMap map[string][]item

// build 把解码结果变成可寻址的 code 表。
//
// 这里只做**数据解码**，不做任何口味判断：
//
//   - 不实现 mergeTags。它把 category-*-cn 与 category-*@cn 并进 geolocation-cn
//     并**原地覆盖**，还派生一个 dlc.dat 里本不存在的 cn —— 那是隐式行为，而且
//     "谁算中国大陆域名"是配置该说的事。实测它的增量也远比看起来小：
//     geolocation-cn 自己就有 158 条 include:，29 个 category-*-cn 里 26 个早已
//     被包含；派生的 domain_suffix:"cn" 也是冗余的（tld-cn 里就有裸 cn）。
//   - 不实现 filterTags。删冗余 code@attr 留着无害（白名单不选就不输出）；
//     !attr 的集合差是上游对自己数据的一致性修正，但同样是原地覆盖，
//     而且受影响的 code 只有运行时才知道 —— 改成报出来，由配置决定要不要减。
//
// 必须做的是 @attr 展开：不把 google@ads 变成一个可寻址的 code，
// exclude: {geosite: ["google@ads"]} 就无从写起。
//
// 注意**带属性的域名同时留在父 code 里** —— 属性是标记，不是移出。所以
// google 是包含 google@ads 的，"要 google 但不要广告"才需要做差。
func build(sites []rawSite) (codeMap, []string) {
	codes := make(codeMap, len(sites)*2)
	for _, site := range sites {
		code := strings.ToLower(site.Code)
		base := make([]item, 0, len(site.Domains)*2)
		byAttr := map[string][]item{}

		for _, domain := range site.Domains {
			mapped := mapDomain(domain)
			base = append(base, mapped...)
			for _, attr := range domain.Attributes {
				key := strings.ToLower(attr)
				byAttr[key] = append(byAttr[key], mapped...)
			}
		}
		codes[code] = dedupeItems(base)
		for attr, items := range byAttr {
			codes[code+"@"+attr] = dedupeItems(items)
		}
	}
	return codes, contradictions(codes)
}

// mapDomain 把一条 v2fly 域名记录映射成 sing-box 字段。
//
// RootDomain 同时产出两条，这是上游的做法也是对的：
//
//	domain:        example.com     （值含点时才产出）
//	domain_suffix: .example.com    （前导点必须保留）
//
// 两条合起来等价于**无点**的 domain_suffix: example.com。我们的等价收敛正好会
// 把它们压回那一条 —— 而且这不是巧合：sing-box 自己的 domain.Matcher.Dump
// 在反编译 .srs 时做的就是同一个变换。
func mapDomain(d rawDomain) []item {
	switch d.Type {
	case typePlain:
		return []item{{ruleset.FieldDomainKeyword, d.Value}}
	case typeRegex:
		return []item{{ruleset.FieldDomainRegex, d.Value}}
	case typeFull:
		return []item{{ruleset.FieldDomain, d.Value}}
	case typeRootDomain:
		out := make([]item, 0, 2)
		if strings.Contains(d.Value, ".") {
			out = append(out, item{ruleset.FieldDomain, d.Value})
		}
		out = append(out, item{ruleset.FieldDomainSuffix, "." + d.Value})
		return out
	default:
		return nil
	}
}

func dedupeItems(in []item) []item {
	if len(in) < 2 {
		return in
	}
	seen := make(map[item]struct{}, len(in))
	out := in[:0:0]
	for _, it := range in {
		if _, dup := seen[it]; dup {
			continue
		}
		seen[it] = struct{}{}
		out = append(out, it)
	}
	return out
}

// contradictions 找出与自己名字矛盾的属性 code，例如 geolocation-!cn@cn
// ——"非中国大陆"这个列表里标着"中国大陆"的那些条目。
//
// 上游 filterTags 会把它们从父 code 里减掉并删除。我们不自动做：那是原地覆盖，
// 会让 geolocation-!cn 静默地不等于 dlc.dat 里写的那个。改成报出来，要排除就写
//
//	{"geosite": ["geolocation-!cn"], "exclude": {"geosite": ["geolocation-!cn@cn"]}}
func contradictions(codes codeMap) []string {
	var found []string
	for code := range codes {
		base, attr, ok := strings.Cut(code, "@")
		if !ok {
			continue
		}
		last := base
		if idx := strings.LastIndexByte(base, '-'); idx >= 0 && idx+1 < len(base) {
			last = base[idx+1:]
		}
		// last 与 attr 互为取反：cn vs !cn，或者 !cn vs cn。
		if "!"+last == attr || last == "!"+attr {
			found = append(found, code)
		}
	}
	sort.Strings(found)
	return found
}

// resolve 把一批 code 或 glob 展开成实际的 code 名。
//
// glob 是 path.Match 语义，匹配的是 **code 名**而不是域名 —— 配置里有两个世界的
// 星号，这个是选清单的那个。
//
// 一个模式都没匹配上要报错：上游删掉或改名一个 code 时，静默产出零条规则是
// 最难发现的失败形态。
func (c codeMap) resolve(pattern string) ([]string, error) {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	if pattern == "" {
		return nil, fmt.Errorf("code 不能为空")
	}
	if !hasGlob(pattern) {
		if _, ok := c[pattern]; !ok {
			return nil, fmt.Errorf("dlc.dat 里没有 code %q", pattern)
		}
		return []string{pattern}, nil
	}
	var matched []string
	for code := range c {
		ok, err := matchGlob(pattern, code)
		if err != nil {
			return nil, fmt.Errorf("glob %q 非法: %w", pattern, err)
		}
		if ok {
			matched = append(matched, code)
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("glob %q 一个 code 都没匹配上", pattern)
	}
	sort.Strings(matched)
	return matched, nil
}

func hasGlob(s string) bool {
	return strings.ContainsAny(s, "*?[")
}
