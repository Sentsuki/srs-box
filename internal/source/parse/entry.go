package parse

import (
	"net/netip"
	"regexp"
	"strings"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
)

// 条目层：一个"条目"变成若干规则值。
//
// 条目有两种形态，分支互斥，没有 fallthrough 到"猜"：
//
//   - typed —— DOMAIN-SUFFIX,example.com,PROXY。策略列（第三段）直接丢弃。
//     Clash / Surge / Quantumult X 的差异几乎都在这一列，丢掉之后它们就是
//     同一种东西。
//   - bare —— 裸值。种类逐条判断（IP 还是域名），不需要先把整个文件归类，
//     所以同一文件里混写 IP 和域名也能正确处理。

var (
	typeNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]*$`)
	// ipishRe 是所有合法 IP 的超集，用来快速否定。
	ipishRe = regexp.MustCompile(`^[0-9a-fA-F:.]+(?:/\d{1,3})?$`)
	// 只剥"空白 + # 或 //"形式的行尾注释。不剥无空白前缀的 #，
	// 免得把 domain_regex 里的 # 当注释砍掉。
	inlineCommentRe = regexp.MustCompile(`\s+(?:#|//).*$`)
	// yamlMappingRe 命中 "TYPE:" 或 "TYPE: value" 这种写法，见 guardYAMLMapping。
	yamlMappingRe = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_-]*)\s*:`)
)

// entry 把一个条目并进规则集。
func (p *parser) entry(raw string) error {
	head, rest, hasComma := strings.Cut(raw, ",")
	head = strings.TrimSpace(head)
	upper := strings.ToUpper(head)

	if hasComma {
		if mode, ok := logicalMode[upper]; ok {
			return p.logical(mode.mode, mode.invert, rest, raw)
		}
		if field, ok := types[upper]; ok {
			var value string
			if field == ruleset.FieldDomainRegex {
				value = regexValue(rest)
			} else {
				first, _, _ := strings.Cut(rest, ",")
				value = stripInlineComment(strings.TrimSpace(first))
			}
			return p.commit(field, value, raw)
		}
		if _, ok := skip[upper]; ok {
			p.set.Diag.Skip(upper)
			return nil
		}
		if typeNameRe.MatchString(head) {
			p.set.Diag.UnknownType(upper)
			return nil
		}
	}
	return p.bare(stripInlineComment(raw), raw)
}

// bare 处理裸值：按值自身的形态决定字段，不依赖文件级归类。
func (p *parser) bare(value, origin string) error {
	if value == "" {
		return nil
	}
	field := ruleset.FieldDomain
	switch {
	case looksLikeIP(value):
		field = ruleset.FieldIPCIDR
	case strings.HasPrefix(value, "+."):
		// Clash 的 +.example.com 意思是"example.com 及其全部子域"，
		// 正好是 sing-box **无点** domain_suffix 的语义 —— 所以点要剥掉。
		field, value = ruleset.FieldDomainSuffix, value[2:]
	case strings.Contains(value, "*"):
		pattern, ok := wildcardToRegex(value)
		if !ok {
			// ad*.example.com 这种只好当普通域名处理（进而被记成非法值），
			// 而不是猜一个正则出来。
			break
		}
		field, value = ruleset.FieldDomainRegex, pattern
	case strings.HasPrefix(value, "."):
		// domainset 里 .example.com 同样是"apex + 子域"的约定，同上剥点。
		field, value = ruleset.FieldDomainSuffix, value[1:]
	}
	return p.commit(field, value, origin)
}

// logical 把 AND,((DOMAIN,a),(DOMAIN-SUFFIX,b)) 变成 sing-box 的逻辑规则。
//
// 任一子项无法表达时**整条丢弃**。逻辑规则少一个子项就是另一条规则 ——
// AND 少一项等于放宽匹配面，把它输出去比丢掉更危险。
func (p *parser) logical(mode string, invert bool, rest, origin string) error {
	giveUp := func(reason string) {
		p.set.Diag.BadValue("logical", reason+"，整条逻辑规则丢弃: "+truncate(origin, 80))
	}

	var children []option.HeadlessRule
	for _, part := range findGroups(rest) {
		subHead, subRest, ok := strings.Cut(part, ",")
		subType := strings.ToUpper(strings.TrimSpace(subHead))
		field, known := types[subType]
		if !ok || !known {
			giveUp("子项类型 " + subType + " 无法表达")
			return nil
		}
		if p.allow != nil {
			if _, allowedField := p.allow[field]; !allowedField {
				return parseErr(origin, "严格格式不允许逻辑规则里的 "+field.String()+" 子项",
					"把 format 去掉即可")
			}
		}
		var value string
		if field == ruleset.FieldDomainRegex {
			value = regexValue(subRest)
		} else {
			first, _, _ := strings.Cut(subRest, ",")
			value = stripInlineComment(strings.TrimSpace(first))
		}
		// 逻辑规则走 AddVerbatim，绕开了 Add 那条归一化管道，所以这里必须自己
		// 归一：否则 DST-PORT,443 会产出字符串端口，sing-box 直接拒绝编译**整个**
		// 规则集。ruleset.SingleRule 把归一和装配放在一起，让人想绕都绕不过去。
		child, err := ruleset.SingleRule(field, value)
		if err != nil {
			giveUp("子项取值非法（" + err.Error() + "）")
			return nil
		}
		children = append(children, child)
	}
	if len(children) == 0 {
		giveUp("没有可用子项")
		return nil
	}
	p.set.AddVerbatim(option.HeadlessRule{
		Type: C.RuleTypeLogical,
		LogicalOptions: option.LogicalHeadlessRule{
			Mode:   mode,
			Invert: invert,
			Rules:  children,
		},
	})
	return nil
}

// findGroups 取出最内层的 (...) 片段，等价于 Python 的 re.findall(r"\(([^()]*)\)")。
func findGroups(s string) []string {
	var out []string
	depth := 0
	start := -1
	for i, c := range s {
		switch c {
		case '(':
			depth++
			start = i + 1
		case ')':
			if depth > 0 && start >= 0 {
				out = append(out, s[start:i])
			}
			if depth > 0 {
				depth--
			}
			start = -1
		}
	}
	return out
}

// regexValue 从 rest 里取出正则值，在第一个**顶层**逗号处截断。
//
// 正则里的逗号几乎总在 {m,n}、[a,b] 或 (a|b) 内部，而策略列的逗号在最外层。
// 无脑按逗号切会把 ^a{1,3}\.com$ 砍成 ^a{1 —— 砍完仍是合法正则、仍能编译，
// 只是匹配的东西变了，是最难发现的一类错误。
func regexValue(rest string) string {
	depth := 0
	escaped := false
	inClass := false
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		switch {
		case escaped:
			escaped = false
		case c == '\\':
			escaped = true
		case inClass:
			if c == ']' {
				inClass = false
			}
		case c == '[':
			inClass = true
		case c == '(' || c == '{':
			depth++
		case c == ')' || c == '}':
			if depth > 0 {
				depth--
			}
		case c == ',' && depth == 0:
			return strings.TrimSpace(rest[:i])
		}
	}
	return strings.TrimSpace(rest)
}

// wildcardToRegex 把 *.example.com 转成 ^[^.]+\.example\.com$。
//
// Clash 的 * 严格匹配**一个** label：a.example.com 命中，a.b.example.com 和
// example.com 都不命中。sing-box 的 domain / domain_suffix 都没有等价写法，
// 只能落到 domain_regex —— 用 domain_suffix 近似会把多级子域也吃进来，
// 那是放宽匹配面。
//
// 只认整段 label 是 * 的写法；ad*.example.com 这种返回 false，交给上层当普通
// 域名处理，而不是猜。
func wildcardToRegex(host string) (string, bool) {
	labels := strings.Split(host, ".")
	starred := false
	for _, label := range labels {
		if label == "*" {
			starred = true
			continue
		}
		if strings.Contains(label, "*") {
			return "", false
		}
	}
	if !starred {
		return "", false
	}
	parts := make([]string, len(labels))
	for i, label := range labels {
		if label == "*" {
			parts[i] = "[^.]+"
		} else {
			parts[i] = regexp.QuoteMeta(label)
		}
	}
	return "^" + strings.Join(parts, `\.`) + "$", true
}

// looksLikeIP 先用字符集快速排除，再用真解析确认。
//
// 只看字符集会把纯十六进制字面的域名误判成 IP —— bad.cc、cafe.fee、dead.beef
// 全部由 0-9a-f. 组成。误判的后果不只是丢一条规则：domainset 断言下会因字段
// 不在白名单而整份失败，整个规则集陪葬。
//
// 字符集正则是所有合法 IP 的超集，拿它做快速否定不会漏判；真解析只在少数
// "长得像 IP"的值上跑，几十万行的域名列表不会因此变慢。
func looksLikeIP(value string) bool {
	if !ipishRe.MatchString(value) {
		return false
	}
	if _, err := netip.ParsePrefix(value); err == nil {
		return true
	}
	_, err := netip.ParseAddr(value)
	return err == nil
}

func stripInlineComment(value string) string {
	return strings.TrimSpace(inlineCommentRe.ReplaceAllString(value, ""))
}
