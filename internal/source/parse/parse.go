// Package parse 把一份源内容并进规则集。
//
// 这一层不做"格式声明"。内容自己会说明它是什么：
//
//  1. 魔数 "SRS" 且版本 ≤ 上限   → srs.Read      硬提交
//  2. 去 BOM/空白后首字节是 { 或 [ → encoding/json 硬提交
//  3. 其余一切                    → 行提取器
//
// 只有两种东西需要在配置里显式声明，都不是"格式判定"：
//
//   - adguard：语法与裸域名列表有重叠，判不出来；而且它编译成专属的
//     AdGuardDomain 字段和带 invert 的嵌套逻辑规则，猜错就是语义反转。
//   - cidr / domainset：它们不是格式，是**断言** —— 容器就是行提取器，
//     区别只在"混进别的东西就当场失败"。防的正是上游返回 HTTP 200 的 HTML
//     错误页这种悄悄腐坏。
package parse

import (
	"fmt"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// Format 是配置里可以写的那几个例外。空值表示按内容自动判定。
type Format string

const (
	// FormatAuto 按内容判定，配置里不写就是这个。
	FormatAuto Format = ""
	// FormatAdGuard AdGuard 过滤器语法，必须显式声明。
	FormatAdGuard Format = "adguard"
	// FormatCIDR 断言：只允许 IP/CIDR。
	FormatCIDR Format = "cidr"
	// FormatDomainSet 断言：只允许域名。
	FormatDomainSet Format = "domainset"
)

// ParseFormat 解析配置里的 format 取值。
func ParseFormat(raw string) (Format, error) {
	switch Format(raw) {
	case FormatAuto, FormatAdGuard, FormatCIDR, FormatDomainSet:
		return Format(raw), nil
	default:
		return FormatAuto, fmt.Errorf("format 取值非法 %q，只能是 adguard / cidr / domainset —— "+
			"singbox、yaml、text、srs 都不用写了，按内容自动判定", raw)
	}
}

func (f Format) String() string {
	if f == FormatAuto {
		return "auto"
	}
	return string(f)
}

// Error 是源内容无法按声明或判定的形态解析。
type Error struct {
	Origin string // 出问题的那一行 / 那个片段
	Reason string
	Hint   string
}

func (e *Error) Error() string {
	msg := e.Reason
	if e.Origin != "" {
		msg += fmt.Sprintf("（来自 %q）", truncate(e.Origin, 80))
	}
	if e.Hint != "" {
		msg += "\n  " + e.Hint
	}
	return msg
}

func parseErr(origin, reason, hint string) error {
	return &Error{Origin: origin, Reason: reason, Hint: hint}
}

// allowed 是断言格式的字段白名单。nil 表示不限制。
//
// domainset 放行 domain_regex：通配符域名（*.example.com）是域名列表的常客，
// 转换后落在 domain_regex 上 —— 不放行的话一个通配符就会让整个规则集失败。
var allowed = map[Format]map[ruleset.Field]struct{}{
	FormatCIDR: {
		ruleset.FieldIPCIDR: {},
	},
	FormatDomainSet: {
		ruleset.FieldDomain:       {},
		ruleset.FieldDomainSuffix: {},
		ruleset.FieldDomainRegex:  {},
	},
}

// Into 把一份源内容按 format 并进 set。format 为 FormatAuto 时按内容判定。
func Into(data []byte, format Format, set *ruleset.RuleSet) error {
	p := &parser{set: set, allow: allowed[format]}
	switch format {
	case FormatAdGuard:
		return p.adguard(data)
	case FormatCIDR, FormatDomainSet:
		// 断言格式的容器固定是行提取器 —— 它们不描述结构，只描述内容约束。
		return p.lines(data)
	default:
		return p.auto(data)
	}
}

type parser struct {
	set *ruleset.RuleSet
	// allow 非 nil 时，出现白名单外的字段就整份失败。
	allow map[ruleset.Field]struct{}
}

// commit 落一个规则值。断言格式下越界即失败，其余情况记账后继续。
func (p *parser) commit(f ruleset.Field, value, origin string) error {
	if p.allow != nil {
		if _, ok := p.allow[f]; !ok {
			return parseErr(origin,
				fmt.Sprintf("严格格式不允许 %s 规则", f),
				`该源确实混有此类规则的话，把 format 去掉即可 —— 自动判定不做断言`)
		}
	}
	if !p.set.AddLenient(f, value) && p.allow != nil {
		return parseErr(origin, "严格格式下遇到非法值",
			"检查上游内容是否已变化，或把 format 去掉")
	}
	return nil
}

func truncate(s string, n int) string {
	flat := make([]rune, 0, n+1)
	space := false
	for _, r := range s {
		if r == '\n' || r == '\r' || r == '\t' || r == ' ' {
			space = true
			continue
		}
		if space && len(flat) > 0 {
			flat = append(flat, ' ')
		}
		space = false
		flat = append(flat, r)
		if len(flat) > n {
			return string(flat[:n]) + "…"
		}
	}
	return string(flat)
}
