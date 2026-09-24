package parse

import (
	"encoding/json"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/sagernet/sing-box/option"
)

// tree 是树提取器：递归走整棵 JSON，遇到已知字段名就收值。
//
// 不判断"这是不是一份合法的 sing-box rule-set"。顶层是 {"version":N,"rules":[…]}
// 也行，是 {"payload":[…]} 也行，是别人自定义的包装结构也行 —— 比要求顶层必须有
// rules 数组宽松得多，Clash meta 的 provider JSON 都能直接吃。
//
// 三类节点各走各路：
//
//   - 已知字段名 → 取其值
//   - 裸字符串   → **交给行提取器**（{"payload":["DOMAIN-SUFFIX,a.com"]}）
//   - type:logical 或带 invert 的对象 → 整个子树原样透传进 verbatim
//
// 第二条是关键：两个提取器是**组合**关系而非二选一。少了它，Clash 的 YAML/JSON
// provider 会产出零条规则。
func (p *parser) tree(data []byte) error {
	var node any
	if err := json.Unmarshal(data, &node); err != nil {
		return parseErr(string(data), "JSON 解析失败: "+err.Error(),
			"首字节是 { 或 [ 就按 JSON 处理，不回退按行解析 —— "+
				"残缺的 JSON 回退成文本会解析出一堆看不出异常的垃圾域名。")
	}
	return p.walk(node, data)
}

func (p *parser) walk(node any, raw []byte) error {
	switch v := node.(type) {
	case map[string]any:
		if isVerbatimRule(v) {
			return p.passthrough(v)
		}
		if isRuleObject(v) {
			// 含未知字段的规则**整条透传**，绝不"只挑认识的字段拿"。
			// 单条规则里的字段是要同时满足的条件，悄悄丢掉一个 query_type
			// 等于去掉一个条件，规则会匹配到比原来更多的流量。
			if !allFieldsKnown(v) {
				return p.passthrough(v)
			}
			for key, child := range v {
				field, known := ruleset.Lookup(key)
				if !known {
					continue // 只可能是 "type": "default"
				}
				if err := p.leaf(field, child); err != nil {
					return err
				}
			}
			return nil
		}
		// 不是规则对象，只是外层包装（{"version":4,"rules":[…]}、
		// {"payload":[…]}、别人自定义的结构）—— 继续往下走。
		for _, child := range v {
			if err := p.walk(child, raw); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := p.walk(child, raw); err != nil {
				return err
			}
		}
	case string:
		// 裸字符串走条目层：它可能是 "DOMAIN-SUFFIX,a.com"，也可能是 "+.a.com"。
		entry, ok := cleanLine(v)
		if !ok {
			return nil
		}
		return p.entry(entry)
	}
	return nil
}

// isRuleObject 报告这个对象是不是一条规则 —— 判据是它至少带一个我们认识的字段名。
func isRuleObject(v map[string]any) bool {
	for key := range v {
		if _, known := ruleset.Lookup(key); known {
			return true
		}
	}
	return false
}

// allFieldsKnown 报告这条规则的字段是否全在我们拆开处理的范围内。
func allFieldsKnown(v map[string]any) bool {
	for key := range v {
		if _, known := ruleset.Lookup(key); known {
			continue
		}
		if key == "type" {
			continue
		}
		return false
	}
	return true
}

// leaf 处理"已知字段名 → 值"。值可以是标量，也可以是数组 —— badoption.Listable
// 两种都接受，上游产出哪种的都有。
func (p *parser) leaf(field ruleset.Field, value any) error {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			if err := p.leafScalar(field, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return p.leafScalar(field, value)
	}
}

func (p *parser) leafScalar(field ruleset.Field, value any) error {
	switch v := value.(type) {
	case string:
		return p.commit(field, v, v)
	case float64:
		// 端口在 JSON 里是数字。json.Unmarshal 成 any 一律给 float64。
		return p.commit(field, formatNumber(v), formatNumber(v))
	case bool, nil:
		p.set.Diag.BadValue(field.String(), "字段值类型不对，期望字符串或数组")
		return nil
	default:
		p.set.Diag.BadValue(field.String(), "字段值类型不对，期望字符串或数组")
		return nil
	}
}

// isVerbatimRule 报告这个对象是否必须整条透传。
//
// 逻辑规则和带 invert 的规则不能把值拆出来跟别的规则混在一起去重 ——
// 那会改变语义。
func isVerbatimRule(v map[string]any) bool {
	if t, ok := v["type"].(string); ok && t == "logical" {
		return true
	}
	if inv, ok := v["invert"].(bool); ok && inv {
		return true
	}
	return false
}

// passthrough 把一个子树反序列化成 option.HeadlessRule 后原样透传。
//
// 认不认识交给 sing-box 自己的 unmarshaller —— 编译在进程内，它读不进去的东西
// 编译时一样会死，只是死得更晚、错误更难懂。所以在这里失败是对的。
func (p *parser) passthrough(v map[string]any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		p.set.Diag.BadValue("verbatim", "子树无法重新序列化: "+err.Error())
		return nil
	}
	var rule option.HeadlessRule
	if err := json.Unmarshal(raw, &rule); err != nil {
		p.set.Diag.BadValue("verbatim", "sing-box 不认识这条规则: "+err.Error())
		return nil
	}
	p.set.AddVerbatim(rule)
	return nil
}

func formatNumber(f float64) string {
	raw, err := json.Marshal(f)
	if err != nil {
		return ""
	}
	return string(raw)
}
