package parse

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/sagernet/sing-box/option"
)

// Rule 把一条 sing-box headless rule 并进 set。它是 inline 的唯一入口。
//
// 与 tree 的区别在于不做任何"猜"：tree 为了吃下各家的包装结构，会递归找已知
// 字段、把裸字符串交给行提取器；这里只认 sing-box 自己的规则对象。配置是手写的，
// 手写的东西写错了就该当场报错，而不是被"宽容地"解析成另一条规则。
//
// format 只取断言部分（cidr / domainset 的字段白名单）；adguard 描述的是文本语法，
// 对 JSON 对象没有意义，自然不生效。
func Rule(data []byte, format Format, set *ruleset.RuleSet) error {
	if err := ValidateRule(data); err != nil {
		return err
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		return parseErr(string(data), "JSON 解析失败: "+err.Error(), "")
	}
	p := &parser{set: set, allow: allowed[format]}
	// 逻辑规则、带 invert 的规则、含我们不拆的字段的规则整条透传 —— 理由同 tree.walk。
	if isVerbatimRule(v) || !allFieldsKnown(v) {
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

// ValidateRule 检查 data 是不是一条 sing-box 认的 headless rule。
//
// 校验交给 sing-box 自己的 unmarshaller：字段名拼错、值类型不对它都会拒绝，
// 而且永远和编译时的标准一致。配置加载时就调用它，错误能带上配置路径。
func ValidateRule(data []byte) error {
	var rule option.HeadlessRule
	if err := json.Unmarshal(data, &rule); err != nil {
		return fmt.Errorf("不是合法的 sing-box headless rule: %v", err)
	}
	if !rule.IsValid() {
		return errors.New("规则里没有任何匹配条件")
	}
	return nil
}
