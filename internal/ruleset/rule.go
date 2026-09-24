package ruleset

import (
	"encoding/json"

	"github.com/sagernet/sing-box/option"
)

// marshalRule 是透传规则的稳定序列化，同时充当去重键。
//
// 用 encoding/json 而不是 sing 自己的 json 包：后者会把单元素列表塌成标量
// （{"domain":"a.com"}），同一条规则因此可能有两种文本形态，去重键就不稳了。
func marshalRule(rule option.HeadlessRule) ([]byte, error) {
	return json.Marshal(rule)
}

// ruleHits 递归判断一条规则（含嵌套逻辑规则）的任意字符串叶子是否命中。
//
// 走解码后的结构而不是在 JSON 文本上做子串匹配：后者会把**字段名**也算进去，
// 一个叫 "domain" 的过滤词能命中每一条规则。
func ruleHits(rule option.HeadlessRule, needles []string) bool {
	raw, err := marshalRule(rule)
	if err != nil {
		// 序列化不了的规则写不出去，当命中处理（丢弃）比留着安全。
		return true
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		return true
	}
	return nodeHits(node, needles)
}

func nodeHits(node any, needles []string) bool {
	switch v := node.(type) {
	case map[string]any:
		for _, child := range v {
			if nodeHits(child, needles) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if nodeHits(child, needles) {
				return true
			}
		}
	case string:
		return hits(v, needles)
	}
	return false
}
