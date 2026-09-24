package emit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// JSON 把规则集写成 sing-box headless rule JSON。
//
// 不走 option 类型自带的序列化，有两个原因：
//
//   - badoption.Listable 会把单元素列表塌成标量（{"domain":"a.com"}），
//     于是产物里标量和数组混着出现 —— 既不好 diff，也和旧版产物不一致。
//   - encoding/json 默认把 < > & 转成 < 之类。domain_regex 里这些字符很常见，
//     转义之后产物虽然合法但没法读，也会让与旧版的对拍全线飘红。
//
// 所以这里自己拼：值永远是数组，转义关掉，缩进两空格，末尾一个换行。
func JSON(w io.Writer, set *ruleset.RuleSet, version uint8) error {
	rules := make([]json.RawMessage, 0, len(ruleset.AllFields())+len(set.Verbatim()))

	// 目标地址组一条 rule 里出全 —— 和 ruleset.Options 同一个理由：规则集只有
	// 一条 rule，sing-box 才会把它并进引用方的匹配组。见 Options 的注释。
	var destParts []string
	for _, f := range ruleset.AllFields() {
		if !f.InDestinationGroup() {
			continue
		}
		values := set.Values(f)
		if len(values) == 0 {
			continue
		}
		raw, err := encode(values)
		if err != nil {
			return fmt.Errorf("序列化 %s 失败: %w", f, err)
		}
		destParts = append(destParts, fmt.Sprintf("%q:%s", f.String(), raw))
	}

	for _, f := range ruleset.AllFields() {
		if f.InDestinationGroup() {
			if len(destParts) > 0 {
				rules = append(rules, json.RawMessage("{"+strings.Join(destParts, ",")+"}"))
				destParts = nil
			}
			continue
		}
		var raw json.RawMessage
		var err error
		if f.IsPort() {
			ports := set.Ports(f)
			if len(ports) == 0 {
				continue
			}
			raw, err = encode(map[string][]uint16{f.String(): ports})
		} else {
			values := set.Values(f)
			if len(values) == 0 {
				continue
			}
			raw, err = encode(map[string][]string{f.String(): values})
		}
		if err != nil {
			return fmt.Errorf("序列化 %s 失败: %w", f, err)
		}
		rules = append(rules, raw)
	}

	// 透传规则只能交给 sing-box 自己序列化 —— 我们手上只有 option.HeadlessRule，
	// 拼不出它的 logical 嵌套。代价是这些规则里的单元素列表仍是标量形态。
	for _, rule := range set.Verbatim() {
		raw, err := encode(rule)
		if err != nil {
			return fmt.Errorf("序列化透传规则失败: %w", err)
		}
		rules = append(rules, raw)
	}

	doc := struct {
		Version uint8             `json:"version"`
		Rules   []json.RawMessage `json:"rules"`
	}{Version: version, Rules: rules}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(doc) // Encode 自带末尾换行
}

// JSONFile 把规则集原子地写到 path，返回产物字节数。
func JSONFile(path string, set *ruleset.RuleSet, version uint8) (int64, error) {
	return writeFileAtomic(path, func(w io.Writer) error {
		return JSON(w, set, version)
	})
}

func encode(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}
