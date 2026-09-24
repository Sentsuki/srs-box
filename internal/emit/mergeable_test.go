package emit_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// 只含目标地址（域名 + ip_cidr）的规则集必须**恰好**产出一条 rule。
//
// sing-box 只在规则集只有一条 rule 时，才把它并进引用方的匹配组
// （route/rule/rule_item_rule_set.go 的 mergeableRuleIn）。多出一条，
//
//	{"domain_suffix": ["example.org"], "rule_set": ["geosite-google"]}
//
// 就从 OR 退化成 AND，域名一条都匹配不上 —— 而产物本身看不出任何异常，
// 所以这条不变量必须由测试守住。
func TestDestinationOnlySetEmitsSingleRule(t *testing.T) {
	s := ruleset.New("dest")
	s.AddLenient(ruleset.FieldDomain, "exact.example")
	s.AddLenient(ruleset.FieldDomainSuffix, ".suffix.example")
	s.AddLenient(ruleset.FieldDomainKeyword, "keyword")
	s.AddLenient(ruleset.FieldDomainRegex, `^ad\.example$`)
	s.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")

	if got := len(s.Options().Rules); got != 1 {
		t.Errorf("Options() 产出 %d 条 rule，要 1 条", got)
	}

	var buf bytes.Buffer
	if err := emit.JSON(&buf, s, 4); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Rules []map[string]json.RawMessage `json:"rules"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Rules) != 1 {
		t.Fatalf("JSON 产出 %d 条 rule，要 1 条：%s", len(doc.Rules), buf.String())
	}
	for _, name := range []string{"domain", "domain_suffix", "domain_keyword", "domain_regex", "ip_cidr"} {
		if _, ok := doc.Rules[0][name]; !ok {
			t.Errorf("第一条 rule 里没有 %s：%s", name, buf.String())
		}
	}
}

// rule 内是 AND 项的字段仍要各自成条：合进去会变成"必须同时命中"。
func TestAndItemsStaySeparate(t *testing.T) {
	s := ruleset.New("mixed")
	s.AddLenient(ruleset.FieldDomainSuffix, ".example")
	s.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")
	s.AddLenient(ruleset.FieldProcessName, "Telegram")
	s.AddPort(ruleset.FieldPort, 443)

	if got := len(s.Options().Rules); got != 3 {
		t.Errorf("Options() 产出 %d 条 rule，要 3 条（域名+ip_cidr / process_name / port）", got)
	}
}

// 两条产出路径必须给出同一套形状。
//
// ruleset.Options 拼结构体、emit.JSON 手拼 JSON，分组逻辑各写了一遍 ——
// 改一边漏另一边，.srs 与 .json 就会语义漂移，而"两种产物不可能语义漂移"
// 正是 Options 作为唯一序列化出口的立身之本。
func TestJSONAndOptionsAgreeOnShape(t *testing.T) {
	cases := []struct {
		name  string
		build func(*ruleset.RuleSet)
	}{
		{"只有域名", func(s *ruleset.RuleSet) {
			s.AddLenient(ruleset.FieldDomain, "a.example")
			s.AddLenient(ruleset.FieldDomainSuffix, ".b.example")
		}},
		{"域名加 ip_cidr", func(s *ruleset.RuleSet) {
			s.AddLenient(ruleset.FieldDomainKeyword, "ads")
			s.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")
		}},
		{"只有 ip_cidr", func(s *ruleset.RuleSet) {
			s.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")
		}},
		{"混入 AND 项字段", func(s *ruleset.RuleSet) {
			s.AddLenient(ruleset.FieldDomainSuffix, ".example")
			s.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")
			s.AddLenient(ruleset.FieldNetwork, "tcp")
			s.AddLenient(ruleset.FieldSourceIPCIDR, "10.0.0.0/8")
			s.AddLenient(ruleset.FieldProcessName, "Telegram")
			s.AddPort(ruleset.FieldPort, 443)
			s.AddPort(ruleset.FieldSourcePort, 1080)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := ruleset.New("t")
			c.build(s)

			var buf bytes.Buffer
			if err := emit.JSON(&buf, s, 4); err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Rules []map[string]json.RawMessage `json:"rules"`
			}
			if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}

			opts := s.Options()
			if len(doc.Rules) != len(opts.Rules) {
				t.Fatalf("JSON %d 条 rule，Options %d 条：%s", len(doc.Rules), len(opts.Rules), buf.String())
			}
			if got := s.RuleObjects(); got != len(opts.Rules) {
				t.Errorf("RuleObjects() = %d，实际 %d 条", got, len(opts.Rules))
			}
			// 逐条比字段集合：哪个字段落在第几条 rule 上，两边必须一致。
			for i, rule := range doc.Rules {
				var jsonFields []string
				for name := range rule {
					if name == "type" {
						continue
					}
					jsonFields = append(jsonFields, name)
				}
				sort.Strings(jsonFields)

				raw, err := json.Marshal(opts.Rules[i])
				if err != nil {
					t.Fatal(err)
				}
				var asMap map[string]json.RawMessage
				if err := json.Unmarshal(raw, &asMap); err != nil {
					t.Fatal(err)
				}
				var optFields []string
				for name := range asMap {
					if name == "type" {
						continue
					}
					optFields = append(optFields, name)
				}
				sort.Strings(optFields)

				if !reflect.DeepEqual(jsonFields, optFields) {
					t.Errorf("第 %d 条 rule 字段不一致：JSON %v，Options %v", i, jsonFields, optFields)
				}
			}
		})
	}
}
