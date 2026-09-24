package emit_test

import (
	"bytes"
	"encoding/json"
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
