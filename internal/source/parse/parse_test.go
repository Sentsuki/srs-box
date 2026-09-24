package parse

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/ruleset"
)

func mustParse(t *testing.T, body string, format Format) *ruleset.RuleSet {
	t.Helper()
	set := ruleset.New("t")
	if err := Into([]byte(body), format, set); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	return set
}

func values(t *testing.T, set *ruleset.RuleSet, f ruleset.Field) []string {
	t.Helper()
	return set.Values(f)
}

// ---------------- 条目层：typed ----------------

func TestTypedRules(t *testing.T) {
	body := `
# 注释
DOMAIN,a.example.com,PROXY
HOST,b.example.com
DOMAIN-SUFFIX,suffix.example,DIRECT
HOST-SUFFIX,host-suffix.example
DOMAIN-KEYWORD,keyword,REJECT
IP-CIDR,1.2.3.0/24,DIRECT,no-resolve
IP-CIDR6,2001:db8::/32
SRC-IP-CIDR,10.0.0.0/8
DST-PORT,443
SRC-PORT,1080
PROCESS-NAME,Telegram
PACKAGE-NAME,org.telegram.messenger
NETWORK,udp
`
	set := mustParse(t, body, FormatAuto)

	checks := []struct {
		field ruleset.Field
		want  []string
	}{
		{ruleset.FieldDomain, []string{"a.example.com", "b.example.com"}},
		{ruleset.FieldDomainSuffix, []string{"host-suffix.example", "suffix.example"}},
		{ruleset.FieldDomainKeyword, []string{"keyword"}},
		{ruleset.FieldIPCIDR, []string{"1.2.3.0/24", "2001:db8::/32"}},
		{ruleset.FieldSourceIPCIDR, []string{"10.0.0.0/8"}},
		{ruleset.FieldProcessName, []string{"Telegram"}},
		{ruleset.FieldPackageName, []string{"org.telegram.messenger"}},
		{ruleset.FieldNetwork, []string{"udp"}},
	}
	for _, c := range checks {
		if got := values(t, set, c.field); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s = %v, want %v", c.field, got, c.want)
		}
	}
	if got := set.Ports(ruleset.FieldPort); !reflect.DeepEqual(got, []uint16{443}) {
		t.Errorf("port = %v", got)
	}
	if got := set.Ports(ruleset.FieldSourcePort); !reflect.DeepEqual(got, []uint16{1080}) {
		t.Errorf("source_port = %v", got)
	}
}

// 策略列丢掉之后 Clash / Surge / Quantumult X 就是同一种东西。
func TestPolicyColumnIsDropped(t *testing.T) {
	for _, line := range []string{
		"DOMAIN-SUFFIX,example.com",
		"DOMAIN-SUFFIX,example.com,PROXY",
		"DOMAIN-SUFFIX,example.com,DIRECT,no-resolve",
		"DOMAIN-SUFFIX,example.com # 行尾注释",
	} {
		set := mustParse(t, line, FormatAuto)
		if got := values(t, set, ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com"}) {
			t.Errorf("%q → %v", line, got)
		}
	}
}

func TestSkippedTypesAreCounted(t *testing.T) {
	set := mustParse(t, "GEOIP,CN,DIRECT\nURL-REGEX,^https?://ad\nRULE-SET,foo,PROXY\n", FormatAuto)
	if set.Total() != 0 {
		t.Errorf("表达不了的类型不该产出规则，实际 %d 条", set.Total())
	}
	for _, want := range []string{"GEOIP", "URL-REGEX", "RULE-SET"} {
		if set.Diag.Skipped[want] != 1 {
			t.Errorf("%s 未计入 skipped: %+v", want, set.Diag.Skipped)
		}
	}
}

func TestUnknownTypeIsCounted(t *testing.T) {
	set := mustParse(t, "TOTALLY-MADE-UP,value,PROXY\n", FormatAuto)
	if set.Diag.Unknown["TOTALLY-MADE-UP"] != 1 {
		t.Errorf("未知类型未计数: %+v", set.Diag.Unknown)
	}
	if set.Total() != 0 {
		t.Errorf("未知类型不该产出规则")
	}
}

// ---------------- 条目层：bare ----------------

func TestBareValues(t *testing.T) {
	body := `
example.com
+.plus.example
.dot.example
1.2.3.4
1.2.3.0/24
2001:db8::/32
*.wild.example
`
	set := mustParse(t, body, FormatAuto)

	if got := values(t, set, ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"example.com"}) {
		t.Errorf("domain = %v", got)
	}
	// +. 和 . 都是"apex + 子域"的约定，正是 sing-box 无点 suffix 的语义，
	// 所以前导点要剥掉 —— 留着会变成"只匹配子域"，匹配面变窄。
	if got := values(t, set, ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"dot.example", "plus.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	if got := values(t, set, ruleset.FieldIPCIDR); !reflect.DeepEqual(got, []string{"1.2.3.0/24", "1.2.3.4/32", "2001:db8::/32"}) {
		t.Errorf("ip_cidr = %v", got)
	}
	if got := values(t, set, ruleset.FieldDomainRegex); !reflect.DeepEqual(got, []string{`^[^.]+\.wild\.example$`}) {
		t.Errorf("domain_regex = %v", got)
	}
}

// 纯十六进制字面的域名不能被误判成 IP。误判在 domainset 断言下会让整个规则集陪葬。
func TestHexLookingDomainsAreNotIPs(t *testing.T) {
	for _, host := range []string{"bad.cc", "cafe.fee", "dead.beef", "ab.cd"} {
		set := mustParse(t, host, FormatAuto)
		if got := values(t, set, ruleset.FieldDomain); !reflect.DeepEqual(got, []string{host}) {
			t.Errorf("%q 被判成了 IP: domain=%v ip_cidr=%v", host, got, values(t, set, ruleset.FieldIPCIDR))
		}
	}
}

// Clash 的 * 严格匹配一个 label，只能落到 domain_regex；
// 用 domain_suffix 近似会把多级子域也吃进来。
func TestWildcardOnlyWholeLabel(t *testing.T) {
	set := mustParse(t, "*.a.example\nad*.b.example\n", FormatAuto)
	if got := values(t, set, ruleset.FieldDomainRegex); !reflect.DeepEqual(got, []string{`^[^.]+\.a\.example$`}) {
		t.Errorf("domain_regex = %v", got)
	}
	// ad*.b.example 无法表达，当普通域名处理 → 归一失败 → 记账
	if set.Diag.InvalidTotal() != 1 {
		t.Errorf("部分通配符应当被记成非法值，实际诊断 %+v", set.Diag.Invalid)
	}
}

// ---------------- 正则值截断 ----------------

func TestRegexValueStopsAtTopLevelComma(t *testing.T) {
	cases := []struct{ in, want string }{
		{`DOMAIN-REGEX,^a{1,3}\.com$,PROXY`, `^a{1,3}\.com$`},
		{`DOMAIN-REGEX,^[a,b]\.com$`, `^[a,b]\.com$`},
		{`DOMAIN-REGEX,^(a|b),DIRECT`, `^(a|b)`},
		{`DOMAIN-REGEX,^a\,b$`, `^a\,b$`},
		{`DOMAIN-REGEX,^plain$`, `^plain$`},
	}
	for _, c := range cases {
		set := mustParse(t, c.in, FormatAuto)
		if got := values(t, set, ruleset.FieldDomainRegex); !reflect.DeepEqual(got, []string{c.want}) {
			t.Errorf("%q → %v, want [%s]", c.in, got, c.want)
		}
	}
}

// ---------------- 逻辑规则 ----------------

func TestLogicalRule(t *testing.T) {
	set := mustParse(t, `AND,((DOMAIN-SUFFIX,example.com),(DST-PORT,443))`, FormatAuto)
	if len(set.Verbatim()) != 1 {
		t.Fatalf("应当产出一条逻辑规则，实际 %d 条；诊断 %+v", len(set.Verbatim()), set.Diag)
	}
	raw, _ := json.Marshal(set.Verbatim()[0])
	got := string(raw)
	if !strings.Contains(got, `"mode":"and"`) {
		t.Errorf("mode 不对: %s", got)
	}
	// 端口必须是数字 —— 逻辑规则绕开了 Add 的归一管道，忘了归一会产出
	// 字符串端口，sing-box 直接拒绝编译整个规则集。
	if !strings.Contains(got, `"port":443`) {
		t.Errorf("端口没归一成数字: %s", got)
	}
}

func TestLogicalNotBecomesInvert(t *testing.T) {
	set := mustParse(t, `NOT,((DOMAIN,a.example.com))`, FormatAuto)
	if len(set.Verbatim()) != 1 {
		t.Fatalf("诊断 %+v", set.Diag)
	}
	raw, _ := json.Marshal(set.Verbatim()[0])
	if !strings.Contains(string(raw), `"invert":true`) {
		t.Errorf("NOT 应当变成 invert: %s", raw)
	}
}

// 逻辑规则任一子项无法表达时整条丢弃 —— AND 少一项等于放宽匹配面，
// 输出出去比丢掉更危险。
func TestLogicalDropsWholeRuleOnBadChild(t *testing.T) {
	for _, body := range []string{
		`AND,((DOMAIN,a.example.com),(GEOIP,CN))`,       // 子项类型表达不了
		`AND,((DOMAIN,a.example.com),(DST-PORT,99999))`, // 子项取值非法
	} {
		set := mustParse(t, body, FormatAuto)
		if len(set.Verbatim()) != 0 {
			t.Errorf("%q 应当整条丢弃，实际留下 %d 条", body, len(set.Verbatim()))
		}
		if set.Diag.InvalidTotal() == 0 {
			t.Errorf("%q 丢弃时未记账", body)
		}
	}
}

// ---------------- 容器判定 ----------------

func TestProbeJSON(t *testing.T) {
	body := `{"version":4,"rules":[{"domain":["a.example.com"],"domain_suffix":"b.example"}]}`
	set := mustParse(t, body, FormatAuto)
	if got := values(t, set, ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"a.example.com"}) {
		t.Errorf("domain = %v", got)
	}
	// 标量和数组两种写法都要接受
	if got := values(t, set, ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"b.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
}

// 树提取器不要求顶层是合法 rule-set —— 任意包装结构都能吃。
func TestTreeAcceptsArbitraryWrappers(t *testing.T) {
	for _, body := range []string{
		`{"rules":[{"domain":"a.example.com"}]}`,
		`[{"domain":"a.example.com"}]`,
		`{"whatever":{"nested":{"deeper":[{"domain":"a.example.com"}]}}}`,
		`{"payload":["DOMAIN,a.example.com"]}`,
	} {
		set := mustParse(t, body, FormatAuto)
		if got := values(t, set, ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"a.example.com"}) {
			t.Errorf("%s → domain = %v", body, got)
		}
	}
}

// 含未知字段的规则必须整条透传，不能只挑认识的字段拿 ——
// 悄悄丢掉 query_type 等于去掉一个条件，规则会匹配到更多流量。
func TestUnknownFieldForcesPassthrough(t *testing.T) {
	set := mustParse(t, `{"rules":[{"domain":"a.example.com","query_type":["A"]}]}`, FormatAuto)
	if got := values(t, set, ruleset.FieldDomain); got != nil {
		t.Errorf("不该拆开，实际 domain = %v", got)
	}
	if len(set.Verbatim()) != 1 {
		t.Fatalf("应当整条透传，实际 %d 条；诊断 %+v", len(set.Verbatim()), set.Diag)
	}
	raw, _ := json.Marshal(set.Verbatim()[0])
	if !strings.Contains(string(raw), "query_type") {
		t.Errorf("透传时丢了 query_type: %s", raw)
	}
}

func TestLogicalInJSONIsPassedThrough(t *testing.T) {
	body := `{"rules":[{"type":"logical","mode":"or","rules":[{"domain":"a.example.com"},{"domain":"b.example.com"}]}]}`
	set := mustParse(t, body, FormatAuto)
	if len(set.Verbatim()) != 1 {
		t.Fatalf("逻辑规则应当整条透传，实际 %d 条", len(set.Verbatim()))
	}
	if set.Values(ruleset.FieldDomain) != nil {
		t.Error("逻辑规则的值不该被拆出来跟别的规则混着去重")
	}
}

// 残缺的 JSON 不回退成按行解析 —— 回退会解析出一堆看不出异常的垃圾域名。
func TestBrokenJSONIsHardError(t *testing.T) {
	if err := Into([]byte(`{"rules":[{"domain":"a.com"}`), FormatAuto, ruleset.New("t")); err == nil {
		t.Error("残缺 JSON 应当报错而不是回退")
	}
}

// Surge / Quantumult X 的整份配置以 [General] 开头 —— 撞进 JSON 分支硬报错是对的，
// 那不是规则列表。
func TestSurgeConfigIsRejected(t *testing.T) {
	if err := Into([]byte("[General]\nloglevel = notify\n"), FormatAuto, ruleset.New("t")); err == nil {
		t.Error("整份 Surge 配置应当报错")
	}
}

// ---------------- YAML ----------------

// 真实的 Clash YAML rule-provider：payload + 扁平列表。行提取器直接吃下。
func TestClashYAMLGoesThroughLineExtractor(t *testing.T) {
	body := `payload:
  # Alibaba
  - DOMAIN,httpdns.alicdn.com
  - IP-CIDR,203.107.1.0/24
  - '+.quoted.example'
`
	set := mustParse(t, body, FormatAuto)
	if got := values(t, set, ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"httpdns.alicdn.com"}) {
		t.Errorf("domain = %v", got)
	}
	if got := values(t, set, ruleset.FieldIPCIDR); !reflect.DeepEqual(got, []string{"203.107.1.0/24"}) {
		t.Errorf("ip_cidr = %v", got)
	}
	if got := values(t, set, ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"quoted.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	// payload: 被认成容器键，不该产生噪声诊断
	if set.Diag.InvalidTotal() != 0 {
		t.Errorf("不该有非法值，实际 %+v / %v", set.Diag.Invalid, set.Diag.InvalidSamples)
	}
}

// YAML 嵌套映射是取消 YAML 分支后唯一会静默出错的形态，必须硬报错。
func TestYAMLNestedMappingIsRejected(t *testing.T) {
	body := `payload:
  DOMAIN-SUFFIX:
    - a.example.com
`
	err := Into([]byte(body), FormatAuto, ruleset.New("t"))
	if err == nil {
		t.Fatal("嵌套映射应当报错 —— 按行解析会把 a.example.com 判成 domain 而不是 domain_suffix")
	}
	if !strings.Contains(err.Error(), "DOMAIN-SUFFIX") {
		t.Errorf("报错信息应当点名那个键: %v", err)
	}
}

// ---------------- 断言格式 ----------------

func TestCIDRAssertion(t *testing.T) {
	set := mustParse(t, "1.2.3.0/24\n2001:db8::/32\n", FormatCIDR)
	if got := values(t, set, ruleset.FieldIPCIDR); len(got) != 2 {
		t.Errorf("ip_cidr = %v", got)
	}
	// 混进域名即失败 —— 防的正是上游返回 HTTP 200 的 HTML 错误页
	if err := Into([]byte("1.2.3.0/24\nexample.com\n"), FormatCIDR, ruleset.New("t")); err == nil {
		t.Error("cidr 断言下混入域名应当报错")
	}
	if err := Into([]byte("<html><body>404</body></html>\n"), FormatCIDR, ruleset.New("t")); err == nil {
		t.Error("cidr 断言应当挡下 HTML 错误页")
	}
}

func TestDomainSetAssertion(t *testing.T) {
	// 通配符域名是域名列表的常客，转换后落在 domain_regex 上，必须放行
	set := mustParse(t, "example.com\n.sub.example\n*.wild.example\n", FormatDomainSet)
	if set.Total() != 3 {
		t.Errorf("Total = %d, want 3", set.Total())
	}
	if err := Into([]byte("example.com\n1.2.3.0/24\n"), FormatDomainSet, ruleset.New("t")); err == nil {
		t.Error("domainset 断言下混入 IP 应当报错")
	}
}

func TestFormatParsing(t *testing.T) {
	for _, raw := range []string{"", "adguard", "cidr", "domainset"} {
		if _, err := ParseFormat(raw); err != nil {
			t.Errorf("ParseFormat(%q) 报错: %v", raw, err)
		}
	}
	// 旧的四个取值现在都不用写了
	for _, raw := range []string{"singbox", "yaml", "text", "srs"} {
		if _, err := ParseFormat(raw); err == nil {
			t.Errorf("ParseFormat(%q) 应当报错", raw)
		}
	}
}

// ---------------- .srs 作为输入 ----------------

// 链接 sing-box 之后白捡的能力：不少上游只发 .srs 不发 JSON。
func TestSRSAsInput(t *testing.T) {
	origin := ruleset.New("origin")
	origin.AddLenient(ruleset.FieldDomain, "a.example.com")
	origin.AddLenient(ruleset.FieldDomainSuffix, "b.example")
	origin.AddLenient(ruleset.FieldIPCIDR, "1.2.3.0/24")

	var buf strings.Builder
	if err := emit.SRS(&buf, origin, 4); err != nil {
		t.Fatal(err)
	}

	set := mustParse(t, buf.String(), FormatAuto)
	if set.Total() == 0 {
		t.Fatal("没能从 .srs 里读出任何规则")
	}
	raw, _ := json.Marshal(set.Options())
	for _, want := range []string{"a.example.com", "b.example", "1.2.3.0/24"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("读回的内容里缺 %q: %s", want, raw)
		}
	}
}

func TestTruncatedSRSIsHardError(t *testing.T) {
	if err := Into([]byte("SRS\x04garbage"), FormatAuto, ruleset.New("t")); err == nil {
		t.Error("魔数匹配但内容损坏时应当报错，不能回退按行解析")
	}
}

// 以 SRS 开头的文本文件不该被误判 —— 第四字节必须是 1-5 这种控制字符。
func TestTextStartingWithSRSIsNotMisdetected(t *testing.T) {
	set := mustParse(t, "SRSomething.example.com\nother.example\n", FormatAuto)
	if got := values(t, set, ruleset.FieldDomain); len(got) != 2 {
		t.Errorf("domain = %v, want 2 条", got)
	}
}

// ---------------- 混合 ----------------

// 同一个规则集喂多份不同形态的源，值应当合并去重。
func TestMixedSourcesMergeAndDedupe(t *testing.T) {
	set := ruleset.New("mixed")
	inputs := []string{
		`{"version":4,"rules":[{"domain_suffix":["example.com"]}]}`, // sing-box JSON
		"DOMAIN-SUFFIX,example.com,PROXY\n",                         // Clash .list
		"+.example.com\n",                                           // 裸值
		"payload:\n  - DOMAIN-SUFFIX,example.com\n",                 // Clash YAML
	}
	for _, in := range inputs {
		if err := Into([]byte(in), FormatAuto, set); err != nil {
			t.Fatalf("%q: %v", in, err)
		}
	}
	if got := values(t, set, ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com"}) {
		t.Errorf("四个源说同一件事，应当合并成一条，实际 %v", got)
	}
}
