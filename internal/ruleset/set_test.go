package ruleset

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

// Options 的 setStrings/setPorts 是手写 switch。漏掉一个 case 的后果是那个字段
// 的值被静默丢弃 —— 产物少一批规则而不报错。这个测试让它当场暴露。
func TestOptionsCoversAllFields(t *testing.T) {
	for _, f := range AllFields() {
		s := New("t")
		if f.IsPort() {
			s.AddPort(f, 443)
		} else if err := s.Add(f, sampleValue(f)); err != nil {
			t.Fatalf("Add(%s, %q) 报错: %v", f, sampleValue(f), err)
		}
		raw, err := json.Marshal(s.Options())
		if err != nil {
			t.Fatalf("%s: marshal 报错: %v", f, err)
		}
		if !strings.Contains(string(raw), `"`+f.String()+`"`) {
			t.Errorf("字段 %s 没进 Options()，setStrings/setPorts 缺 case；实际 %s", f, raw)
		}
	}
}

// 字段名必须是 sing-box 真实的 json tag。升级依赖时上游改名会在这里暴露，
// 而不是等到产物被 sing-box 拒绝。
func TestFieldNamesMatchSingBox(t *testing.T) {
	tags := map[string]bool{}
	rt := reflect.TypeFor[option.DefaultHeadlessRule]()
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("json")
		if name, _, _ := strings.Cut(tag, ","); name != "" && name != "-" {
			tags[name] = true
		}
	}
	for _, f := range AllFields() {
		if !tags[f.String()] {
			t.Errorf("字段 %q 不是 option.DefaultHeadlessRule 的 json tag", f)
		}
	}
	t.Logf("sing-box 共 %d 个 headless rule 字段，我们拆开处理 %d 个，其余走 verbatim", len(tags), len(AllFields()))
}

func sampleValue(f Field) string {
	switch f {
	case FieldNetwork:
		return "tcp"
	case FieldDomain, FieldDomainSuffix:
		return "example.com"
	case FieldDomainKeyword:
		return "example"
	case FieldDomainRegex:
		return `^a\.example\.com$`
	case FieldSourceIPCIDR, FieldIPCIDR:
		return "1.2.3.0/24"
	default:
		return "Telegram"
	}
}

func TestOptionsIsDeterministic(t *testing.T) {
	build := func() string {
		s := New("t")
		// 故意乱序插入：map 迭代顺序随机，排序必须在 Values/Ports 里定死。
		for _, d := range []string{"z.com", "a.com", "m.com", "a.com"} {
			s.AddLenient(FieldDomain, d)
		}
		for _, p := range []uint16{443, 80, 8080, 80} {
			s.AddPort(FieldPort, p)
		}
		raw, err := json.Marshal(s.Options())
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	first := build()
	for i := 0; i < 20; i++ {
		if got := build(); got != first {
			t.Fatalf("产出不稳定:\n%s\n%s", first, got)
		}
	}
	if !strings.Contains(first, `"a.com","m.com","z.com"`) {
		t.Errorf("域名未排序去重: %s", first)
	}
}

func TestVerbatimDedupe(t *testing.T) {
	rule := logicalRule()
	s := New("t")
	s.AddVerbatim(rule)
	s.AddVerbatim(rule)
	s.AddVerbatim(logicalRule()) // 另建一个但内容相同
	if got := len(s.Verbatim()); got != 1 {
		t.Errorf("相同的透传规则应当只留一条，实际 %d 条", got)
	}
}

func logicalRule() option.HeadlessRule {
	return option.HeadlessRule{
		Type: C.RuleTypeLogical,
		LogicalOptions: option.LogicalHeadlessRule{
			Mode:   C.LogicalTypeAnd,
			Invert: true,
			Rules: []option.HeadlessRule{
				{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{
					Domain: badoption.Listable[string]{"a.example.com"},
				}},
				{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{
					Port: badoption.Listable[uint16]{443},
				}},
			},
		},
	}
}

func TestAggregateCIDR(t *testing.T) {
	s := New("t")
	for _, c := range []string{"1.2.0.0/24", "1.2.1.0/24", "1.2.3.0/24", "2001:db8::/33", "2001:db8:8000::/33"} {
		if !s.AddLenient(FieldIPCIDR, c) {
			t.Fatalf("AddLenient(%q) 失败", c)
		}
	}
	removed := s.AggregateCIDR()
	got := s.Values(FieldIPCIDR)
	// 1.2.0.0/24 + 1.2.1.0/24 → 1.2.0.0/23；两个 /33 → 2001:db8::/32
	want := []string{"1.2.0.0/23", "1.2.3.0/24", "2001:db8::/32"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("聚合结果 = %v, want %v", got, want)
	}
	if removed != 2 {
		t.Errorf("减少条数 = %d, want 2", removed)
	}
	if s.Diag.Aggregated != 2 {
		t.Errorf("诊断未记账: %d", s.Diag.Aggregated)
	}
}

// 水印过滤必须同时作用于透传规则 —— 只扫 plain 的话，带 invert 的规则里的
// 水印域名会原样漏进产物。
func TestDropValuesContainingHitsVerbatim(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomain, "keep.example.com")
	s.AddLenient(FieldDomain, "watermark.skk.moe")
	s.AddVerbatim(option.HeadlessRule{
		Type: C.RuleTypeLogical,
		LogicalOptions: option.LogicalHeadlessRule{
			Mode: C.LogicalTypeAnd,
			Rules: []option.HeadlessRule{{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{
				Domain: badoption.Listable[string]{"nested.skk.moe"},
			}}},
		},
	})
	s.AddVerbatim(logicalRule())

	removed := s.DropValuesContaining([]string{"SKK.MOE"}) // 大小写不敏感
	if removed != 2 {
		t.Errorf("删除条数 = %d, want 2（一个 plain 值 + 一条逻辑规则）", removed)
	}
	if got := s.Values(FieldDomain); !reflect.DeepEqual(got, []string{"keep.example.com"}) {
		t.Errorf("plain 过滤结果 = %v", got)
	}
	if got := len(s.Verbatim()); got != 1 {
		t.Errorf("透传规则剩 %d 条, want 1", got)
	}
}

// 过滤词不能命中字段名：拿整段 JSON 做子串匹配的话，"domain" 这种词会
// 把每一条规则都删掉。
func TestDropValuesContainingIgnoresFieldNames(t *testing.T) {
	s := New("t")
	s.AddVerbatim(logicalRule())
	if removed := s.DropValuesContaining([]string{"domain"}); removed != 0 {
		t.Errorf("过滤词命中了字段名，删掉了 %d 条", removed)
	}
}

func TestSubtract(t *testing.T) {
	s := New("t")
	for _, d := range []string{"a.com", "b.com", "c.com"} {
		s.AddLenient(FieldDomain, d)
	}
	s.AddPort(FieldPort, 80)
	s.AddPort(FieldPort, 443)
	s.AddVerbatim(logicalRule())

	drop := New("drop")
	drop.AddLenient(FieldDomain, "B.COM") // 归一后应当能对上
	drop.AddPort(FieldPort, 80)
	drop.AddVerbatim(logicalRule())

	removed := s.Subtract(drop)
	if removed != 3 {
		t.Errorf("删除条数 = %d, want 3", removed)
	}
	if got := s.Values(FieldDomain); !reflect.DeepEqual(got, []string{"a.com", "c.com"}) {
		t.Errorf("域名差集 = %v", got)
	}
	if got := s.Ports(FieldPort); !reflect.DeepEqual(got, []uint16{443}) {
		t.Errorf("端口差集 = %v", got)
	}
	if got := len(s.Verbatim()); got != 0 {
		t.Errorf("完全相同的透传规则应当被减掉，剩 %d 条", got)
	}
}

func TestCountsAndTotal(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomain, "a.com")
	s.AddLenient(FieldDomainSuffix, ".b.com")
	s.AddPort(FieldPort, 443)
	s.AddVerbatim(logicalRule())
	if got := s.Total(); got != 4 {
		t.Errorf("Total() = %d, want 4", got)
	}
	if s.Empty() {
		t.Error("Empty() 应为 false")
	}
	counts := s.Counts()
	// 顺序必须跟字段声明顺序一致，verbatim 垫在最后
	want := []string{"domain", "domain_suffix", "port", "verbatim"}
	for i, w := range want {
		if i >= len(counts) || counts[i].Name != w {
			t.Fatalf("Counts() = %+v, want 顺序 %v", counts, want)
		}
	}
}

func TestAddLenientRecordsDiagnostics(t *testing.T) {
	s := New("t")
	if s.AddLenient(FieldDomain, "-bad-.com") {
		t.Error("非法域名不应当被接受")
	}
	if s.Diag.InvalidTotal() != 1 {
		t.Errorf("非法值计数 = %d, want 1", s.Diag.InvalidTotal())
	}
	if len(s.Diag.InvalidSamples) != 1 {
		t.Errorf("样例数 = %d, want 1", len(s.Diag.InvalidSamples))
	}
}
