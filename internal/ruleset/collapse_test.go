package ruleset

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sagernet/sing/common/domain"
)

// suffixCovers 是手写谓词 —— 把整个后缀集合塞进 matcher 的话每条都会命中自己，
// 没法用它判后缀之间的覆盖。这个测试拿真 matcher 当 oracle，逐对核对手写谓词
// 的语义没写歪。
func TestSuffixCoversAgreesWithMatcher(t *testing.T) {
	suffixes := []string{
		"a.com", ".a.com",
		"b.a.com", ".b.a.com",
		"c.b.a.com",
		"com", ".com",
		"ample.com", "example.com", // 字符串后缀陷阱：HasSuffix 成立但不是子域
		"x.org", ".x.org",
	}
	for _, s := range suffixes {
		for _, other := range suffixes {
			got := suffixCovers(s, other)
			want := oracleCovers(s, other)
			if got != want {
				t.Errorf("suffixCovers(%q, %q) = %v, matcher 说 %v", s, other, got, want)
			}
		}
	}
}

// oracleCovers 用 sing-box 真正的 matcher 判断：other 能匹配的代表性名字，
// s 是不是也全都能匹配。
func oracleCovers(s, other string) bool {
	m := domain.NewMatcher(nil, []string{s}, false)
	for _, name := range membersOf(other) {
		if !m.Match(name) {
			return false
		}
	}
	return true
}

// membersOf 给出后缀匹配集合的代表元。
func membersOf(suffix string) []string {
	base, includesSelf := suffixBase(suffix)
	out := []string{"sub." + base, "deep.sub." + base}
	if includesSelf {
		out = append(out, base)
	}
	return out
}

// 把前导点的语义单独钉住 —— 全部四条变换都建立在它上面。
func TestSuffixSemantics(t *testing.T) {
	bare := domain.NewMatcher(nil, []string{"example.com"}, false)
	dotted := domain.NewMatcher(nil, []string{".example.com"}, false)

	if !bare.Match("example.com") {
		t.Error("无点后缀应当匹配 example.com 本身")
	}
	if !bare.Match("a.example.com") {
		t.Error("无点后缀应当匹配子域")
	}
	if dotted.Match("example.com") {
		t.Error("带点后缀不应当匹配 example.com 本身")
	}
	if !dotted.Match("a.example.com") {
		t.Error("带点后缀应当匹配子域")
	}
	if bare.Match("notexample.com") {
		t.Error("后缀匹配是按 label 边界的，不该命中 notexample.com")
	}
}

func TestCollapseDottedPair(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomain, "example.com")
	s.AddLenient(FieldDomainSuffix, ".example.com")

	if n := s.Collapse(); n != 1 {
		t.Errorf("删除条数 = %d, want 1", n)
	}
	if got := s.Values(FieldDomain); got != nil {
		t.Errorf("domain 应当清空，实际 %v", got)
	}
	if got := s.Values(FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com"}) {
		t.Errorf("domain_suffix = %v, want [example.com]", got)
	}
}

// 设计里那个"三个源说同一件事"的场景：Clash 给无点后缀、skk domainset 给带点
// 后缀、geosite 的 RootDomain 给 domain + 带点后缀。三个规则值，收敛后剩一条。
func TestCollapseThreeSourcesSayingTheSameThing(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomainSuffix, "example.com")
	s.AddLenient(FieldDomainSuffix, ".example.com")
	s.AddLenient(FieldDomain, "example.com")
	if s.Total() != 3 {
		t.Fatalf("前置条件错了，Total = %d", s.Total())
	}

	s.Collapse()
	if got := s.Values(FieldDomain); got != nil {
		t.Errorf("domain = %v, want 空", got)
	}
	if got := s.Values(FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com"}) {
		t.Errorf("domain_suffix = %v, want [example.com]", got)
	}
	if s.Total() != 1 {
		t.Errorf("Total = %d, want 1", s.Total())
	}
}

func TestCollapseNestedSuffixes(t *testing.T) {
	s := New("t")
	for _, v := range []string{"a.com", "b.a.com", ".c.b.a.com", "x.org"} {
		s.AddLenient(FieldDomainSuffix, v)
	}
	s.Collapse()
	got := s.Values(FieldDomainSuffix)
	want := []string{"a.com", "x.org"} // b.a.com 与 .c.b.a.com 都被 a.com 覆盖
	if !reflect.DeepEqual(got, want) {
		t.Errorf("= %v, want %v", got, want)
	}
}

func TestCollapseDomainsUnderSuffix(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomainSuffix, "example.com")
	for _, d := range []string{"a.example.com", "example.com", "notexample.com", "other.org"} {
		s.AddLenient(FieldDomain, d)
	}
	s.Collapse()
	got := s.Values(FieldDomain)
	// notexample.com 是字符串后缀但不是子域，必须留着
	want := []string{"notexample.com", "other.org"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("= %v, want %v", got, want)
	}
}

// 带点后缀不覆盖 base 本身 —— 这是最容易写错的一条。
func TestCollapseDottedSuffixKeepsBareDomain(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomainSuffix, ".example.com")
	s.AddLenient(FieldDomain, "a.example.com") // 被覆盖
	s.AddLenient(FieldDomain, "other.org")
	s.Collapse()
	// example.com 本身不在集合里，所以 .example.com 不会被合并成无点形式
	if got := s.Values(FieldDomainSuffix); !reflect.DeepEqual(got, []string{".example.com"}) {
		t.Errorf("domain_suffix = %v, want [.example.com]", got)
	}
	if got := s.Values(FieldDomain); !reflect.DeepEqual(got, []string{"other.org"}) {
		t.Errorf("domain = %v, want [other.org]", got)
	}
}

func TestCollapseByKeyword(t *testing.T) {
	s := New("t")
	s.AddLenient(FieldDomainKeyword, "analytics")
	s.AddLenient(FieldDomain, "google-analytics.com")
	s.AddLenient(FieldDomain, "keep.example.com")
	s.AddLenient(FieldDomainSuffix, ".analytics.example")
	s.AddLenient(FieldDomainSuffix, "keep.org")

	s.Collapse()
	if got := s.Values(FieldDomain); !reflect.DeepEqual(got, []string{"keep.example.com"}) {
		t.Errorf("domain = %v", got)
	}
	if got := s.Values(FieldDomainSuffix); !reflect.DeepEqual(got, []string{"keep.org"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	if got := s.Values(FieldDomainKeyword); !reflect.DeepEqual(got, []string{"analytics"}) {
		t.Errorf("keyword 自己不该被动，实际 %v", got)
	}
}

// 收敛必须幂等：跑第二遍不能再删任何东西，否则说明某两条变换在互相拆台。
func TestCollapseIsIdempotent(t *testing.T) {
	build := func() *RuleSet {
		s := New("t")
		for _, v := range []string{"a.com", ".a.com", "b.a.com", ".x.org", "y.net"} {
			s.AddLenient(FieldDomainSuffix, v)
		}
		for _, v := range []string{"a.com", "deep.a.com", "z.net", "ads.tracker.io"} {
			s.AddLenient(FieldDomain, v)
		}
		s.AddLenient(FieldDomainKeyword, "tracker")
		return s
	}
	s := build()
	if first := s.Collapse(); first == 0 {
		t.Fatal("第一遍什么都没删，用例失效")
	}
	if second := s.Collapse(); second != 0 {
		t.Errorf("第二遍又删了 %d 条，收敛不是幂等的", second)
	}

	want := s.Values(FieldDomainSuffix)
	for i := 0; i < 20; i++ {
		other := build()
		other.Collapse()
		if got := other.Values(FieldDomainSuffix); !reflect.DeepEqual(got, want) {
			t.Fatalf("收敛结果不稳定: %v vs %v", got, want)
		}
	}
}

// 收敛不改变匹配结果 —— 这是"默认开启"唯一的依据。拿收敛前后的规则各建一个
// matcher，对一批探针域名逐个比对。
func TestCollapsePreservesMatching(t *testing.T) {
	build := func() *RuleSet {
		s := New("t")
		for _, v := range []string{"a.com", ".a.com", "b.a.com", ".c.b.a.com", "x.org", ".keep.net"} {
			s.AddLenient(FieldDomainSuffix, v)
		}
		for _, v := range []string{"a.com", "deep.a.com", "lone.example", "notexample.com"} {
			s.AddLenient(FieldDomain, v)
		}
		s.AddLenient(FieldDomainKeyword, "tracker")
		return s
	}
	before := build()
	after := build()
	after.Collapse()

	probes := []string{
		"a.com", "sub.a.com", "b.a.com", "sub.b.a.com", "c.b.a.com", "deep.c.b.a.com",
		"x.org", "sub.x.org", "keep.net", "sub.keep.net",
		"lone.example", "sub.lone.example",
		"notexample.com", "example.com",
		"ads.tracker.io", "tracker", "unrelated.test",
	}
	bm := buildMatcher(before)
	am := buildMatcher(after)
	for _, p := range probes {
		if bm(p) != am(p) {
			t.Errorf("探针 %q: 收敛前 %v, 收敛后 %v", p, bm(p), am(p))
		}
	}
	if after.Total() >= before.Total() {
		t.Errorf("收敛没减少条数: %d → %d", before.Total(), after.Total())
	}
	t.Logf("条数 %d → %d", before.Total(), after.Total())
}

// buildMatcher 用 sing-box 的 matcher 复现一个规则集对域名的判定。
func buildMatcher(s *RuleSet) func(string) bool {
	suffixes := nonEmpty(s.Values(FieldDomainSuffix))
	domains := s.Values(FieldDomain)
	var m *domain.Matcher
	if len(suffixes) > 0 || len(domains) > 0 {
		m = domain.NewMatcher(domains, suffixes, false)
	}
	keywords := s.Values(FieldDomainKeyword)
	return func(name string) bool {
		if m != nil && m.Match(name) {
			return true
		}
		for _, k := range keywords {
			if strings.Contains(name, k) {
				return true
			}
		}
		return false
	}
}
