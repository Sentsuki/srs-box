package ruleset

import (
	"fmt"
	"testing"
)

// 真实规模下的收敛开销。geosite 的 cn、skk 的 reject 都是十万量级，
// 其中 reject 还带着几百个 domain_keyword —— 关键词那一步是 O(值数 × 关键词数)，
// 是四条变换里唯一可能变慢的地方，这里把它量出来。
func BenchmarkCollapseLarge(b *testing.B) {
	for _, size := range []int{10_000, 50_000} {
		b.Run(fmt.Sprintf("values=%d", size), func(b *testing.B) {
			// 每轮都要一份新数据（收敛是幂等的，跑第二遍什么都不删），
			// 而构造数据不该计进耗时 —— b.Loop 自带这个语义，
			// 不必再手工 StopTimer/StartTimer。
			var s *RuleSet
			for b.Loop() {
				b.StopTimer()
				s = largeSet(size, 500)
				b.StartTimer()
				s.Collapse()
			}
		})
	}
}

// 只有后缀、没有关键词的场景 —— 用来看祖先查表那一步本身的开销。
func BenchmarkCollapseSuffixesOnly(b *testing.B) {
	var s *RuleSet
	for b.Loop() {
		b.StopTimer()
		s = largeSet(50_000, 0)
		b.StartTimer()
		s.Collapse()
	}
}

func largeSet(values, keywords int) *RuleSet {
	s := New("bench")
	for i := range values {
		// 制造真实的层级关系：每 20 个共享一个二级域，于是会有大量可收敛项。
		parent := fmt.Sprintf("p%d.example", i/20)
		switch i % 4 {
		case 0:
			s.AddLenient(FieldDomainSuffix, parent)
		case 1:
			s.AddLenient(FieldDomainSuffix, "."+parent)
		case 2:
			s.AddLenient(FieldDomain, fmt.Sprintf("h%d.%s", i, parent))
		case 3:
			s.AddLenient(FieldDomain, fmt.Sprintf("t%d.tracker%d.io", i, i%37))
		}
	}
	for i := range keywords {
		s.AddLenient(FieldDomainKeyword, fmt.Sprintf("tracker%d", i))
	}
	return s
}

func TestCollapseLargeIsSane(t *testing.T) {
	s := largeSet(20_000, 200)
	before := s.Total()
	removed := s.Collapse()
	after := s.Total()
	if before-removed != after {
		t.Errorf("记账不平: %d - %d != %d", before, removed, after)
	}
	if removed == 0 {
		t.Fatal("这么多层级关系一条都没收敛，不对")
	}
	if n := s.Collapse(); n != 0 {
		t.Errorf("大规模下不幂等，第二遍又删了 %d 条", n)
	}
	t.Logf("%d → %d（删掉 %d）", before, after, removed)
}
