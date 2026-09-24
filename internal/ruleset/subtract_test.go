package ruleset

import (
	"fmt"
	"math/rand"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/sagernet/sing/common/domain"
	"go4.org/netipx"
)

type val struct {
	f Field
	v string
}

func build(t *testing.T, name string, vals []val) *RuleSet {
	t.Helper()
	s := New(name)
	for _, p := range vals {
		if !s.AddLenient(p.f, p.v) {
			t.Fatalf("%s: 加不进去 %s:%s", name, p.f, p.v)
		}
	}
	return s
}

// dump 把集合里的字符串值列成 "字段:值"，按字段声明顺序 + 值字典序。
func dump(s *RuleSet) []string {
	var out []string
	for _, f := range AllFields() {
		for _, v := range s.Values(f) {
			out = append(out, f.String()+":"+v)
		}
	}
	return out
}

// 差集的覆盖矩阵。每一行都是一种"两边说同一件事、但编码不同"的真实组合 ——
// 改成字面差集的话这张表里只有前两行能过。
func TestSubtractCoverageMatrix(t *testing.T) {
	cases := []struct {
		name    string
		main    []val
		drop    []val
		removed int
		want    []string
		// 改窄与表达不了的计数，0 表示不该出现
		narrowed      int
		unexpressible int
	}{{
		name:    "字面相同的 suffix",
		main:    []val{{FieldDomainSuffix, "a.com"}},
		drop:    []val{{FieldDomainSuffix, "a.com"}},
		removed: 1,
	}, {
		name:    "字面相同的 domain",
		main:    []val{{FieldDomain, "a.com"}},
		drop:    []val{{FieldDomain, "a.com"}},
		removed: 1,
	}, {
		// Clash 的 +.a.com 对上 geosite RootDomain 的两条编码。
		name:    "无点 suffix 对上 domain + 带点 suffix",
		main:    []val{{FieldDomainSuffix, "a.com"}},
		drop:    []val{{FieldDomain, "a.com"}, {FieldDomainSuffix, ".a.com"}},
		removed: 1,
	}, {
		name:    "domain 被无点 suffix 覆盖",
		main:    []val{{FieldDomain, "x.a.com"}},
		drop:    []val{{FieldDomainSuffix, "a.com"}},
		removed: 1,
	}, {
		name:    "domain 被带点 suffix 覆盖",
		main:    []val{{FieldDomain, "x.a.com"}},
		drop:    []val{{FieldDomainSuffix, ".a.com"}},
		removed: 1,
	}, {
		name:    "带点 suffix 被祖先后缀覆盖",
		main:    []val{{FieldDomainSuffix, ".x.a.com"}},
		drop:    []val{{FieldDomainSuffix, "a.com"}},
		removed: 1,
	}, {
		name:    "domain 被 keyword 覆盖",
		main:    []val{{FieldDomain, "ads.a.com"}},
		drop:    []val{{FieldDomainKeyword, "ads."}},
		removed: 1,
	}, {
		name:    "suffix 被 keyword 覆盖",
		main:    []val{{FieldDomainSuffix, "ads.a.com"}},
		drop:    []val{{FieldDomainKeyword, "ads."}},
		removed: 1,
	}, {
		name:    "keyword 被更宽的 keyword 覆盖",
		main:    []val{{FieldDomainKeyword, "ads.google"}},
		drop:    []val{{FieldDomainKeyword, "ads."}},
		removed: 1,
	}, {
		// 反方向不成立：窄关键词盖不住宽关键词。
		name: "更窄的 keyword 盖不住",
		main: []val{{FieldDomainKeyword, "ads."}},
		drop: []val{{FieldDomainKeyword, "ads.google"}},
		want: []string{"domain_keyword:ads."},
	}, {
		name:     "无点 suffix 减掉 apex 变带点",
		main:     []val{{FieldDomainSuffix, "a.com"}},
		drop:     []val{{FieldDomain, "a.com"}},
		want:     []string{"domain_suffix:.a.com"},
		narrowed: 1,
	}, {
		name:     "无点 suffix 减掉子域变 domain",
		main:     []val{{FieldDomainSuffix, "a.com"}},
		drop:     []val{{FieldDomainSuffix, ".a.com"}},
		want:     []string{"domain:a.com"},
		narrowed: 1,
	}, {
		// 这条是设计边界：写不出"a.com 及其子域但不含 x.a.com"。
		name:          "exclude 更窄，表达不了",
		main:          []val{{FieldDomainSuffix, "a.com"}},
		drop:          []val{{FieldDomain, "x.a.com"}},
		want:          []string{"domain_suffix:a.com"},
		unexpressible: 1,
	}, {
		name: "label 边界：ample.com 不是 example.com 的祖先",
		main: []val{{FieldDomain, "example.com"}},
		drop: []val{{FieldDomainSuffix, "ample.com"}},
		want: []string{"domain:example.com"},
	}, {
		name: "完全不相交",
		main: []val{{FieldDomain, "b.com"}},
		drop: []val{{FieldDomainSuffix, "a.com"}},
		want: []string{"domain:b.com"},
	}, {
		// domain 只匹配一个名字，没被覆盖就是不相交 —— 不该报表达不了。
		name: "exclude 比 domain 更窄",
		main: []val{{FieldDomain, "a.com"}},
		drop: []val{{FieldDomain, "x.a.com"}},
		want: []string{"domain:a.com"},
	}, {
		name:    "domain_regex 只按字面",
		main:    []val{{FieldDomainRegex, `^a\.com$`}, {FieldDomainRegex, `^b\.com$`}},
		drop:    []val{{FieldDomainRegex, `^a\.com$`}},
		removed: 1,
		want:    []string{`domain_regex:^b\.com$`},
	}, {
		name:    "process_name 按字面且大小写敏感",
		main:    []val{{FieldProcessName, "Telegram"}, {FieldProcessName, "telegram"}},
		drop:    []val{{FieldProcessName, "telegram"}},
		removed: 1,
		want:    []string{"process_name:Telegram"},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			main := build(t, "main", c.main)
			drop := build(t, "drop", c.drop)
			removed := main.Subtract(drop)
			if removed != c.removed {
				t.Errorf("删除条数 = %d, want %d", removed, c.removed)
			}
			if got := dump(main); !reflect.DeepEqual(got, c.want) {
				t.Errorf("剩余 = %v, want %v", got, c.want)
			}
			if main.Diag.Narrowed != c.narrowed {
				t.Errorf("Narrowed = %d, want %d", main.Diag.Narrowed, c.narrowed)
			}
			if main.Diag.Unexpressible != c.unexpressible {
				t.Errorf("Unexpressible = %d, want %d", main.Diag.Unexpressible, c.unexpressible)
			}
		})
	}
}

// exclude 一侧会被收敛（那是对上两种编码的关键），但不能被改坏 ——
// 同一个 drop 集合可能要给多个字段、多轮判定用。
func TestSubtractDoesNotMutateExclude(t *testing.T) {
	drop := build(t, "drop", []val{
		{FieldDomain, "a.com"}, {FieldDomainSuffix, ".a.com"}, {FieldIPCIDR, "10.0.0.0/8"},
	})
	before := dump(drop)

	main := build(t, "main", []val{{FieldDomainSuffix, "a.com"}, {FieldIPCIDR, "10.1.0.0/16"}})
	main.Subtract(drop)

	if got := dump(drop); !reflect.DeepEqual(got, before) {
		t.Errorf("exclude 被改了: %v → %v", before, got)
	}
}

// CIDR 是唯一能做完整差集的字段：结果仍然是一组前缀，不存在表达不了的情况。
func TestSubtractCIDR(t *testing.T) {
	t.Run("被完全包含就整条删掉", func(t *testing.T) {
		main := build(t, "main", []val{{FieldIPCIDR, "10.1.0.0/16"}})
		drop := build(t, "drop", []val{{FieldIPCIDR, "10.0.0.0/8"}})
		if removed := main.Subtract(drop); removed != 1 {
			t.Errorf("删除条数 = %d, want 1", removed)
		}
		if got := main.Values(FieldIPCIDR); len(got) != 0 {
			t.Errorf("应当一条不剩，剩 %v", got)
		}
	})

	t.Run("部分相交就裁开", func(t *testing.T) {
		main := build(t, "main", []val{{FieldIPCIDR, "10.0.0.0/8"}})
		drop := build(t, "drop", []val{{FieldIPCIDR, "10.1.0.0/16"}})
		main.Subtract(drop)
		if main.Diag.Narrowed != 1 {
			t.Errorf("Narrowed = %d, want 1", main.Diag.Narrowed)
		}
		if main.Diag.Unexpressible != 0 {
			t.Error("CIDR 不该出现表达不了的情况")
		}
		got := ipset(t, main.Values(FieldIPCIDR))
		if got.OverlapsPrefix(netip.MustParsePrefix("10.1.0.0/16")) {
			t.Error("被排除的网段还在")
		}
		for _, keep := range []string{"10.0.0.0/16", "10.2.0.0/15", "10.255.255.255/32"} {
			if !got.ContainsPrefix(netip.MustParsePrefix(keep)) {
				t.Errorf("不该被排除的 %s 丢了", keep)
			}
		}
	})

	t.Run("不相交的前缀原样不动，不顺带聚合", func(t *testing.T) {
		// aggregate 是显式开关，exclude 不该悄悄替用户打开它。
		main := build(t, "main", []val{
			{FieldIPCIDR, "1.2.0.0/24"}, {FieldIPCIDR, "1.2.1.0/24"}, {FieldIPCIDR, "10.0.0.0/8"},
		})
		drop := build(t, "drop", []val{{FieldIPCIDR, "10.0.0.0/8"}})
		main.Subtract(drop)
		want := []string{"1.2.0.0/24", "1.2.1.0/24"}
		if got := main.Values(FieldIPCIDR); !reflect.DeepEqual(got, want) {
			t.Errorf("剩余 = %v, want %v（相邻网段被顺带合并了？）", got, want)
		}
	})

	t.Run("v6 与 v4 互不影响", func(t *testing.T) {
		main := build(t, "main", []val{{FieldIPCIDR, "2001:db8::/32"}, {FieldIPCIDR, "10.0.0.0/8"}})
		drop := build(t, "drop", []val{{FieldIPCIDR, "2001:db8::/32"}})
		if removed := main.Subtract(drop); removed != 1 {
			t.Errorf("删除条数 = %d, want 1", removed)
		}
		if got := main.Values(FieldIPCIDR); !reflect.DeepEqual(got, []string{"10.0.0.0/8"}) {
			t.Errorf("剩余 = %v", got)
		}
	})

	t.Run("source_ip_cidr 同样处理", func(t *testing.T) {
		main := build(t, "main", []val{{FieldSourceIPCIDR, "10.1.0.0/16"}})
		drop := build(t, "drop", []val{{FieldSourceIPCIDR, "10.0.0.0/8"}})
		if removed := main.Subtract(drop); removed != 1 {
			t.Errorf("删除条数 = %d, want 1", removed)
		}
	})
}

func ipset(t *testing.T, values []string) *netipx.IPSet {
	t.Helper()
	var b netipx.IPSetBuilder
	for _, v := range values {
		b.AddPrefix(netip.MustParsePrefix(v))
	}
	set, err := b.IPSet()
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// 性质测试：差集跑完之后，**剩下的规则里不该有任何一条被 exclude 完全覆盖**。
//
// 判定不用被测代码自己的谓词（那是循环论证），而是拿 sing-box 的 matcher
// 采样：一条后缀被覆盖，当且仅当它匹配的名字 exclude 全都匹配 —— 对后缀集合
// 来说，取一个任意子域和（无点时）apex 两个样本就足以判定。
func TestSubtractLeavesNothingCovered(t *testing.T) {
	rng := rand.New(rand.NewSource(20260924))
	labels := []string{"a", "b", "c", "cdn", "ads", "api", "x"}
	tlds := []string{"com", "net", "org", "io"}

	randomHost := func() string {
		n := 2 + rng.Intn(3)
		parts := make([]string, 0, n)
		for i := 0; i < n-1; i++ {
			parts = append(parts, labels[rng.Intn(len(labels))])
		}
		return strings.Join(parts, ".") + "." + tlds[rng.Intn(len(tlds))]
	}

	for round := 0; round < 200; round++ {
		main := New("main")
		drop := New("drop")
		for i := 0; i < 12; i++ {
			host := randomHost()
			switch rng.Intn(3) {
			case 0:
				main.AddLenient(FieldDomain, host)
			case 1:
				main.AddLenient(FieldDomainSuffix, host)
			default:
				main.AddLenient(FieldDomainSuffix, "."+host)
			}
		}
		for i := 0; i < 5; i++ {
			host := randomHost()
			switch rng.Intn(3) {
			case 0:
				drop.AddLenient(FieldDomain, host)
			case 1:
				drop.AddLenient(FieldDomainSuffix, host)
			default:
				drop.AddLenient(FieldDomainSuffix, "."+host)
			}
		}

		// exclude 一侧的 matcher 用**原始**形态建，跟被测代码收敛出来的那份无关。
		exact := map[string]bool{}
		for _, d := range drop.Values(FieldDomain) {
			exact[d] = true
		}
		list := drop.Values(FieldDomainSuffix)
		var matcher *domain.Matcher
		if len(list) > 0 {
			matcher = domain.NewMatcher(nil, list, false)
		}
		hitByDrop := func(name string) bool {
			return exact[name] || (matcher != nil && matcher.Match(name))
		}

		main.Subtract(drop)

		for _, v := range main.Values(FieldDomain) {
			if hitByDrop(v) {
				t.Fatalf("第 %d 轮：domain %q 被 exclude 命中却没删掉", round, v)
			}
		}
		for _, v := range main.Values(FieldDomainSuffix) {
			base, includesSelf := suffixBase(v)
			// 任取一个不可能与数据撞上的子域做样本。
			if !hitByDrop("zzq." + base) {
				continue
			}
			if includesSelf && !hitByDrop(base) {
				continue
			}
			t.Fatalf("第 %d 轮：suffix %q 被 exclude 完全覆盖却没删掉", round, v)
		}
	}
}

// 回归：真实世界最常见的那种编码错配 —— 主集合是 Clash / sing-box JSON 的
// 无点写法，exclude 是 geosite 的 RootDomain 双条写法。
func TestSubtractRealWorldEncodingMismatch(t *testing.T) {
	main := build(t, "main", []val{
		{FieldDomainSuffix, "youtube.com"},           // 来自 +.youtube.com
		{FieldDomain, "googleads.g.doubleclick.net"}, // 来自 sing-box JSON 的 domain
		{FieldDomainSuffix, "keep.example"},
	})
	drop := build(t, "drop", []val{
		{FieldDomain, "youtube.com"}, {FieldDomainSuffix, ".youtube.com"},
		{FieldDomain, "doubleclick.net"}, {FieldDomainSuffix, ".doubleclick.net"},
	})
	if removed := main.Subtract(drop); removed != 2 {
		t.Errorf("删除条数 = %d, want 2", removed)
	}
	if got := dump(main); !reflect.DeepEqual(got, []string{"domain_suffix:keep.example"}) {
		t.Errorf("剩余 = %v", got)
	}
}

// 大 exclude 集合下的代价：祖先查表是 O(n × label 数)，不是两层循环。
func BenchmarkSubtractLargeExclude(b *testing.B) {
	makeSets := func() (*RuleSet, *RuleSet) {
		main := New("main")
		for i := range 50_000 {
			main.AddLenient(FieldDomain, fmt.Sprintf("h%d.p%d.example", i, i/20))
		}
		drop := New("drop")
		for i := range 20_000 {
			drop.AddLenient(FieldDomainSuffix, fmt.Sprintf("p%d.example", i))
			drop.AddLenient(FieldDomain, fmt.Sprintf("p%d.example", i))
		}
		return main, drop
	}
	for b.Loop() {
		b.StopTimer()
		main, drop := makeSets()
		b.StartTimer()
		main.Subtract(drop)
	}
}

// 改窄产生的新值必须再过一遍 exclude：带点形式含前导点，可能命中一个无点形式
// 没命中的关键词。
func TestSubtractRecheckAfterNarrowing(t *testing.T) {
	main := build(t, "main", []val{{FieldDomainSuffix, "a.com"}})
	drop := build(t, "drop", []val{{FieldDomain, "a.com"}, {FieldDomainKeyword, ".a.com"}})
	if removed := main.Subtract(drop); removed != 1 {
		t.Errorf("删除条数 = %d, want 1", removed)
	}
	if got := dump(main); len(got) != 0 {
		t.Errorf("改窄后的值仍被 exclude 覆盖，应当删掉，剩 %v", got)
	}
	if main.Diag.Narrowed != 0 {
		t.Errorf("Narrowed = %d, want 0", main.Diag.Narrowed)
	}
}

// apex 与子域同时被排掉 = 整条被覆盖。
func TestSubtractBothHalvesDropped(t *testing.T) {
	main := build(t, "main", []val{{FieldDomainSuffix, "a.com"}})
	drop := New("drop")
	// 绕过 exclude 一侧的收敛，直接构造两半分开的形态
	drop.AddLenient(FieldDomain, "a.com")
	drop.AddLenient(FieldDomainSuffix, ".a.com")
	if removed := main.Subtract(drop); removed != 1 {
		t.Errorf("删除条数 = %d, want 1", removed)
	}
	if got := dump(main); len(got) != 0 {
		t.Errorf("应当一条不剩，剩 %v", got)
	}
}
