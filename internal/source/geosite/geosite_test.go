package geosite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source"
)

// ---------------- protobuf 解码 ----------------
//
// 手写解码器是这一包里最有风险的部分。先用自造的最小样本钉住 wire format，
// 再在 TestAgainstRealDLC 里拿真实 dlc.dat 与源码树交叉核对。

func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func tag(field, wire int) []byte { return varint(uint64(field)<<3 | uint64(wire)) }

func lenDelim(field int, payload []byte) []byte {
	out := tag(field, wireBytes)
	out = append(out, varint(uint64(len(payload)))...)
	return append(out, payload...)
}

func domainMsg(t domainType, value string, attrs ...string) []byte {
	var body []byte
	// proto3 不序列化默认值，Plain(0) 的 type 字段本来就不会出现 —— 这里照做，
	// 顺便验证解码器对"字段缺席"的处理。
	if t != typePlain {
		body = append(body, tag(1, wireVarint)...)
		body = append(body, varint(uint64(t))...)
	}
	body = append(body, lenDelim(2, []byte(value))...)
	for _, a := range attrs {
		attr := lenDelim(1, []byte(a))
		attr = append(attr, tag(2, wireVarint)...) // bool_value = true
		attr = append(attr, varint(1)...)
		body = append(body, lenDelim(3, attr)...)
	}
	return body
}

func siteMsg(code string, domains ...[]byte) []byte {
	body := lenDelim(1, []byte(code))
	for _, d := range domains {
		body = append(body, lenDelim(2, d)...)
	}
	return body
}

func listMsg(sites ...[]byte) []byte {
	var out []byte
	for _, s := range sites {
		out = append(out, lenDelim(1, s)...)
	}
	return out
}

func TestDecodeMinimal(t *testing.T) {
	data := listMsg(
		siteMsg("EXAMPLE",
			domainMsg(typeFull, "full.example"),
			domainMsg(typeRootDomain, "root.example"),
			domainMsg(typeRegex, `^re\.example$`),
			domainMsg(typePlain, "keyword"),
			domainMsg(typeRootDomain, "tagged.example", "ads", "cn"),
		),
		siteMsg("OTHER", domainMsg(typeFull, "other.example")),
	)
	sites, err := decodeDLC(data)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if len(sites) != 2 {
		t.Fatalf("site 数 = %d, want 2", len(sites))
	}
	if sites[0].Code != "EXAMPLE" || len(sites[0].Domains) != 5 {
		t.Fatalf("site[0] = %+v", sites[0])
	}
	got := sites[0].Domains
	if got[0].Type != typeFull || got[0].Value != "full.example" {
		t.Errorf("Full 解错: %+v", got[0])
	}
	// type 字段缺席时必须落回 Plain(0)
	if got[3].Type != typePlain || got[3].Value != "keyword" {
		t.Errorf("Plain（字段缺席）解错: %+v", got[3])
	}
	if !reflect.DeepEqual(got[4].Attributes, []string{"ads", "cn"}) {
		t.Errorf("属性解错: %+v", got[4])
	}
}

// 上游给 message 加字段时不该让我们解不动。
func TestDecodeSkipsUnknownFields(t *testing.T) {
	var body []byte
	body = append(body, lenDelim(1, []byte("EXAMPLE"))...)
	body = append(body, tag(9, wireVarint)...) // 未知 varint 字段
	body = append(body, varint(12345)...)
	body = append(body, lenDelim(8, []byte("未知的 bytes 字段"))...)
	body = append(body, tag(7, wireFixed32)...)
	body = append(body, 1, 2, 3, 4)
	body = append(body, tag(6, wireFixed64)...)
	body = append(body, 1, 2, 3, 4, 5, 6, 7, 8)
	body = append(body, lenDelim(2, domainMsg(typeFull, "kept.example"))...)

	sites, err := decodeDLC(listMsg(body))
	if err != nil {
		t.Fatalf("未知字段应当被跳过，实际报错: %v", err)
	}
	if len(sites) != 1 || len(sites[0].Domains) != 1 || sites[0].Domains[0].Value != "kept.example" {
		t.Errorf("跳过未知字段之后内容不对: %+v", sites)
	}
}

// 输入是从网上下载的，损坏的长度前缀绝不能让进程 panic。
func TestDecodeRejectsTruncated(t *testing.T) {
	good := listMsg(siteMsg("A", domainMsg(typeFull, "a.example")))
	for cut := 1; cut < len(good); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("截断到 %d 字节时 panic 了: %v", cut, r)
				}
			}()
			_, _ = decodeDLC(good[:cut])
		}()
	}
	// 伪造一个超长的长度前缀
	bogus := append(tag(1, wireBytes), varint(1<<40)...)
	if _, err := decodeDLC(bogus); err == nil {
		t.Error("伪造的长度前缀应当报错")
	}
}

// ---------------- 字段映射 ----------------

func TestMapDomain(t *testing.T) {
	cases := []struct {
		in   rawDomain
		want []item
	}{
		{rawDomain{Type: typeFull, Value: "a.example"},
			[]item{{ruleset.FieldDomain, "a.example"}}},
		{rawDomain{Type: typeRegex, Value: `^x$`},
			[]item{{ruleset.FieldDomainRegex, `^x$`}}},
		{rawDomain{Type: typePlain, Value: "kw"},
			[]item{{ruleset.FieldDomainKeyword, "kw"}}},
		// RootDomain 同时产出两条，前导点必须保留
		{rawDomain{Type: typeRootDomain, Value: "a.example"},
			[]item{{ruleset.FieldDomain, "a.example"}, {ruleset.FieldDomainSuffix, ".a.example"}}},
		// 值不含点时只产出后缀（顶级域）
		{rawDomain{Type: typeRootDomain, Value: "cn"},
			[]item{{ruleset.FieldDomainSuffix, ".cn"}}},
	}
	for _, c := range cases {
		if got := mapDomain(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("mapDomain(%+v) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

// ---------------- @attr 展开 ----------------

func testCodes(t *testing.T) codeMap {
	t.Helper()
	data := listMsg(
		siteMsg("GOOGLE",
			domainMsg(typeRootDomain, "google.com"),
			domainMsg(typeRootDomain, "doubleclick.net", "ads"),
			domainMsg(typeRootDomain, "google.cn", "cn"),
		),
		siteMsg("GEOLOCATION-!CN",
			domainMsg(typeRootDomain, "google.com"),
			domainMsg(typeRootDomain, "inside.example", "cn"),
		),
	)
	sites, err := decodeDLC(data)
	if err != nil {
		t.Fatal(err)
	}
	codes, _ := build(sites)
	return codes
}

func TestAttributeExpansion(t *testing.T) {
	codes := testCodes(t)
	for _, want := range []string{"google", "google@ads", "google@cn", "geolocation-!cn", "geolocation-!cn@cn"} {
		if _, ok := codes[want]; !ok {
			t.Errorf("缺 code %q；实际有 %v", want, keysOf(codes))
		}
	}
	// code 名必须小写
	if _, ok := codes["GOOGLE"]; ok {
		t.Error("code 名没有小写")
	}
}

// 带属性的域名**同时留在父 code 里** —— 属性是标记，不是移出。
// "要 google 但不要广告"才需要做差；这条弄反的话 exclude 会变成空操作。
func TestAttributedDomainsStayInParent(t *testing.T) {
	codes := testCodes(t)
	parent := valuesOf(codes["google"])
	if !contains(parent, "doubleclick.net") {
		t.Errorf("带 @ads 的域名从父 code 里消失了: %v", parent)
	}
	ads := valuesOf(codes["google@ads"])
	if !contains(ads, "doubleclick.net") || contains(ads, "google.com") {
		t.Errorf("google@ads 内容不对: %v", ads)
	}
}

// 与自己名字矛盾的属性 code 要报出来，但**不自动排除** ——
// 上游 filterTags 会原地覆盖父 code，那是隐式行为。
func TestContradictionsAreReportedNotApplied(t *testing.T) {
	data := listMsg(siteMsg("GEOLOCATION-!CN",
		domainMsg(typeRootDomain, "keep.example"),
		domainMsg(typeRootDomain, "inside.example", "cn"),
	))
	sites, _ := decodeDLC(data)
	codes, odd := build(sites)
	if !reflect.DeepEqual(odd, []string{"geolocation-!cn@cn"}) {
		t.Errorf("矛盾 code 清单 = %v", odd)
	}
	// 父 code 里那条矛盾的域名必须还在 —— 没有被自动减掉
	if !contains(valuesOf(codes["geolocation-!cn"]), "inside.example") {
		t.Error("矛盾的域名被自动减掉了，那是上游 filterTags 的行为，我们不做")
	}
}

// ---------------- glob ----------------

func TestResolveGlobs(t *testing.T) {
	codes := testCodes(t)
	cases := []struct {
		pattern string
		want    []string
	}{
		{"google", []string{"google"}},
		{"GOOGLE", []string{"google"}}, // 查表前小写
		{"google@*", []string{"google@ads", "google@cn"}},
		{"*@cn", []string{"geolocation-!cn@cn", "google@cn"}},
		{"geolocation-!cn", []string{"geolocation-!cn"}}, // ! 是字面字符
	}
	for _, c := range cases {
		got, err := codes.resolve(c.pattern)
		if err != nil {
			t.Errorf("resolve(%q): %v", c.pattern, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("resolve(%q) = %v, want %v", c.pattern, got, c.want)
		}
	}
}

// 一个 code 都没匹配上要报错：上游改名时静默产出零条规则是最难发现的失败形态。
func TestResolveEmptyMatchIsError(t *testing.T) {
	codes := testCodes(t)
	for _, pattern := range []string{"nope", "category-*", ""} {
		if got, err := codes.resolve(pattern); err == nil {
			t.Errorf("resolve(%q) = %v，应当报错", pattern, got)
		}
	}
}

// ---------------- 下载与校验 ----------------

func TestFetchVerifiesChecksum(t *testing.T) {
	payload := listMsg(siteMsg("A", domainMsg(typeFull, "a.example")))
	sum := sha256.Sum256(payload)

	newServer := func(checksum string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, ".sha256sum") {
				fmt.Fprintf(w, "%s  dlc.dat\n", checksum)
				return
			}
			w.Write(payload)
		}))
	}

	t.Run("匹配", func(t *testing.T) {
		srv := newServer(hex.EncodeToString(sum[:]))
		defer srv.Close()
		got, err := fetchAt(context.Background(), srv.Client(), srv.URL+"/")
		if err != nil {
			t.Fatalf("校验和正确时不该失败: %v", err)
		}
		if len(got) != len(payload) {
			t.Errorf("内容长度 = %d, want %d", len(got), len(payload))
		}
	})

	t.Run("不匹配", func(t *testing.T) {
		srv := newServer(strings.Repeat("0", 64))
		defer srv.Close()
		_, err := fetchAt(context.Background(), srv.Client(), srv.URL+"/")
		if err == nil {
			t.Fatal("校验和不匹配时必须拒绝")
		}
		if !strings.Contains(err.Error(), "校验和不匹配") {
			t.Errorf("错误信息不对: %v", err)
		}
	})
}

func TestVerifySHA256ParsesChecksumFile(t *testing.T) {
	data := []byte("hello")
	sum := sha256.Sum256(data)
	hexSum := hex.EncodeToString(sum[:])
	for _, form := range []string{
		hexSum,
		hexSum + "  dlc.dat",
		hexSum + " *dlc.dat",
		hexSum + "  dlc.dat\n",
	} {
		if err := verifySHA256(data, []byte(form)); err != nil {
			t.Errorf("校验和文件格式 %q 应当被接受: %v", form, err)
		}
	}
	if err := verifySHA256(data, []byte("短了")); err == nil {
		t.Error("格式不对的校验和文件应当报错")
	}
}

// ---------------- Source ----------------

func localSource(t *testing.T) *Source {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "dlc.dat")
	data := listMsg(
		siteMsg("GOOGLE",
			domainMsg(typeRootDomain, "google.com"),
			domainMsg(typeRootDomain, "doubleclick.net", "ads"),
		),
		siteMsg("NETFLIX", domainMsg(typeRootDomain, "netflix.com")),
	)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return New(Options{File: path})
}

func TestSourceFeed(t *testing.T) {
	src := localSource(t)
	if err := src.Prepare(context.Background(), []string{"google"}); err != nil {
		t.Fatal(err)
	}
	set := ruleset.New("t")
	if err := src.Feed("google", source.Options{}, set); err != nil {
		t.Fatal(err)
	}
	// RootDomain 产出 domain + 带点 suffix 两条
	if got := set.Values(ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"doubleclick.net", "google.com"}) {
		t.Errorf("domain = %v", got)
	}
	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{".doubleclick.net", ".google.com"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	// 收敛会把这两条压成无点后缀 —— 和 sing-box 自己 Dump 的形态一致
	set.Collapse()
	if got := set.Values(ruleset.FieldDomain); got != nil {
		t.Errorf("收敛后 domain 应当清空: %v", got)
	}
	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"doubleclick.net", "google.com"}) {
		t.Errorf("收敛后 domain_suffix = %v", got)
	}
}

func TestSourceExcludeViaAttribute(t *testing.T) {
	src := localSource(t)
	if err := src.Prepare(context.Background(), []string{"google", "google@ads"}); err != nil {
		t.Fatal(err)
	}
	keep := ruleset.New("keep")
	if err := src.Feed("google", source.Options{}, keep); err != nil {
		t.Fatal(err)
	}
	drop := ruleset.New("drop")
	if err := src.Feed("google@ads", source.Options{}, drop); err != nil {
		t.Fatal(err)
	}
	keep.Subtract(drop)
	keep.Collapse()
	if got := keep.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"google.com"}) {
		t.Errorf("要 google 不要广告，结果 = %v", got)
	}
}

func TestSourceBulkExpansion(t *testing.T) {
	src := localSource(t)
	if err := src.Prepare(context.Background(), []string{"*"}); err != nil {
		t.Fatal(err)
	}
	all, err := src.ExpandBulk([]string{"*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(all, []string{"google", "google@ads", "netflix"}) {
		t.Errorf("全量展开 = %v", all)
	}
	// 批量时基本必写：滤掉属性变体
	noAttr, err := src.ExpandBulk([]string{"*"}, []string{"*@*"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(noAttr, []string{"google", "netflix"}) {
		t.Errorf("排除属性变体后 = %v", noAttr)
	}
	if _, err := src.ExpandBulk(nil, nil); err == nil {
		t.Error("include 为空应当报错")
	}
	if _, err := src.ExpandBulk([]string{"*"}, []string{"*"}); err == nil {
		t.Error("include 与 exclude 互相抵消应当报错")
	}
}

func TestSourceLoadFailureIsClassWide(t *testing.T) {
	src := New(Options{File: filepath.Join(t.TempDir(), "missing.dat")})
	err := src.Prepare(context.Background(), []string{"cn"})
	if err == nil {
		t.Fatal("dlc.dat 拿不到应当是整类输入不可用")
	}
	// 之后每次 Feed 都要带着同一个原因，而不是变成"code 不存在"
	if ferr := src.Feed("cn", source.Options{}, ruleset.New("t")); ferr == nil {
		t.Error("加载失败后 Feed 应当继续报错")
	}
}

// ---------------- 真实 dlc.dat ----------------

// 拿真实 dlc.dat 与本地 domain-list-community 源码树交叉核对。
// 手写解码器的正确性最终靠这个。
func TestAgainstRealDLC(t *testing.T) {
	path := os.Getenv("SRSBOX_DLC")
	if path == "" {
		t.Skip("设 SRSBOX_DLC=<dlc.dat 路径> 运行")
	}
	src := New(Options{File: path})
	if err := src.Prepare(context.Background(), []string{"cn"}); err != nil {
		t.Fatal(err)
	}
	codes := src.Codes()
	t.Logf("可寻址 code: %d", len(codes))
	t.Logf("自相矛盾的属性 code: %v", src.Contradictions())

	// 源码树里每个文件是一个 code，数量必须相等
	if dir := os.Getenv("SRSBOX_DLC_DATA"); dir != "" {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		base := 0
		for _, c := range codes {
			if !strings.Contains(c, "@") {
				base++
			}
		}
		if base != len(entries) {
			t.Errorf("基础 code 数 = %d，源码树文件数 = %d", base, len(entries))
		}
	}

	// 全库喂一遍：归一化不该拒绝任何值
	set := ruleset.New("all")
	for _, code := range codes {
		if err := src.Feed(code, source.Options{}, set); err != nil {
			t.Fatalf("Feed(%s): %v", code, err)
		}
	}
	t.Logf("全库并集 %d 条，归一化拒绝 %d 条", set.Total(), set.Diag.InvalidTotal())
	if set.Diag.InvalidTotal() > 0 {
		t.Errorf("归一化拒绝了 %d 条 v2fly 的值: %v", set.Diag.InvalidTotal(), set.Diag.InvalidSamples)
	}
}

// ---------------- helpers ----------------

func keysOf(c codeMap) []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

func valuesOf(items []item) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.value)
	}
	return out
}

func contains(list []string, want string) bool {
	return slices.Contains(list, want)
}

// ---------------- normalize ----------------

// 造一份含畸形条目的 dlc.dat，验证两种模式的差别。
//
// 实测 v2fly 全库（1898 个 code、73321 条值）归一化拒绝 0 条，所以这个开关
// 今天没有可观察的效果 —— 它是给上游哪天腐坏用的绊线，必须有测试证明它真的会响。
func dlcWithBadValue(t *testing.T) string {
	t.Helper()
	data := listMsg(siteMsg("MIXED",
		domainMsg(typeFull, "good.example"),
		domainMsg(typeFull, "-bad-.example"), // label 以连字符开头结尾，归一会拒
		domainMsg(typeFull, "also-good.example"),
	))
	path := filepath.Join(t.TempDir(), "dlc.dat")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNormalizeLenientDropsAndCounts(t *testing.T) {
	src := New(Options{File: dlcWithBadValue(t), Normalize: NormalizeLenient})
	if err := src.Prepare(context.Background(), []string{"mixed"}); err != nil {
		t.Fatal(err)
	}
	set := ruleset.New("t")
	if err := src.Feed("mixed", source.Options{}, set); err != nil {
		t.Fatalf("lenient 不该让整条失败: %v", err)
	}
	if got := set.Values(ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"also-good.example", "good.example"}) {
		t.Errorf("好的值应当照常进来: %v", got)
	}
	// 丢弃必须记账 —— 静默丢规则是这个项目一直在消灭的东西
	if set.Diag.InvalidTotal() != 1 {
		t.Errorf("非法值计数 = %d, want 1", set.Diag.InvalidTotal())
	}
	if len(set.Diag.InvalidSamples) != 1 {
		t.Errorf("样例数 = %d, want 1", len(set.Diag.InvalidSamples))
	}
}

func TestNormalizeStrictFailsTheInput(t *testing.T) {
	src := New(Options{File: dlcWithBadValue(t), Normalize: NormalizeStrict})
	if err := src.Prepare(context.Background(), []string{"mixed"}); err != nil {
		t.Fatal(err)
	}
	err := src.Feed("mixed", source.Options{}, ruleset.New("t"))
	if err == nil {
		t.Fatal("strict 下遇到非法值应当报错")
	}
	if !strings.Contains(err.Error(), "-bad-.example") {
		t.Errorf("错误信息应当指出是哪个值: %v", err)
	}
}

// 省略 normalize 等于 lenient。
func TestNormalizeDefaultsToLenient(t *testing.T) {
	src := New(Options{File: dlcWithBadValue(t)})
	if err := src.Prepare(context.Background(), []string{"mixed"}); err != nil {
		t.Fatal(err)
	}
	if err := src.Feed("mixed", source.Options{}, ruleset.New("t")); err != nil {
		t.Errorf("默认应当是 lenient: %v", err)
	}
}

// 解码失败时错误信息必须指出**出错位置**。dlc.dat 是从网上下载的二进制，
// 一句"解析失败"对排错毫无帮助 —— 嵌套包装出来的路径才是有用的东西。
func TestDecodeErrorNamesTheLocation(t *testing.T) {
	// 造一条 Domain.value 的长度前缀被截断的记录
	body := lenDelim(1, []byte("EXAMPLE"))
	bad := tag(2, wireBytes)
	bad = append(bad, varint(99)...) // 声称 99 字节，实际没有
	bad = append(bad, []byte("短")...)
	body = append(body, lenDelim(2, bad)...)

	_, err := decodeDLC(listMsg(body))
	if err == nil {
		t.Fatal("截断的记录应当报错")
	}
	msg := err.Error()
	// 逐层包装应当拼出一条路径
	for _, want := range []string{"GeoSiteList.entry", "GeoSite.domain", "Domain.value"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误信息里缺路径片段 %q:\n  %s", want, msg)
		}
	}
	t.Logf("错误信息: %s", msg)
}
