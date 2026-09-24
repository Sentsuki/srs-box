package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/config"
	"github.com/Sentsuki/srs-box/internal/report"
)

// setup 起一个本地源服务器并切到临时工作目录（files 与 output 都是相对路径）。
func setup(t *testing.T) (base string, dir string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/good":
			fmt.Fprint(w, "DOMAIN-SUFFIX,good.example\nDOMAIN,a.good.example\n")
		case "/other":
			fmt.Fprint(w, "DOMAIN-SUFFIX,other.example\n")
		case "/cidr":
			fmt.Fprint(w, "1.2.0.0/24\n1.2.1.0/24\n")
		case "/allow":
			fmt.Fprint(w, "DOMAIN-SUFFIX,other.example\n")
		case "/html":
			fmt.Fprint(w, "<html><body>404</body></html>\n")
		case "/watermark":
			fmt.Fprint(w, "DOMAIN,keep.example\nDOMAIN,mark.ruleset.skk.moe\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	dir = t.TempDir()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return srv.URL, dir
}

func load(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatalf("配置有误: %v", err)
	}
	return cfg
}

func runAll(t *testing.T, cfg *config.Config, opts Options) *report.Run {
	t.Helper()
	run, err := Run(context.Background(), cfg, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return run
}

func byName(run *report.Run) map[string]*report.Result {
	out := map[string]*report.Result{}
	for _, res := range run.Results {
		out[res.Name] = res
	}
	return out
}

// 整个项目唯一的硬规则：每个规则集是独立单元。一个失败不能牵连其他。
func TestFailureIsIsolated(t *testing.T) {
	base, dir := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" }, "json": { "dir": "out/json" } },
  "rulesets": {
    "good": ["%s/good"],
    "dead": ["%s/missing"],
    "also-good": ["%s/other"]
  }
}`, base, base, base))

	run := runAll(t, cfg, Options{})
	got := byName(run)
	if !got["good"].OK || !got["also-good"].OK {
		t.Errorf("好规则集受牵连了: good=%v also-good=%v", got["good"].Err, got["also-good"].Err)
	}
	if got["dead"].OK {
		t.Error("坏规则集应当失败")
	}
	if len(got["dead"].FailedSources) != 1 {
		t.Errorf("失败原因没记下来: %+v", got["dead"])
	}
	// 好规则集的产物照常落盘
	for _, name := range []string{"good", "also-good"} {
		for _, ext := range []string{"srs", "json"} {
			path := filepath.Join(dir, "out", ext, name+"."+ext)
			if _, err := os.Stat(path); err != nil {
				t.Errorf("缺产物 %s: %v", path, err)
			}
		}
	}
}

// 一个规则集的多个输入里只挂了一部分 —— 照常构建，但必须记在摘要里。
func TestPartialSourceFailureStillBuilds(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "mixed": ["%s/good", "%s/missing"] }
}`, base, base))

	res := byName(runAll(t, cfg, Options{}))["mixed"]
	if !res.OK {
		t.Fatalf("部分输入可用就该构建: %v", res.Err)
	}
	if len(res.FailedSources) != 1 {
		t.Errorf("挂掉的源必须出现在摘要里: %+v", res.FailedSources)
	}
	if res.Rules == 0 {
		t.Error("可用输入的内容没进来")
	}
}

// exclude 的输入全挂时必须拒绝产出 —— 否则本该排除的东西会留在产物里，
// 而产物看起来完全正常。
func TestExcludeFailureRefusesToProduce(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": {
    "guarded": { "sources": ["%s/good"], "exclude": { "sources": ["%s/missing"] } }
  }
}`, base, base))

	res := byName(runAll(t, cfg, Options{}))["guarded"]
	if res.OK {
		t.Fatal("exclude 全挂时不该产出")
	}
	if !strings.Contains(res.Err.Error(), "拒绝产出") {
		t.Errorf("错误信息应当说清原因: %v", res.Err)
	}
}

func TestExcludeSubtracts(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": {
    "net": {
      "sources": ["%s/good", "%s/other"],
      "exclude": { "sources": ["%s/allow"] }
    }
  }
}`, base, base, base))

	res := byName(runAll(t, cfg, Options{}))["net"]
	if !res.OK {
		t.Fatalf("应当成功: %v", res.Err)
	}
	if res.Diag.Subtracted == 0 {
		t.Error("差集没生效")
	}
}

// 变换顺序：收敛必须在差集之后。这里 domain a.good.example 会被
// domain_suffix good.example 覆盖掉，收敛计数应当非零。
func TestCollapseRunsAndIsCounted(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "c": ["%s/good"] }
}`, base))
	res := byName(runAll(t, cfg, Options{}))["c"]
	if res.Diag.Collapsed != 1 {
		t.Errorf("收敛计数 = %d, want 1", res.Diag.Collapsed)
	}

	// 显式关掉就不该收敛
	cfg2 := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "c": { "sources": ["%s/good"], "collapse": false } }
}`, base))
	if got := byName(runAll(t, cfg2, Options{}))["c"].Diag.Collapsed; got != 0 {
		t.Errorf("collapse:false 时收敛计数 = %d, want 0", got)
	}
}

func TestAggregateOnlyWhenAsked(t *testing.T) {
	base, _ := setup(t)
	on := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "ip": { "sources": ["%s/cidr"], "format": "cidr", "aggregate": true } }
}`, base))
	if got := byName(runAll(t, on, Options{}))["ip"].Diag.Aggregated; got != 1 {
		t.Errorf("聚合计数 = %d, want 1", got)
	}

	off := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "ip": { "sources": ["%s/cidr"], "format": "cidr" } }
}`, base))
	if got := byName(runAll(t, off, Options{}))["ip"].Diag.Aggregated; got != 0 {
		t.Errorf("没写 aggregate 时聚合计数 = %d, want 0", got)
	}
}

// 水印过滤的丢弃条数要出现在诊断里 —— 旧实现把这条藏在内部，静默丢规则。
func TestWatermarkFilterIsCounted(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "w": ["%s/watermark"] }
}`, base))
	res := byName(runAll(t, cfg, Options{}))["w"]
	if res.Diag.Dropped != 1 {
		t.Errorf("丢弃计数 = %d, want 1", res.Diag.Dropped)
	}
	if res.Rules != 1 {
		t.Errorf("剩余规则 = %d, want 1", res.Rules)
	}
}

// 断言格式要一路传到解析层：HTTP 200 的 HTML 错误页必须被挡下。
func TestFormatAssertionReachesParser(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "ip": { "sources": ["%s/html"], "format": "cidr" } }
}`, base))
	res := byName(runAll(t, cfg, Options{}))["ip"]
	if res.OK {
		t.Error("cidr 断言应当挡下 HTML 错误页")
	}
}

// --only 之外的规则集状态必须是 skipped 而不是 failed，否则发布方会把
// 没跑的那些全删光。
func TestOnlySelection(t *testing.T) {
	base, dir := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "a": ["%s/good"], "b": ["%s/other"] }
}`, base, base))

	run := runAll(t, cfg, Options{Only: []string{"a"}})
	if len(run.Results) != 1 {
		t.Fatalf("只该跑一个，实际 %d 个", len(run.Results))
	}
	if run.Status("b") != report.StatusSkipped {
		t.Errorf("b 应当是 skipped，实际 %q", run.Status("b"))
	}
	if _, err := os.Stat(filepath.Join(dir, "out", "srs", "b.srs")); err == nil {
		t.Error("没选中的规则集不该产出文件")
	}
}

func TestOnlyRejectsUnknownName(t *testing.T) {
	base, _ := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "a": ["%s/good"] }
}`, base))
	_, err := Run(context.Background(), cfg, Options{Only: []string{"nope"}})
	if err == nil {
		t.Fatal("--only 指向不存在的规则集应当报错")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("错误信息应当点名: %v", err)
	}
}

func TestDryRunWritesNothing(t *testing.T) {
	base, dir := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" }, "json": { "dir": "out/json" } },
  "rulesets": { "a": ["%s/good"] }
}`, base))

	run := runAll(t, cfg, Options{DryRun: true})
	if !byName(run)["a"].OK {
		t.Error("dry-run 也该判定成功")
	}
	if _, err := os.Stat(filepath.Join(dir, "out")); err == nil {
		t.Error("dry-run 不该建输出目录")
	}
}

// 上一次成功、这一次失败的规则集会留下旧文件，而摘要标着 ✗ ——
// 看目录的人无从分辨哪些是新的。本地输出目录应当只反映本次运行。
func TestPruneStaleRemovesLastRunLeftovers(t *testing.T) {
	base, dir := setup(t)
	body := fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" }, "json": { "dir": "out/json" } },
  "rulesets": { "a": ["%s/%%s"] }
}`, base)

	// 第一次：成功，留下产物
	cfg := load(t, fmt.Sprintf(body, "good"))
	run := runAll(t, cfg, Options{})
	PruneStale(cfg, run)
	srsPath := filepath.Join(dir, "out", "srs", "a.srs")
	if _, err := os.Stat(srsPath); err != nil {
		t.Fatalf("第一次应当产出: %v", err)
	}

	// 第二次：源挂了
	cfg2 := load(t, fmt.Sprintf(body, "missing"))
	run2 := runAll(t, cfg2, Options{})
	if byName(run2)["a"].OK {
		t.Fatal("第二次应当失败")
	}
	PruneStale(cfg2, run2)
	if _, err := os.Stat(srsPath); err == nil {
		t.Error("失败之后旧产物应当被清掉，否则分不清哪些是本次的")
	}
}

// --only 不该误删其余规则集的产物。
func TestPruneStaleRespectsOnly(t *testing.T) {
	base, dir := setup(t)
	full := fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "a": ["%s/good"], "b": ["%s/other"] }
}`, base, base)

	cfg := load(t, full)
	PruneStale(cfg, runAll(t, cfg, Options{}))
	bPath := filepath.Join(dir, "out", "srs", "b.srs")
	if _, err := os.Stat(bPath); err != nil {
		t.Fatalf("前置条件：b 应当已产出: %v", err)
	}

	// 只跑 a，b 的产物必须留着
	cfgOnly := load(t, full)
	PruneStale(cfgOnly, runAll(t, cfgOnly, Options{Only: []string{"a"}}))
	if _, err := os.Stat(bPath); err != nil {
		t.Error("--only 误删了没选中的规则集产物")
	}
}

// 同一个地址被多个规则集引用时只抓一次 —— 先收齐 key 再准备是这条成立的原因。
func TestSharedURLFetchedOnce(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		fmt.Fprint(w, "DOMAIN,a.example\n")
	}))
	defer srv.Close()

	dir := t.TempDir()
	old, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(old)

	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "one": ["%s/x"], "two": ["%s/x"], "three": ["%s/x"] }
}`, srv.URL, srv.URL, srv.URL))

	runAll(t, cfg, Options{})
	if hits != 1 {
		t.Errorf("抓了 %d 次，应当只抓 1 次", hits)
	}
}

func TestLocalFileInput(t *testing.T) {
	_, dir := setup(t)
	if err := os.MkdirAll("data", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("data", "list.txt"), []byte("DOMAIN,local.example\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := load(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "local": { "files": ["data/list.txt"], "inline": ["DOMAIN-SUFFIX,inline.example"] } }
}`)
	res := byName(runAll(t, cfg, Options{}))["local"]
	if !res.OK {
		t.Fatalf("本地文件 + 内联应当成功: %v / %v", res.Err, res.FailedSources)
	}
	if res.Rules != 2 {
		t.Errorf("规则数 = %d, want 2", res.Rules)
	}
	_ = dir
}

// ---------------- geosite ----------------

// makeDLC 造一份最小 dlc.dat（protobuf wire format 手搓，与 geosite 包的测试同法）。
func makeDLC(t *testing.T, dir string) string {
	t.Helper()
	varint := func(v uint64) []byte {
		var out []byte
		for v >= 0x80 {
			out = append(out, byte(v)|0x80)
			v >>= 7
		}
		return append(out, byte(v))
	}
	// protobuf wire type：0 = varint，2 = 长度前缀。
	const wireVarint, wireBytes = 0, 2
	lenDelim := func(field int, payload []byte) []byte {
		out := varint(uint64(field)<<3 | wireBytes)
		out = append(out, varint(uint64(len(payload)))...)
		return append(out, payload...)
	}
	domain := func(typ int, value string, attrs ...string) []byte {
		var body []byte
		if typ != 0 {
			body = append(body, varint(uint64(1)<<3|wireVarint)...)
			body = append(body, varint(uint64(typ))...)
		}
		body = append(body, lenDelim(2, []byte(value))...)
		for _, a := range attrs {
			attr := lenDelim(1, []byte(a))
			attr = append(attr, varint(uint64(2)<<3|wireVarint)...)
			attr = append(attr, varint(1)...)
			body = append(body, lenDelim(3, attr)...)
		}
		return body
	}
	site := func(code string, domains ...[]byte) []byte {
		body := lenDelim(1, []byte(code))
		for _, d := range domains {
			body = append(body, lenDelim(2, d)...)
		}
		return body
	}
	var data []byte
	for _, s := range [][]byte{
		site("GOOGLE", domain(2, "google.com"), domain(2, "doubleclick.net", "ads")),
		site("NETFLIX", domain(2, "netflix.com")),
	} {
		data = append(data, lenDelim(1, s)...)
	}
	path := filepath.Join(dir, "dlc.dat")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGeositeInput(t *testing.T) {
	_, dir := setup(t)
	dlc := makeDLC(t, dir)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q },
  "rulesets": { "g": { "geosite": ["google"] } }
}`, filepath.ToSlash(dlc)))

	res := byName(runAll(t, cfg, Options{}))["g"]
	if !res.OK {
		t.Fatalf("geosite 输入应当成功: %v / %v", res.Err, res.FailedSources)
	}
	// RootDomain 产出 domain + 带点后缀，收敛压回无点后缀
	if res.Rules != 2 {
		t.Errorf("规则数 = %d, want 2（google.com 与 doubleclick.net）", res.Rules)
	}
}

// 混写：URL + geosite code 进同一个规则集，这是"任意来源混合去重"的核心场景。
func TestGeositeMixedWithHTTP(t *testing.T) {
	base, dir := setup(t)
	dlc := makeDLC(t, dir)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q },
  "rulesets": { "mix": { "sources": ["%s/good"], "geosite": ["netflix"] } }
}`, filepath.ToSlash(dlc), base))

	res := byName(runAll(t, cfg, Options{}))["mix"]
	if !res.OK {
		t.Fatalf("混写应当成功: %v / %v", res.Err, res.FailedSources)
	}
	if res.Rules != 2 { // good.example（HTTP，收敛掉一条）+ netflix.com
		t.Errorf("规则数 = %d, want 2", res.Rules)
	}
}

// 属性差集：要 google 但不要它的广告域名。
func TestGeositeAttributeExclude(t *testing.T) {
	_, dir := setup(t)
	dlc := makeDLC(t, dir)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q },
  "rulesets": {
    "g": { "geosite": ["google"], "exclude": { "geosite": ["google@ads"] } }
  }
}`, filepath.ToSlash(dlc)))

	res := byName(runAll(t, cfg, Options{}))["g"]
	if !res.OK {
		t.Fatalf("应当成功: %v", res.Err)
	}
	if res.Rules != 1 {
		t.Errorf("规则数 = %d, want 1（只剩 google.com）", res.Rules)
	}
	if res.Diag.Subtracted == 0 {
		t.Error("差集没生效")
	}
}

// dlc.dat 拿不到是**整类输入**不可用，不能让整次运行失败 ——
// 引用 geosite 的规则集各自记账，其余照常产出。
func TestGeositeFailureDoesNotSinkTheRun(t *testing.T) {
	base, dir := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q },
  "rulesets": { "http-only": ["%s/good"], "geo": { "geosite": ["cn"] } }
}`, filepath.ToSlash(filepath.Join(dir, "missing.dat")), base))

	run := runAll(t, cfg, Options{})
	got := byName(run)
	if !got["http-only"].OK {
		t.Errorf("不引用 geosite 的规则集不该受牵连: %v", got["http-only"].Err)
	}
	if got["geo"].OK {
		t.Error("引用 geosite 的规则集应当失败")
	}
}

// bulk 展开：一条配置生成多个规则集，名字来自运行时的数据。
func TestGeositeBulkExpansion(t *testing.T) {
	_, dir := setup(t)
	dlc := makeDLC(t, dir)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q, "bulk": { "include": ["*"], "exclude": ["*@*"] } },
  "rulesets": { "keep": { "geosite": ["netflix"] } }
}`, filepath.ToSlash(dlc)))

	run := runAll(t, cfg, Options{})
	got := byName(run)
	for _, want := range []string{"keep", "geosite-google", "geosite-netflix"} {
		if got[want] == nil || !got[want].OK {
			t.Errorf("缺规则集 %q 或它失败了", want)
		}
	}
	// exclude 滤掉了属性变体
	if got["geosite-google@ads"] != nil {
		t.Error("bulk.exclude 没滤掉属性变体")
	}
	if !run.Authoritative {
		t.Error("bulk 展开成功时名单是完整的")
	}
}

// bulk 展开不了时**名单不完整** —— 发布方必须据此跳过孤儿清理，
// 否则一次 GitHub 抖动会删光整个前缀。这条在旧的 shell 发布脚本里表达不出来。
func TestGeositeBulkFailureMarksListIncomplete(t *testing.T) {
	base, dir := setup(t)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q, "bulk": { "include": ["*"] } },
  "rulesets": { "http-only": ["%s/good"] }
}`, filepath.ToSlash(filepath.Join(dir, "missing.dat")), base))

	run := runAll(t, cfg, Options{})
	if run.Authoritative {
		t.Error("bulk 没能展开时，规则集名单不该被当成完整的")
	}
	if !byName(run)["http-only"].OK {
		t.Error("其余规则集应当照常产出")
	}
}

// bulk 与配置里的规则集撞名：跳过并标记名单不完整，绝不静默覆盖。
func TestGeositeBulkNameClash(t *testing.T) {
	_, dir := setup(t)
	dlc := makeDLC(t, dir)
	cfg := load(t, fmt.Sprintf(`{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "geosite": { "file": %q, "bulk": { "prefix": "", "include": ["netflix"] } },
  "rulesets": { "netflix": { "geosite": ["google"] } }
}`, filepath.ToSlash(dlc)))

	run := runAll(t, cfg, Options{})
	if run.Authoritative {
		t.Error("撞名时名单不该被当成完整的")
	}
	// 配置里那条必须原样保留 —— 它引用的是 google 而不是 netflix
	res := byName(run)["netflix"]
	if res == nil || !res.OK || res.Rules != 2 {
		t.Errorf("配置里的规则集被 bulk 覆盖了: %+v", res)
	}
}
