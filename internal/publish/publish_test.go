package publish

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/report"
)

// 用本地裸仓库当远端：完全离线，但走的是真 git，三态语义才算真被验证过。
func bareRemote(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("没有 git")
	}
	dir := filepath.Join(t.TempDir(), "remote.git")
	run(t, "", "init", "--quiet", "--bare", dir)
	return filepath.ToSlash(dir)
}

func run(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// seed 在远端分支上预置一批文件，模拟"上一次发布"。
func seed(t *testing.T, remote, branch, ext string, names ...string) {
	t.Helper()
	work := t.TempDir()
	run(t, "", "init", "--quiet", "-b", branch, work)
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(work, n+"."+ext), []byte("旧内容 "+n+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run(t, work, "add", "-A")
	run(t, work, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "seed")
	run(t, work, "remote", "add", "origin", remote)
	run(t, work, "push", "--quiet", "origin", "HEAD:"+branch)
}

// listRemote 列出远端分支上的文件名。
func listRemote(t *testing.T, remote, branch string) []string {
	t.Helper()
	out := run(t, "", "--no-pager", "-C", remote, "ls-tree", "--name-only", branch)
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line != "" {
			names = append(names, line)
		}
	}
	sort.Strings(names)
	return names
}

func readRemote(t *testing.T, remote, branch, file string) string {
	t.Helper()
	return run(t, "", "--no-pager", "-C", remote, "show", branch+":"+file)
}

// makeReport 造一份运行报告并写出产物目录。
func makeReport(t *testing.T, dir, ext string, statuses map[string]report.Status, authoritative bool) *report.File {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var entries []report.Entry
	for _, name := range sortedNames(statuses) {
		entries = append(entries, report.Entry{Name: name, Status: statuses[name]})
		if statuses[name] == report.StatusOK {
			if err := os.WriteFile(filepath.Join(dir, name+"."+ext), []byte("新内容 "+name+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	return &report.File{Schema: report.Schema, Authoritative: authoritative, Entries: entries}
}

func sortedNames(m map[string]report.Status) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// 三态语义：产出的覆盖、失败的保留、不在配置里的清掉。
func TestThreeStatePublish(t *testing.T) {
	remote := bareRemote(t)
	// 上一次发布留下四个文件，其中 gone 这次已经不在配置里了
	seed(t, remote, "srs_release", "srs", "fresh", "stale", "gone", "skipped")

	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "srs", map[string]report.Status{
		"fresh":   report.StatusOK,      // 本次产出 → 覆盖
		"stale":   report.StatusFailed,  // 配置里有但失败 → 保留旧文件
		"skipped": report.StatusSkipped, // --only 跳过 → 同样保留
		"newone":  report.StatusOK,      // 新增
		// gone 不在报告里 → 不在配置里 → 孤儿
	}, true)

	stats, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: remote})
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	got := listRemote(t, remote, "srs_release")
	want := []string{"fresh.srs", "newone.srs", "skipped.srs", "stale.srs"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("远端文件 = %v, want %v", got, want)
	}
	// 覆盖的是新内容
	if c := readRemote(t, remote, "srs_release", "fresh.srs"); !strings.Contains(c, "新内容") {
		t.Errorf("fresh 没被覆盖: %q", c)
	}
	// 失败与跳过的保留旧内容 —— 这是"保留上一次发布"的核心
	for _, name := range []string{"stale", "skipped"} {
		if c := readRemote(t, remote, "srs_release", name+".srs"); !strings.Contains(c, "旧内容") {
			t.Errorf("%s 应当保留旧内容，实际 %q", name, c)
		}
	}
	s := stats[0]
	if s.Updated != 2 || s.Retained != 2 || s.Orphaned != 1 || !s.Pushed {
		t.Errorf("统计不对: %+v", s)
	}
}

// 名单不可信时必须跳过孤儿清理 —— 否则一次 GitHub 抖动会删光整个前缀。
// 这条在旧的 shell 发布脚本里根本表达不出来。
func TestSkipsOrphanCleanupWhenNotAuthoritative(t *testing.T) {
	remote := bareRemote(t)
	seed(t, remote, "srs_release", "srs", "keep", "geosite-a", "geosite-b", "geosite-c")

	dir := filepath.Join(t.TempDir(), "out")
	// 报告里只有 keep —— bulk 没能展开，geosite-* 全都"不在配置里"
	rep := makeReport(t, dir, "srs", map[string]report.Status{
		"keep": report.StatusOK,
	}, false) // ← 名单不完整

	stats, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: remote})
	if err != nil {
		t.Fatal(err)
	}
	got := listRemote(t, remote, "srs_release")
	want := []string{"geosite-a.srs", "geosite-b.srs", "geosite-c.srs", "keep.srs"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("名单不可信时不该删任何文件，实际 %v", got)
	}
	if stats[0].Orphaned != 0 || !stats[0].SkipClean {
		t.Errorf("统计应当标明跳过了清理: %+v", stats[0])
	}
}

// 对照组：同样的场景，名单可信时就该清掉。
func TestCleansOrphansWhenAuthoritative(t *testing.T) {
	remote := bareRemote(t)
	seed(t, remote, "srs_release", "srs", "keep", "geosite-a")

	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "srs", map[string]report.Status{"keep": report.StatusOK}, true)

	if _, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: remote}); err != nil {
		t.Fatal(err)
	}
	if got := listRemote(t, remote, "srs_release"); !reflect.DeepEqual(got, []string{"keep.srs"}) {
		t.Errorf("孤儿没清掉: %v", got)
	}
}

// 远端还没有这个分支时新建，而不是报错。
func TestCreatesMissingBranch(t *testing.T) {
	remote := bareRemote(t)
	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "json", map[string]report.Status{"a": report.StatusOK}, true)

	if _, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "json_release", Ext: "json"}},
		Options{Remote: remote}); err != nil {
		t.Fatalf("首次发布应当自动建分支: %v", err)
	}
	if got := listRemote(t, remote, "json_release"); !reflect.DeepEqual(got, []string{"a.json"}) {
		t.Errorf("新建分支内容 = %v", got)
	}
}

// 连不上远端要报错，而不是当成"分支不存在"去新建一个空的推上去。
// 旧 shell 的 `clone … || git init` 把这两种情况混为一谈。
func TestUnreachableRemoteIsAnError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "srs", map[string]report.Status{"a": report.StatusOK}, true)
	_, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: filepath.ToSlash(filepath.Join(t.TempDir(), "nonexistent.git"))})
	if err == nil {
		t.Fatal("连不上远端应当报错")
	}
	if !strings.Contains(err.Error(), "连不上远端") {
		t.Errorf("错误信息不对: %v", err)
	}
}

// 内容没变就不推 —— 每天一次定时任务不该产生空提交。
func TestNoPushWhenUnchanged(t *testing.T) {
	remote := bareRemote(t)
	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "srs", map[string]report.Status{"a": report.StatusOK}, true)
	target := []Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}}

	first, err := Run(context.Background(), rep, target, Options{Remote: remote})
	if err != nil {
		t.Fatal(err)
	}
	if !first[0].Pushed {
		t.Fatal("第一次应当推送")
	}
	second, err := Run(context.Background(), rep, target, Options{Remote: remote})
	if err != nil {
		t.Fatal(err)
	}
	if second[0].Pushed {
		t.Error("内容没变不该产生空提交")
	}
}

func TestDryRunDoesNotPush(t *testing.T) {
	remote := bareRemote(t)
	seed(t, remote, "srs_release", "srs", "old")
	dir := filepath.Join(t.TempDir(), "out")
	rep := makeReport(t, dir, "srs", map[string]report.Status{"a": report.StatusOK}, true)

	stats, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: remote, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats[0].Pushed {
		t.Error("dry-run 不该推送")
	}
	if got := listRemote(t, remote, "srs_release"); !reflect.DeepEqual(got, []string{"old.srs"}) {
		t.Errorf("dry-run 改动了远端: %v", got)
	}
}

// 报告说产出了但文件不在，是真的不对劲，不该悄悄跳过。
func TestMissingProducedFileIsAnError(t *testing.T) {
	remote := bareRemote(t)
	dir := t.TempDir()
	rep := &report.File{Schema: report.Schema, Authoritative: true,
		Entries: []report.Entry{{Name: "ghost", Status: report.StatusOK}}}

	_, err := Run(context.Background(), rep,
		[]Target{{Dir: dir, Branch: "srs_release", Ext: "srs"}},
		Options{Remote: remote})
	if err == nil {
		t.Fatal("产出文件缺失应当报错")
	}
	if !strings.Contains(err.Error(), "读不到") {
		t.Errorf("错误信息不对: %v", err)
	}
}

// token 绝不能出现在命令行参数里 —— exec.ExitError 的默认输出会把 argv 带出来，
// 一次报错就把它打进 CI 日志了。
func TestTokenNeverInCommandLine(t *testing.T) {
	g, err := newGit(t.TempDir(), Options{
		Remote: "https://github.com/owner/name",
		Token:  "ghp_SUPERSECRET_TOKEN_VALUE",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer g.cleanup()

	args := g.args("push", "origin", "HEAD:srs_release")
	for _, a := range args {
		if strings.Contains(a, "SUPERSECRET") {
			t.Fatalf("token 出现在命令行参数里: %q", a)
		}
	}
	// 它应当在一个 0600 的凭据文件里
	if g.credFil == "" {
		t.Fatal("没生成凭据文件")
	}
	info, err := os.Stat(g.credFil)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeIsUnix() && info.Mode().Perm() != 0o600 {
		t.Errorf("凭据文件权限 = %o, want 600", info.Mode().Perm())
	}
	body, err := os.ReadFile(g.credFil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "SUPERSECRET") {
		t.Error("凭据文件里没有 token")
	}
	// cleanup 之后必须删掉
	g.cleanup()
	if _, err := os.Stat(g.credFil); !os.IsNotExist(err) {
		t.Error("凭据文件没被删掉")
	}
}

func runtimeIsUnix() bool { return os.PathSeparator == '/' }

// 产物必须逐字节稳定，绝不让 git 改行尾。
func TestDisablesAutoCRLF(t *testing.T) {
	g, err := newGit(t.TempDir(), Options{Remote: "x"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(g.args("status"), " ")
	if !strings.Contains(joined, "core.autocrlf=false") {
		t.Errorf("没关掉 autocrlf: %s", joined)
	}
}

// ---------------- 运行报告读回 ----------------

func TestLoadReportRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run-report.json")
	run := &report.Run{
		Configured:    []string{"a", "b", "c"},
		Selected:      map[string]bool{"a": true, "b": true},
		Authoritative: false,
		Results: []*report.Result{
			{Name: "a", OK: true, Rules: 10},
			{Name: "b", OK: false},
		},
	}
	if err := run.WriteJSON(path); err != nil {
		t.Fatal(err)
	}
	got, err := report.LoadJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Authoritative {
		t.Error("authoritative 没读回来")
	}
	if !reflect.DeepEqual(got.Produced(), []string{"a"}) {
		t.Errorf("Produced() = %v", got.Produced())
	}
	if !reflect.DeepEqual(got.Configured(), []string{"a", "b", "c"}) {
		t.Errorf("Configured() = %v", got.Configured())
	}
}

// schema 对不上要明确报错 —— 报告和二进制版本不匹配时按老格式发布会出事。
func TestLoadReportRejectsWrongSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.json")
	raw, _ := json.Marshal(map[string]any{
		"schema":   report.Schema + 1,
		"rulesets": []map[string]any{{"name": "a", "status": "ok"}},
	})
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := report.LoadJSON(path); err == nil {
		t.Error("schema 不匹配应当报错")
	}
}
