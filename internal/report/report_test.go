package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

func sample() *Run {
	return &Run{
		Configured: []string{"a", "b", "c", "d"},
		Selected:   map[string]bool{"a": true, "b": true, "c": true},
		Results: []*Result{
			{Name: "a", OK: true, Rules: 1234, SRSSize: 4096},
			{Name: "b", OK: false, Err: errors.New("所有输入都不可用"),
				FailedSources: []string{"url https://x.example: HTTP 404"}},
			{Name: "c", OK: true, Rules: 7, SRSSize: 128},
		},
		Authoritative: true,
	}
}

// 三种结局必须分得清：失败项和被 --only 跳过的项，发布方都要保留旧文件，
// 但只有"配置里已经没有了"才该当孤儿清掉。
func TestThreeStatuses(t *testing.T) {
	run := sample()
	for name, want := range map[string]Status{
		"a": StatusOK,
		"b": StatusFailed,
		"c": StatusOK,
		"d": StatusSkipped, // 不在 Selected 里 —— 这次根本没跑
	} {
		if got := run.Status(name); got != want {
			t.Errorf("Status(%q) = %q, want %q", name, got, want)
		}
	}
	counts := run.Counts()
	if counts[StatusOK] != 2 || counts[StatusFailed] != 1 || counts[StatusSkipped] != 1 {
		t.Errorf("counts = %v", counts)
	}
}

// 没用 --only 时，所有配置项都在选中范围内，不该有 skipped。
func TestNoSelectionMeansNothingSkipped(t *testing.T) {
	run := &Run{
		Configured: []string{"a", "b"},
		Results:    []*Result{{Name: "a", OK: true}, {Name: "b", OK: true}},
	}
	if got := run.Counts()[StatusSkipped]; got != 0 {
		t.Errorf("skipped = %d, want 0", got)
	}
}

func TestWriteJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "run-report.json")
	if err := sample().WriteJSON(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got reportFile
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("产物不是合法 JSON: %v", err)
	}
	if got.Schema != Schema {
		t.Errorf("schema = %d", got.Schema)
	}
	if !got.Authoritative {
		t.Error("authoritative 应为 true")
	}
	if got.Counts["configured"] != 4 || got.Counts["ok"] != 2 ||
		got.Counts["failed"] != 1 || got.Counts["skipped"] != 1 {
		t.Errorf("counts = %v", got.Counts)
	}
	// 配置声明的全集都要在 —— 发布方靠它区分"失败"和"孤儿"
	if len(got.Rulesets) != 4 {
		t.Fatalf("条目数 = %d, want 4", len(got.Rulesets))
	}
	byName := map[string]reportEntry{}
	for _, e := range got.Rulesets {
		byName[e.Name] = e
	}
	if byName["d"].Status != StatusSkipped {
		t.Errorf("d 应当是 skipped，实际 %q", byName["d"].Status)
	}
	if byName["b"].Error == "" || len(byName["b"].FailedSources) != 1 {
		t.Errorf("失败项没带上原因: %+v", byName["b"])
	}
}

// bulk 通配符没能展开时，名单不完整 —— 发布方必须据此跳过孤儿清理。
func TestAuthoritativeFlagTravelsToReport(t *testing.T) {
	run := sample()
	run.Authoritative = false
	path := filepath.Join(t.TempDir(), "r.json")
	if err := run.WriteJSON(path); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), `"authoritative": false`) {
		t.Errorf("authoritative 没写进报告: %s", raw)
	}
}

func TestSummarize(t *testing.T) {
	run := sample()
	run.Results[0].Diag.Skip("GEOIP")
	run.Results[0].Diag.Skip("GEOIP")
	run.Results[0].Diag.Collapsed = 189
	run.Results[0].Diag.Aggregated = 3
	run.Results[2].Diag.BadValue("domain", "非法域名: '-bad-.com'")

	var buf bytes.Buffer
	run.Summarize(&buf)
	out := buf.String()

	for _, want := range []string{
		"✓ a", "✗ b", "1,234 条", // 千分位
		"跳过 GEOIP×2",
		"收敛 -189", // 收敛必须单列 —— 它会随源的可用性波动
		"CIDR 聚合 -3",
		"所有输入都不可用",
		"源不可用: url https://x.example: HTTP 404",
		"产出 2/3 个规则集",
		"非法值样例:",
		"失败 1 个: b",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("摘要里缺 %q:\n%s", want, out)
		}
	}
}

// 摘要顺序必须稳定，否则 CI 日志每次 diff 都是噪声。
func TestSummarizeIsDeterministic(t *testing.T) {
	render := func() string {
		run := sample()
		run.Results[0].Diag.Skip("GEOIP")
		run.Results[0].Diag.Skip("URL-REGEX")
		run.Results[0].Diag.Skip("URL-REGEX")
		var buf bytes.Buffer
		run.Summarize(&buf)
		return buf.String()
	}
	first := render()
	for i := 0; i < 20; i++ {
		if got := render(); got != first {
			t.Fatalf("摘要不稳定:\n%s\n---\n%s", first, got)
		}
	}
	// 计数相同的项按名字排序，不靠 map 迭代顺序
	if !strings.Contains(first, "URL-REGEX×2, GEOIP×1") {
		t.Errorf("诊断排序不对:\n%s", first)
	}
}

func TestGitHubAnnotationsAndSummary(t *testing.T) {
	dir := t.TempDir()
	summaryPath := filepath.Join(dir, "summary.md")
	t.Setenv("GITHUB_STEP_SUMMARY", summaryPath)

	var annotations bytes.Buffer
	if err := sample().WriteGitHubSummary(&annotations); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(annotations.String(), "::warning title=规则集未更新::b:") {
		t.Errorf("注解不对: %s", annotations.String())
	}
	raw, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	if !strings.Contains(out, "## 规则集 2 / 4") {
		t.Errorf("步骤摘要标题不对:\n%s", out)
	}
	if !strings.Contains(out, "保留上一次发布的文件") {
		t.Errorf("没说清失败项会保留旧文件:\n%s", out)
	}
	// 被 --only 跳过的不该报成失败
	if strings.Contains(out, "`d`") {
		t.Errorf("skipped 项不该出现在失败列表里:\n%s", out)
	}
}

func TestGitHubSummaryWarnsWhenNotAuthoritative(t *testing.T) {
	t.Setenv("GITHUB_STEP_SUMMARY", filepath.Join(t.TempDir(), "s.md"))
	run := sample()
	run.Authoritative = false
	if err := run.WriteGitHubSummary(&bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(os.Getenv("GITHUB_STEP_SUMMARY"))
	if !strings.Contains(string(raw), "跳过孤儿清理") {
		t.Errorf("名单不完整时应当提醒:\n%s", raw)
	}
}

// 没设 GITHUB_STEP_SUMMARY 时只发注解，不该报错。
func TestGitHubSummaryWithoutEnv(t *testing.T) {
	t.Setenv("GITHUB_STEP_SUMMARY", "")
	if err := sample().WriteGitHubSummary(&bytes.Buffer{}); err != nil {
		t.Errorf("本地运行不该失败: %v", err)
	}
}

func TestHumanSizeAndThousands(t *testing.T) {
	for _, c := range []struct {
		in   int64
		want string
	}{{0, "0 B"}, {512, "512 B"}, {4096, "4.0 KB"}, {1536 * 1024, "1.5 MB"}} {
		if got := humanSize(c.in); got != c.want {
			t.Errorf("humanSize(%d) = %q, want %q", c.in, got, c.want)
		}
	}
	for _, c := range []struct {
		in   int
		want string
	}{{0, "0"}, {999, "999"}, {1000, "1,000"}, {1234567, "1,234,567"}} {
		if got := thousands(c.in); got != c.want {
			t.Errorf("thousands(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

var _ = ruleset.Diagnostics{}
