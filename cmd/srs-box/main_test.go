package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/buildinfo"
)

// run() 的退出码是对 CI 的契约：0 是"跑完了"，1 是"真坏了"，2 是"用法或配置不对"，
// 130 是"被中断"。workflow 靠它决定某一步红不红，所以它属于要被钉住的行为。

const goodConfig = `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs", "branch": "srs_release" } },
  "rulesets": {
    "good": { "inline": ["DOMAIN,a.example"] }
  }
}`

// partialConfig 一好一坏：只有注释的 inline 解析后一条规则都没有。
const partialConfig = `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": {
    "good": { "inline": ["DOMAIN,a.example"] },
    "bad":  { "inline": ["# 只有注释"] }
  }
}`

const allBadConfig = `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": {
    "bad": { "inline": ["# 只有注释"] }
  }
}`

// inTempDir 切到临时工作目录：配置里的路径和输出目录都是相对的。
func inTempDir(t *testing.T, config string) string {
	t.Helper()
	dir := t.TempDir()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	if config != "" {
		if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(config), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// capture 收走 run 期间的 stdout 与 stderr —— 既要断言输出，也不想让摘要
// 糊满测试日志。
func capture(t *testing.T, fn func() int) (int, string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	code := func() int {
		defer func() {
			os.Stdout, os.Stderr = oldOut, oldErr
			w.Close()
		}()
		return fn()
	}()
	return code, <-done
}

func TestExitCodes(t *testing.T) {
	cases := []struct {
		name   string
		config string
		args   []string
		want   int
	}{
		{"没有子命令", goodConfig, nil, 2},
		{"未知子命令", goodConfig, []string{"frobnicate"}, 2},
		{"version", goodConfig, []string{"version"}, 0},
		{"help", goodConfig, []string{"help"}, 0},
		{"非法参数", goodConfig, []string{"build", "--no-such-flag"}, 2},
		{"配置不存在", "", []string{"build", "-c", "missing.json"}, 2},
		{"配置有语法错", `{ 这不是 JSON`, []string{"build"}, 2},
		{"正常构建", goodConfig, []string{"build", "--dry-run", "-q"}, 0},
		// 单个规则集失败不是 CI 失败 —— 逐规则集隔离的全部意义就在这里
		{"部分失败", partialConfig, []string{"build", "--dry-run", "-q"}, 0},
		{"部分失败 + strict", partialConfig, []string{"build", "--dry-run", "-q", "--strict"}, 1},
		// 一个都没产出才算基础设施坏了
		{"全部失败", allBadConfig, []string{"build", "--dry-run", "-q"}, 1},
		{"全部失败 + strict", allBadConfig, []string{"build", "--dry-run", "-q", "--strict"}, 1},
		{"publish 配置不存在", "", []string{"publish", "-c", "missing.json"}, 2},
		{"publish 报告不存在", goodConfig, []string{"publish", "--report", "missing.json"}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inTempDir(t, c.config)
			got, out := capture(t, func() int { return run(c.args) })
			if got != c.want {
				t.Errorf("run(%q) = %d, want %d\n输出:\n%s", c.args, got, c.want, out)
			}
		})
	}
}

func TestVersionPrintsBuildinfo(t *testing.T) {
	inTempDir(t, "")
	code, out := capture(t, func() int { return run([]string{"version"}) })
	if code != 0 {
		t.Fatalf("退出码 = %d", code)
	}
	if !strings.Contains(out, buildinfo.Version) {
		t.Errorf("version 没打出 %q: %q", buildinfo.Version, out)
	}
}

// 中断退 130，不能退 0 —— 否则 CI 里一次超时会被当成一次正常运行。
func TestInterruptExits130(t *testing.T) {
	cancelled := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}
	old := notifyContext
	notifyContext = cancelled
	t.Cleanup(func() { notifyContext = old })

	t.Run("build", func(t *testing.T) {
		inTempDir(t, goodConfig)
		code, out := capture(t, func() int { return run([]string{"build", "--dry-run", "-q"}) })
		if code != 130 {
			t.Errorf("退出码 = %d, want 130\n输出:\n%s", code, out)
		}
		if !strings.Contains(out, "已中断") {
			t.Errorf("没说清楚是中断，看不出和构建失败的区别: %q", out)
		}
	})

	t.Run("publish", func(t *testing.T) {
		dir := inTempDir(t, goodConfig)
		// 先造一份真的运行报告，好让 publish 走到真正发布那一步
		if code, out := capture(t, func() int {
			notifyContext = old // 报告本身得是正常跑出来的
			defer func() { notifyContext = cancelled }()
			return run([]string{"build", "--report", "run-report.json", "-q"})
		}); code != 0 {
			t.Fatalf("准备运行报告失败: %d\n%s", code, out)
		}
		remote := filepath.ToSlash(filepath.Join(dir, "remote.git"))
		code, out := capture(t, func() int {
			return run([]string{"publish", "--remote", remote, "-q"})
		})
		if code != 130 {
			t.Errorf("退出码 = %d, want 130\n输出:\n%s", code, out)
		}
		// 不能报成"连不上远端" —— publish 是逐目标串行推的，
		// 中断和失败要分得清才知道要不要重跑。
		//
		// 注意这条只覆盖"进 publish 时 ctx 已取消"；跑到一半被 kill
		// 那条路（git.run 里那道 ctx.Err 检查）测不到，它没有稳定的时序。
		if !strings.Contains(out, "已中断") {
			t.Errorf("中断被报成了别的错: %q", out)
		}
	})
}

// 没配 branch 就是"只本地生成不发布"，无事可做也是正常结束。
func TestPublishWithoutBranchIsNoop(t *testing.T) {
	inTempDir(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "out/srs" } },
  "rulesets": { "good": { "inline": ["DOMAIN,a.example"] } }
}`)
	if code, out := capture(t, func() int {
		return run([]string{"build", "--report", "run-report.json", "-q"})
	}); code != 0 {
		t.Fatalf("准备运行报告失败: %d\n%s", code, out)
	}
	code, out := capture(t, func() int { return run([]string{"publish", "-q"}) })
	if code != 0 {
		t.Errorf("退出码 = %d, want 0\n输出:\n%s", code, out)
	}
}

// 既没有 --remote 也没有 GITHUB_REPOSITORY 时是用法错误，不是发布失败。
func TestPublishWithoutRemoteIsUsageError(t *testing.T) {
	t.Setenv("GITHUB_REPOSITORY", "")
	inTempDir(t, goodConfig)
	if code, out := capture(t, func() int {
		return run([]string{"build", "--report", "run-report.json", "-q"})
	}); code != 0 {
		t.Fatalf("准备运行报告失败: %d\n%s", code, out)
	}
	code, out := capture(t, func() int { return run([]string{"publish", "-q"}) })
	if code != 2 {
		t.Errorf("退出码 = %d, want 2\n输出:\n%s", code, out)
	}
}
