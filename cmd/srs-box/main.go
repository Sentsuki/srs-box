// srs-box 把各家规则源编译成 sing-box rule-set。
//
// 用标准库 flag 而不是 cobra：子命令和参数一共十来个，为此背一个依赖不划算 ——
// 整个项目的直接依赖只有四个，这条底线值得守。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Sentsuki/srs-box/internal/config"
	"github.com/Sentsuki/srs-box/internal/pipeline"
	"github.com/Sentsuki/srs-box/internal/publish"
	"github.com/Sentsuki/srs-box/internal/report"
)

const version = "0.3.0"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "build":
		return buildCmd(args[1:])
	case "publish":
		return publishCmd(args[1:])
	case "version", "--version", "-V":
		fmt.Println("srs-box", version)
		return 0
	case "help", "--help", "-h":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "未知子命令 %q\n\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `srs-box —— 把各家规则源编译成 sing-box rule-set

用法:
  srs-box build [选项]
  srs-box publish [选项]
  srs-box version

build 选项:
  -c, --config PATH     配置文件（默认 config.json）
      --only NAME       只处理指定规则集，可重复
  -n, --dry-run         只走流程不写文件
      --strict          任一规则集失败即以非零码退出
      --report PATH     写出机器可读的运行报告
      --github-summary  写 GitHub 步骤摘要并发出注解
  -q, --quiet           只输出摘要，不输出进度

publish 选项:
  -c, --config PATH     配置文件（默认 config.json），用来取发布目标
      --report PATH     build 写出的运行报告（默认 run-report.json）
      --remote URL      仓库地址，默认由 GITHUB_REPOSITORY 推断
  -n, --dry-run         只算不推
  -q, --quiet           不输出进度

publish 从环境变量 GITHUB_TOKEN 取推送凭据。
`)
}

type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

// notifyContext 让 Ctrl-C 一路传到每个 HTTP 请求，下载立刻断。
func notifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func buildCmd(args []string) int {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		cfgPath  string
		only     stringsFlag
		dryRun   bool
		strict   bool
		reportTo string
		gh       bool
		quiet    bool
	)
	fs.StringVar(&cfgPath, "config", "config.json", "配置文件路径")
	fs.StringVar(&cfgPath, "c", "config.json", "配置文件路径（简写）")
	fs.Var(&only, "only", "只处理指定规则集，可重复")
	fs.BoolVar(&dryRun, "dry-run", false, "只走流程不写文件")
	fs.BoolVar(&dryRun, "n", false, "只走流程不写文件（简写）")
	fs.BoolVar(&strict, "strict", false, "任一规则集失败即以非零码退出")
	fs.StringVar(&reportTo, "report", "", "运行报告写到该路径")
	fs.BoolVar(&gh, "github-summary", false, "写 GitHub 步骤摘要并发出注解")
	fs.BoolVar(&quiet, "quiet", false, "只输出摘要")
	fs.BoolVar(&quiet, "q", false, "只输出摘要（简写）")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置有误: %v\n", err)
		return 2
	}

	ctx, stop := notifyContext()
	defer stop()

	progress := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
	if quiet {
		progress = nil
	} else {
		progress("配置 %s：%d 个规则集", cfgPath, len(cfg.Rulesets))
	}

	run, err := pipeline.Run(ctx, cfg, pipeline.Options{
		Only:     only,
		DryRun:   dryRun,
		Progress: progress,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "已中断")
			return 130
		}
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		return 2
	}

	if !dryRun {
		pipeline.PruneStale(cfg, run)
	}

	// 摘要走 stdout，不经过日志 —— 日志级别只该影响诊断信息，不该影响结果。
	run.Summarize(os.Stdout)

	if reportTo != "" {
		if err := run.WriteJSON(reportTo); err != nil {
			fmt.Fprintf(os.Stderr, "写运行报告失败: %v\n", err)
			return 2
		}
	}
	if gh {
		if err := run.WriteGitHubSummary(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "写 GitHub 摘要失败: %v\n", err)
			return 2
		}
	}

	counts := run.Counts()
	// 一个规则集都没产出才算基础设施坏了。单个源失败不是 CI 失败 ——
	// 逐规则集隔离的全部意义就在这里。
	if counts[report.StatusOK] == 0 {
		return 1
	}
	if strict && counts[report.StatusFailed] > 0 {
		return 1
	}
	return 0
}

func publishCmd(args []string) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		cfgPath string
		repPath string
		remote  string
		dryRun  bool
		quiet   bool
	)
	fs.StringVar(&cfgPath, "config", "config.json", "配置文件路径")
	fs.StringVar(&cfgPath, "c", "config.json", "配置文件路径（简写）")
	fs.StringVar(&repPath, "report", "run-report.json", "运行报告路径")
	fs.StringVar(&remote, "remote", "", "仓库地址")
	fs.BoolVar(&dryRun, "dry-run", false, "只算不推")
	fs.BoolVar(&dryRun, "n", false, "只算不推（简写）")
	fs.BoolVar(&quiet, "quiet", false, "不输出进度")
	fs.BoolVar(&quiet, "q", false, "不输出进度（简写）")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置有误: %v\n", err)
		return 2
	}
	rep, err := report.LoadJSON(repPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}

	var targets []publish.Target
	for _, t := range []struct {
		ext string
		a   *config.Artifact
	}{{"srs", cfg.Output.SRS}, {"json", cfg.Output.JSON}} {
		// 没写 branch = 只本地生成不发布。
		if t.a == nil || t.a.Branch == "" {
			continue
		}
		targets = append(targets, publish.Target{Dir: t.a.Dir, Branch: t.a.Branch, Ext: t.ext})
	}
	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "配置里没有任何带 branch 的产物，无事可做")
		return 0
	}

	if remote == "" {
		repo := os.Getenv("GITHUB_REPOSITORY")
		if repo == "" {
			fmt.Fprintln(os.Stderr, "错误: 没有 --remote，也没有 GITHUB_REPOSITORY")
			return 2
		}
		remote = "https://github.com/" + repo
	}

	progress := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
	if quiet {
		progress = nil
	}
	if !rep.Authoritative && progress != nil {
		progress("规则集名单不完整，本次跳过孤儿清理")
	}

	ctx, stop := notifyContext()
	defer stop()

	stats, err := publish.Run(ctx, rep, targets, publish.Options{
		Remote:   remote,
		Token:    os.Getenv("GITHUB_TOKEN"),
		DryRun:   dryRun,
		Progress: progress,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		return 1
	}
	for _, s := range stats {
		verb := "无变化"
		if s.Pushed {
			verb = "已推送"
		}
		fmt.Printf("%s: %s（产出 %d，保留 %d，清理孤儿 %d）\n",
			s.Branch, verb, s.Updated, s.Retained, s.Orphaned)
	}
	return 0
}
