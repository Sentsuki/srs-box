// Package publish 把产物推到发布分支。
//
// 取代 workflow 里那段 shell（77 行、8 次 jq、两个中间文本文件）。搬进 Go 的是
// **决策逻辑**，不是 git 本身 —— git 不可链接、Actions 里本来就有，用 go-git
// 替换几次调用要背一个重依赖而且它的 shallow clone 一向偏弱，不划算。
//
// 三态语义必须原样保留，而且现在终于能测：
//
//	本次产出         → 覆盖
//	配置里有但失败   → 保留上一次发布的文件
//	不在配置里       → 当孤儿删掉
//
// 发布分支**永远只有一个提交**：每次发布造一个无父提交顶上去。产物分支是
// "当前快照"，它的历史没有任何使用者 —— 每天两个提交、几十个二进制文件，
// 一年七百多个提交，旧版本的 blob 永远可达，远端只增不减。断掉父链之后旧对象
// 立刻不可达，GitHub 的 gc 会回收它们。见 doc/doc.md「发布分支只有一个提交」。
//
// 还有第四态：规则集名单本身不可信时（geosite.bulk 的通配符没能展开），
// **跳过孤儿清理**。否则一次 GitHub 抖动就会删光整个前缀。这条在旧的 shell 里
// 根本表达不出来 —— 它只看得见 configured.txt 里有什么，看不见那份名单可不可信。
package publish

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Sentsuki/srs-box/internal/report"
)

// Target 是一个发布目标。
type Target struct {
	// Dir 是本地产物目录。
	Dir string
	// Branch 是发布分支。
	Branch string
	// Ext 是产物扩展名（srs / json），决定认哪些文件。
	Ext string
}

// Options 是发布参数。
type Options struct {
	// Remote 是要推送的仓库地址，形如 https://github.com/owner/name。
	Remote string
	// Token 用于推送鉴权。空则按匿名处理（本地 file:// 远端够用）。
	Token string
	// DryRun 只算不推。
	DryRun bool
	// Progress 可为 nil。
	Progress func(format string, args ...any)
}

// Stats 是一个目标的发布结果。
type Stats struct {
	Branch    string
	Updated   int
	Retained  int
	Orphaned  int
	Pushed    bool
	SkipClean bool
}

// Run 按运行报告发布全部目标。
func Run(ctx context.Context, rep *report.File, targets []Target, opts Options) ([]Stats, error) {
	if opts.Remote == "" {
		return nil, errors.New("没有远端地址")
	}
	produced := toSet(rep.Produced())
	configured := toSet(rep.Configured())

	var stats []Stats
	for _, target := range targets {
		s, err := publishOne(ctx, target, produced, configured, rep.Authoritative, opts)
		if err != nil {
			return stats, fmt.Errorf("发布 %s 失败: %w", target.Branch, err)
		}
		stats = append(stats, s)
	}
	return stats, nil
}

func publishOne(ctx context.Context, t Target, produced, configured map[string]bool, authoritative bool, opts Options) (Stats, error) {
	stats := Stats{Branch: t.Branch, SkipClean: !authoritative}

	work, err := os.MkdirTemp("", "srsbox-publish-")
	if err != nil {
		return stats, err
	}
	defer os.RemoveAll(work)

	g, err := newGit(work, opts)
	if err != nil {
		return stats, err
	}
	defer g.cleanup()

	exists, err := g.cloneBranch(ctx, t.Branch)
	if err != nil {
		return stats, err
	}
	if !exists {
		if err := g.initBranch(ctx, t.Branch); err != nil {
			return stats, err
		}
		progress(opts, "  %s: 远端还没有这个分支，新建", t.Branch)
	}

	// 1) 清孤儿：配置里已经没有这个名字了，说明是刻意删除或改名。
	//
	// 名单不可信时整步跳过 —— 宁可留着几个孤儿，也不能误删几百个还在用的。
	if authoritative {
		names, err := filepath.Glob(filepath.Join(work, "*."+t.Ext))
		if err != nil {
			return stats, err
		}
		sort.Strings(names)
		for _, path := range names {
			name := strings.TrimSuffix(filepath.Base(path), "."+t.Ext)
			if configured[name] {
				continue
			}
			if err := os.Remove(path); err != nil {
				return stats, err
			}
			stats.Orphaned++
			progress(opts, "  %s: 清理孤儿 %s", t.Branch, filepath.Base(path))
		}
	} else {
		progress(opts, "  %s: 规则集名单不完整，跳过孤儿清理", t.Branch)
	}

	// 2) 覆盖本次产出。没产出的名字保持上一次的文件不动 —— 这就是"保留"。
	for _, name := range sortedKeys(produced) {
		src := filepath.Join(t.Dir, name+"."+t.Ext)
		data, err := os.ReadFile(src)
		if err != nil {
			// 报告说产出了但文件不在，是真的不对劲，不该悄悄跳过。
			return stats, fmt.Errorf("运行报告说 %s 已产出，但读不到 %s: %w", name, src, err)
		}
		if err := os.WriteFile(filepath.Join(work, name+"."+t.Ext), data, 0o644); err != nil {
			return stats, err
		}
		stats.Updated++
	}

	// 3) 统计保留数：分支上剩下的、不是本次产出的。
	after, err := filepath.Glob(filepath.Join(work, "*."+t.Ext))
	if err != nil {
		return stats, err
	}
	for _, path := range after {
		name := strings.TrimSuffix(filepath.Base(path), "."+t.Ext)
		if !produced[name] {
			stats.Retained++
		}
	}

	if err := g.add(ctx); err != nil {
		return stats, err
	}
	changed, err := g.hasStagedChanges(ctx)
	if err != nil {
		return stats, err
	}
	if !changed {
		progress(opts, "  %s: 无变化，跳过推送（产出 %d，保留 %d，孤儿 %d）",
			t.Branch, stats.Updated, stats.Retained, stats.Orphaned)
		return stats, nil
	}
	if opts.DryRun {
		progress(opts, "  %s: dry-run，不推送（产出 %d，保留 %d，孤儿 %d）",
			t.Branch, stats.Updated, stats.Retained, stats.Orphaned)
		return stats, nil
	}
	msg := fmt.Sprintf("update %s: %d updated, %d retained, %d orphaned",
		strings.TrimSuffix(t.Branch, "_release"), stats.Updated, stats.Retained, stats.Orphaned)
	commit, err := g.commitTree(ctx, msg)
	if err != nil {
		return stats, err
	}
	if err := g.push(ctx, commit, t.Branch); err != nil {
		return stats, err
	}
	stats.Pushed = true
	progress(opts, "  %s: 已推送（产出 %d，保留 %d，孤儿 %d）",
		t.Branch, stats.Updated, stats.Retained, stats.Orphaned)
	return stats, nil
}

func progress(opts Options, format string, args ...any) {
	if opts.Progress != nil {
		opts.Progress(format, args...)
	}
}

func toSet(in []string) map[string]bool {
	out := make(map[string]bool, len(in))
	for _, v := range in {
		out[v] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---------------- git ----------------

type git struct {
	work    string
	remote  string
	credFil string
	// base 是 clone 下来的分支 tip。推送时拿它做 --force-with-lease 的预期值：
	// 强推会盖掉别人的提交，而"盖掉的必须正是我读过的那一版"这个条件，
	// 恰好等价于原先靠 non-fast-forward 拒绝拿到的那层保护。
	base string
}

// newGit 准备 git 调用环境。
//
// token 走 credential store 文件（0600），**不进命令行参数**。塞进远端 URL 的话
// 它会成为 exec.Command 的一个参数，而 exec.ExitError 的默认输出和很多日志封装
// 都会把命令行带出来 —— 一次报错就把 token 打进 CI 日志了。
func newGit(work string, opts Options) (*git, error) {
	g := &git{work: work, remote: opts.Remote}
	if opts.Token == "" {
		return g, nil
	}
	host := opts.Remote
	if idx := strings.Index(host, "://"); idx >= 0 {
		rest := host[idx+3:]
		if slash := strings.IndexByte(rest, '/'); slash >= 0 {
			rest = rest[:slash]
		}
		host = host[:idx+3] + rest
	}
	file, err := os.CreateTemp("", "srsbox-cred-")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	scheme, hostname, _ := strings.Cut(host, "://")
	if _, err := fmt.Fprintf(file, "%s://x-access-token:%s@%s\n", scheme, opts.Token, hostname); err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	file.Close()
	g.credFil = file.Name()
	return g, nil
}

func (g *git) cleanup() {
	if g.credFil != "" {
		os.Remove(g.credFil)
	}
}

func (g *git) args(extra ...string) []string {
	base := []string{
		"-c", "user.name=github-actions[bot]",
		"-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
		"-c", "core.autocrlf=false", // 产物必须逐字节稳定，绝不让 git 改行尾
	}
	if g.credFil != "" {
		base = append(base, "-c", "credential.helper=", "-c",
			"credential.helper=store --file="+g.credFil)
	}
	return append(base, extra...)
}

func (g *git) run(ctx context.Context, inWork bool, extra ...string) ([]byte, error) {
	args := g.args(extra...)
	cmd := exec.CommandContext(ctx, "git", args...)
	if inWork {
		cmd.Dir = g.work
	}
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		// 跑到一半被打断时 git 是被 kill 的，err 只是个退出状态，调用方
		// errors.Is(err, context.Canceled) 认不出来 —— 于是一次 Ctrl-C 会
		// 报成"发布失败"。把中断原样带出去。
		//
		// （ctx 在启动前就已取消是另一回事：那时 CommandContext 直接返回
		// ctx.Err()，本来就认得出来。这里管的是启动之后那一段。）
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out.Bytes(), fmt.Errorf("git %s 被中断: %w", extra[0], ctxErr)
		}
		// 只带 stderr，不带命令行 —— 命令行里有 credential 文件路径，
		// 而且把完整 argv 打出去是 token 泄漏最常见的入口。
		return out.Bytes(), fmt.Errorf("git %s: %w: %s",
			extra[0], err, strings.TrimSpace(errBuf.String()))
	}
	return out.Bytes(), nil
}

// cloneBranch 浅克隆一个分支。返回 false 表示远端没有这个分支。
//
// 旧 shell 是 `git clone … 2>/dev/null || git init` —— 分支不存在（首次发布，
// 应当 init）和网络抖动被同等对待，后果不是丢数据（无关历史推上去会被拒），
// 但错误信息会非常迷惑。这里先问一句再决定。
func (g *git) cloneBranch(ctx context.Context, branch string) (bool, error) {
	out, err := g.run(ctx, false, "ls-remote", "--heads", g.remote, "refs/heads/"+branch)
	if err != nil {
		return false, fmt.Errorf("连不上远端: %w", err)
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return false, nil
	}
	if _, err := g.run(ctx, false, "clone", "--quiet", "--depth", "1",
		"--branch", branch, g.remote, g.work); err != nil {
		return false, err
	}
	head, err := g.run(ctx, true, "rev-parse", "HEAD")
	if err != nil {
		return false, err
	}
	g.base = strings.TrimSpace(string(head))
	return true, nil
}

func (g *git) initBranch(ctx context.Context, branch string) error {
	if _, err := g.run(ctx, false, "init", "--quiet", "-b", branch, g.work); err != nil {
		return err
	}
	_, err := g.run(ctx, true, "remote", "add", "origin", g.remote)
	return err
}

func (g *git) add(ctx context.Context) error {
	_, err := g.run(ctx, true, "add", "-A")
	return err
}

func (g *git) hasStagedChanges(ctx context.Context) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", g.args("diff", "--cached", "--quiet")...)
	cmd.Dir = g.work
	err := cmd.Run()
	if err == nil {
		return false, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return true, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, fmt.Errorf("git diff --cached 被中断: %w", ctxErr)
	}
	return false, fmt.Errorf("git diff --cached: %w", err)
}

// commitTree 用当前索引造一个**无父**提交，返回它的 sha。
//
// 走 write-tree + commit-tree 这对底层命令，而不是 checkout --orphan + commit：
// 后者要先借一个临时分支名（还得防着跟发布分支撞名），而且"孤儿检出之后已暂存
// 的改动还在不在索引里"要靠 checkout 的语义去推。这里直接拿索引造树，
// HEAD 和工作区一动不动，读起来也就是它字面的意思。
func (g *git) commitTree(ctx context.Context, message string) (string, error) {
	tree, err := g.run(ctx, true, "write-tree")
	if err != nil {
		return "", err
	}
	sha, err := g.run(ctx, true, "commit-tree", strings.TrimSpace(string(tree)), "-m", message)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(sha)), nil
}

// push 把 commit 推成分支的新 tip。
//
// 新提交没有父，所以对已存在的分支必然是 non-fast-forward，必须强推。用
// --force-with-lease 而不是 --force：预期值是 clone 时读到的 tip，别人在这中间
// 推过东西就会被拒绝，而不是被悄悄盖掉。分支还不存在时没有预期值可给，
// 普通推送本来就只有分支仍不存在才会成功。
func (g *git) push(ctx context.Context, commit, branch string) error {
	args := []string{"push", "--quiet"}
	if g.base != "" {
		args = append(args, "--force-with-lease=refs/heads/"+branch+":"+g.base)
	}
	args = append(args, "origin", commit+":refs/heads/"+branch)
	_, err := g.run(ctx, true, args...)
	return err
}
