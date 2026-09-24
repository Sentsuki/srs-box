// Package report 汇总一次运行的结果。
//
// 摘要直接写 stdout，**不经过 logger**。Python 版之前把整个摘要打成 INFO 日志，
// 而配置里的日志级别会把它整体过滤掉 —— CI 日志里永远看不到哪个规则集失败、
// 为什么失败。日志级别只该影响诊断信息，不该影响结果。
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Sentsuki/srs-box/internal/ruleset"
)

// Status 是一个规则集在本次运行里的结局。
type Status string

const (
	// StatusOK 本次产出了。
	StatusOK Status = "ok"
	// StatusFailed 配置里声明着，但这次没产出 —— 发布方应保留上一次的文件。
	StatusFailed Status = "failed"
	// StatusSkipped 被 --only 排除在外，这次根本没跑 —— 同样必须保留旧文件，
	// 否则一次 --only 就会删光其余全部。
	StatusSkipped Status = "skipped"
)

// Result 是单个规则集的最终状态。
type Result struct {
	Name string
	OK   bool
	// Rules 是变换之后的规则条数。
	Rules int
	Err   error
	Diag  ruleset.Diagnostics

	JSONPath string
	SRSPath  string
	SRSSize  int64

	// FailedSources 是取不到或解析失败的输入。部分失败不影响构建，
	// 但必须出现在摘要里 —— 静默少一半规则是最难发现的失败形态。
	FailedSources []string
}

// Run 是一次运行的全部结果。
type Run struct {
	Results []*Result
	// Configured 是配置里声明的全部规则集名。
	Configured []string
	// Selected 是本次实际要处理的子集（--only）。
	Selected map[string]bool
	// Authoritative 报告 Configured 这份清单是否完整。
	//
	// geosite.bulk 带通配符时，真实的规则集名单要下载完才知道；provider 整体
	// 失败时这份清单就不完整，**此时绝不能做孤儿清理** —— 否则一次 GitHub 抖动
	// 会删光整个 geosite- 前缀。这条在旧的 shell 发布脚本里根本表达不出来，
	// 它只看得见 configured.txt 里有什么，看不见"这份名单本身可不可信"。
	Authoritative bool
}

// Status 返回某个规则集的结局。
func (r *Run) Status(name string) Status {
	if len(r.Selected) > 0 && !r.Selected[name] {
		return StatusSkipped
	}
	for _, res := range r.Results {
		if res.Name == name {
			if res.OK {
				return StatusOK
			}
			return StatusFailed
		}
	}
	return StatusFailed
}

// Counts 统计三种结局。
func (r *Run) Counts() map[Status]int {
	out := map[Status]int{StatusOK: 0, StatusFailed: 0, StatusSkipped: 0}
	for _, name := range r.Configured {
		out[r.Status(name)]++
	}
	return out
}

// ---------------- 机器可读报告 ----------------

// Schema 是运行报告的版本。
//
// 它现在是**内部契约** —— 写它和读它的是同一个程序（build 与 publish），
// 不再需要顾虑 shell 里的 jq 表达式，加字段是自由的。
const Schema = 2

type reportFile struct {
	Schema        int            `json:"schema"`
	GeneratedAt   string         `json:"generated_at"`
	Authoritative bool           `json:"authoritative"`
	Counts        map[string]int `json:"counts"`
	Rulesets      []reportEntry  `json:"rulesets"`
}

type reportEntry struct {
	Name          string   `json:"name"`
	Status        Status   `json:"status"`
	Rules         int      `json:"rules,omitempty"`
	SRSSize       int64    `json:"srs_size,omitempty"`
	Error         string   `json:"error,omitempty"`
	FailedSources []string `json:"failed_sources,omitempty"`
}

// WriteJSON 把运行报告写到 path。
//
// 发布方需要区分两种"产物缺席"：配置里还声明着但这次没产出（源挂了，应保留
// 上一次的文件），和配置里已经没有了（改名或删除，应当作孤儿清掉）。光看输出
// 目录分不出这两者，所以这里把配置声明的全集连同每一项的状态一起写出来。
func (r *Run) WriteJSON(path string) error {
	byName := map[string]*Result{}
	for _, res := range r.Results {
		byName[res.Name] = res
	}
	entries := make([]reportEntry, 0, len(r.Configured))
	for _, name := range r.Configured {
		entry := reportEntry{Name: name, Status: r.Status(name)}
		if res := byName[name]; res != nil {
			entry.Rules = res.Rules
			entry.SRSSize = res.SRSSize
			if res.Err != nil {
				entry.Error = res.Err.Error()
			}
			entry.FailedSources = res.FailedSources
		}
		entries = append(entries, entry)
	}
	counts := map[string]int{"configured": len(r.Configured)}
	for status, n := range r.Counts() {
		counts[string(status)] = n
	}

	payload := reportFile{
		Schema:        Schema,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Authoritative: r.Authoritative,
		Counts:        counts,
		Rulesets:      entries,
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

// ---------------- 终端摘要 ----------------

// Summarize 把摘要写到 w。
func (r *Run) Summarize(w io.Writer) {
	results := append([]*Result(nil), r.Results...)
	sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

	width := 10
	for _, res := range results {
		if len(res.Name) > width {
			width = len(res.Name)
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "规则集")
	fmt.Fprintln(w, strings.Repeat("─", 64))

	var okCount, totalRules int
	var totalSize int64
	for _, res := range results {
		mark := "✗"
		if res.OK {
			mark = "✓"
			okCount++
			totalRules += res.Rules
			totalSize += res.SRSSize
		}
		size := strings.Repeat(" ", 9)
		if res.SRSSize > 0 {
			size = fmt.Sprintf("%9s", humanSize(res.SRSSize))
		}
		line := fmt.Sprintf("  %s %-*s  %7s 条 %s", mark, width, res.Name, thousands(res.Rules), size)
		if notes := res.notes(); len(notes) > 0 {
			line += "   " + strings.Join(notes, "; ")
		}
		fmt.Fprintln(w, line)
		if res.Err != nil {
			for _, chunk := range strings.Split(res.Err.Error(), "\n") {
				fmt.Fprintf(w, "      %s\n", chunk)
			}
		}
		for _, src := range res.FailedSources {
			fmt.Fprintf(w, "      源不可用: %s\n", src)
		}
	}

	fmt.Fprintln(w, strings.Repeat("─", 64))
	tail := ""
	if totalSize > 0 {
		tail = "，" + humanSize(totalSize)
	}
	fmt.Fprintf(w, "  产出 %d/%d 个规则集，共 %s 条规则%s\n", okCount, len(results), thousands(totalRules), tail)

	var samples []string
	for _, res := range results {
		for _, s := range res.Diag.InvalidSamples {
			if len(samples) < 5 {
				samples = append(samples, s)
			}
		}
	}
	if len(samples) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "非法值样例:")
		for _, s := range samples {
			fmt.Fprintf(w, "  · %s\n", s)
		}
	}

	var bad []string
	for _, res := range results {
		if !res.OK {
			bad = append(bad, res.Name)
		}
	}
	if len(bad) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "失败 %d 个: %s\n", len(bad), strings.Join(bad, ", "))
	}
	fmt.Fprintln(w)
}

func (res *Result) notes() []string {
	var notes []string
	d := &res.Diag
	if len(d.Skipped) > 0 {
		notes = append(notes, "跳过 "+topPairs(d.Skipped, 4))
	}
	if len(d.Unknown) > 0 {
		notes = append(notes, "未知类型 "+topPairs(d.Unknown, 4))
	}
	if n := d.InvalidTotal(); n > 0 {
		notes = append(notes, fmt.Sprintf("非法值 %d", n))
	}
	if d.Subtracted > 0 {
		notes = append(notes, fmt.Sprintf("差集 -%d", d.Subtracted))
	}
	// 收敛单列：它会随源的可用性波动（贡献 domain_keyword 的源挂掉，被它压住的
	// 域名就全回来了），不单独记账的话"今天怎么多了三万条"会很难查。
	if d.Collapsed > 0 {
		notes = append(notes, fmt.Sprintf("收敛 -%d", d.Collapsed))
	}
	if d.Aggregated > 0 {
		notes = append(notes, fmt.Sprintf("CIDR 聚合 -%d", d.Aggregated))
	}
	if d.Dropped > 0 {
		notes = append(notes, fmt.Sprintf("按过滤规则丢弃 %d", d.Dropped))
	}
	if n := len(res.FailedSources); n > 0 {
		notes = append(notes, fmt.Sprintf("%d 个源不可用", n))
	}
	return notes
}

func topPairs(counts map[string]int, n int) string {
	top := ruleset.Top(counts, n)
	parts := make([]string, 0, len(top))
	for _, p := range top {
		parts = append(parts, fmt.Sprintf("%s×%d", p.Name, p.Count))
	}
	return strings.Join(parts, ", ")
}

func humanSize(size int64) string {
	value := float64(size)
	for _, unit := range []string{"B", "KB", "MB", "GB"} {
		if value < 1024 || unit == "GB" {
			if unit == "B" {
				return fmt.Sprintf("%.0f B", value)
			}
			return fmt.Sprintf("%.1f %s", value, unit)
		}
		value /= 1024
	}
	return ""
}

func thousands(n int) string {
	s := fmt.Sprint(n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

// ---------------- GitHub ----------------

// WriteGitHubSummary 写步骤摘要并发出注解。
//
// 取代 workflow 里那段 jq + shell：counts 从同一份数据算出来，不必再经过
// run-report.json 往返一次。
func (r *Run) WriteGitHubSummary(annotations io.Writer) error {
	counts := r.Counts()
	var failed []*Result
	byName := map[string]*Result{}
	for _, res := range r.Results {
		byName[res.Name] = res
	}
	for _, name := range r.Configured {
		if r.Status(name) == StatusFailed {
			if res := byName[name]; res != nil {
				failed = append(failed, res)
			} else {
				failed = append(failed, &Result{Name: name})
			}
		}
	}

	for _, res := range failed {
		reason := "源不可用"
		if res.Err != nil {
			reason = strings.ReplaceAll(res.Err.Error(), "\n", " ")
		}
		fmt.Fprintf(annotations, "::warning title=规则集未更新::%s: %s\n", res.Name, reason)
	}

	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "## 规则集 %d / %d\n\n", counts[StatusOK], len(r.Configured))
	if !r.Authoritative {
		fmt.Fprintf(file, "> 本次规则集名单不完整（bulk 通配符未能完全展开），发布时会跳过孤儿清理。\n\n")
	}
	if len(failed) == 0 {
		fmt.Fprintln(file, "全部规则集更新成功。")
		return nil
	}
	fmt.Fprintln(file, "本次未更新（将保留上一次发布的文件）：")
	fmt.Fprintln(file)
	for _, res := range failed {
		reason := "源不可用"
		if res.Err != nil {
			reason = strings.ReplaceAll(res.Err.Error(), "\n", " ")
		}
		fmt.Fprintf(file, "- `%s` — %s\n", res.Name, reason)
	}
	return nil
}
