// Package config 加载并校验 config.json。
//
// 没有 schema 版本号 —— 自用项目不需要版本协商。防手滑改由**拒绝未知键**承担：
// 忘了删的 sing_box、打错的 rulesets_version，都会当场报错并列出认识的键。
// 这比版本号更准，而且对拼写错误同样有效。
//
// rulesets 里只有一种条目形状：名字 → 一组输入 + 几个选项。没有 base / items /
// prefix，URL 写全。那组机制只省打字，代价是读配置时得先判断"这一条是哪种形状"。
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/source/parse"
)

// Error 是配置结构或取值有误。消息可直接展示给人。
type Error struct {
	Where  string
	Reason string
}

func (e *Error) Error() string {
	if e.Where == "" {
		return e.Reason
	}
	return e.Where + ": " + e.Reason
}

func errf(where, format string, args ...any) error {
	return &Error{Where: where, Reason: fmt.Sprintf(format, args...)}
}

// Config 是整份配置。
type Config struct {
	RulesetVersion uint8    `json:"ruleset_version"`
	Output         Output   `json:"output"`
	Fetch          Fetch    `json:"fetch"`
	Geosite        *Geosite `json:"geosite,omitempty"`

	// Rulesets 按名字排序，保证处理顺序和摘要顺序稳定。
	Rulesets []*Ruleset `json:"-"`
}

// file 是配置文件的字面结构。rulesets 在文件里是个对象（名字 → 规则集），
// 在 Config 里是排好序的切片 —— 两种形状分开，免得一个字段背两种含义。
type file struct {
	RulesetVersion uint8               `json:"ruleset_version"`
	Output         Output              `json:"output"`
	Fetch          Fetch               `json:"fetch"`
	Geosite        *Geosite            `json:"geosite,omitempty"`
	Rulesets       map[string]*Ruleset `json:"rulesets"`
}

// Output 描述产物：每种产物写到哪、发到哪。
//
// 键名就是扩展名，所以不需要 ext 字段；dir 只出现一次，不像拆成 output 和
// publish 两块时那样要写两遍。省略 branch = 只本地生成不发布；
// 整条省略 = 根本不产出这种产物。
type Output struct {
	SRS  *Artifact `json:"srs,omitempty"`
	JSON *Artifact `json:"json,omitempty"`
}

type Artifact struct {
	Dir    string `json:"dir"`
	Branch string `json:"branch,omitempty"`
}

// Fetch 是 HTTP 输入源的设置。
type Fetch struct {
	Concurrency int      `json:"concurrency,omitempty"`
	Timeout     Duration `json:"timeout,omitempty"`
	Retries     int      `json:"retries,omitempty"`
}

// Geosite 是 geosite 输入源的设置。
type Geosite struct {
	Repo      string `json:"repo,omitempty"`
	Normalize string `json:"normalize,omitempty"`
	Bulk      *Bulk  `json:"bulk,omitempty"`
	// File 指定本地 dlc.dat，非空时不走网络 —— 离线跑，或者用自己从
	// domain-list-community 源码树构建出来的那份。
	File string `json:"file,omitempty"`
}

// Bulk 是唯一的"一条配置生成多个规则集"的口子。
//
// 它待在这里而不是 rulesets 里，是因为 rulesets 的每一条都该是"描述一个规则集"；
// 而它生成的名字来自运行时的数据（1500 多个 code 手写不出来），性质不同。
type Bulk struct {
	Prefix  *string    `json:"prefix,omitempty"`
	Include stringList `json:"include,omitempty"`
	Exclude stringList `json:"exclude,omitempty"`
}

// Inputs 是一个规则集的输入来源。四个键的含义在 exclude 里完全一样。
type Inputs struct {
	Sources stringList `json:"sources,omitempty"`
	Files   stringList `json:"files,omitempty"`
	Geosite stringList `json:"geosite,omitempty"`
	Inline  stringList `json:"inline,omitempty"`
}

func (i Inputs) empty() bool {
	return len(i.Sources) == 0 && len(i.Files) == 0 && len(i.Geosite) == 0 && len(i.Inline) == 0
}

// Ruleset 是一个具名规则集。
type Ruleset struct {
	Name string `json:"-"`

	Inputs
	Exclude *Inputs `json:"exclude,omitempty"`

	Format    string `json:"format,omitempty"`
	Aggregate bool   `json:"aggregate,omitempty"`
	// Collapse 省略即开启。用指针区分"没写"和"显式写了 false"。
	Collapse *bool `json:"collapse,omitempty"`

	parsedFormat parse.Format
}

// ParsedFormat 返回已校验的格式。
func (r *Ruleset) ParsedFormat() parse.Format { return r.parsedFormat }

// CollapseEnabled 报告是否做等价收敛。省略即开启。
func (r *Ruleset) CollapseEnabled() bool { return r.Collapse == nil || *r.Collapse }

// UnmarshalJSON 支持简写：裸字符串或裸数组等于 sources。这是唯一的简写。
func (r *Ruleset) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && (trimmed[0] == '"' || trimmed[0] == '[') {
		var list stringList
		if err := json.Unmarshal(data, &list); err != nil {
			return err
		}
		r.Sources = list
		return nil
	}
	type plain Ruleset // 防止递归调用本方法
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode((*plain)(r))
}

// stringList 接受单个字符串或字符串数组。
//
// sources / files / geosite / inline 四个键同构，都用它 —— "一个还是一组"
// 这种事不该让人记住写法差异。
type stringList []string

func (l *stringList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var one string
		if err := json.Unmarshal(data, &one); err != nil {
			return err
		}
		*l = stringList{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*l = many
	return nil
}

// Duration 接受 Go duration 字符串（"30s"），也接受裸数字（按秒）。
type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return fmt.Errorf("不是合法的时长 %q（例如 \"30s\"、\"1m30s\"）", text)
		}
		*d = Duration(parsed)
		return nil
	}
	var seconds float64
	if err := json.Unmarshal(data, &seconds); err != nil {
		return fmt.Errorf("应为时长字符串或秒数")
	}
	*d = Duration(time.Duration(seconds * float64(time.Second)))
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

// nameRe 限制规则集名 —— 它会直接变成输出文件名。
//
// 允许 ! 和 @ 是必须的：geosite 的 bulk 会用 code 名生成规则集名，
// 而 v2fly 的 code 里这两个字符很常见（geolocation-!cn、google@ads）。
// 两者在 Windows 和 POSIX 文件系统上都合法，在 URL 路径段里也不用转义；
// 官方 sing-geosite 发布的文件就叫 geosite-geolocation-!cn.srs。
var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._!@-]*$`)

// ValidName 报告一个名字能不能当输出文件名。供 bulk 在运行时校验生成的名字。
func ValidName(name string) bool { return nameRe.MatchString(name) }

// 默认值。写在一处，校验里不再散落魔数。
const (
	defaultConcurrency = 16
	defaultTimeout     = 30 * time.Second
	defaultRetries     = 3
)

// Load 读取并校验配置。任何问题都返回 *Error，消息可直接展示。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errf("", "配置文件不存在: %s", path)
		}
		return nil, errf("", "读配置失败: %v", err)
	}
	return Parse(data)
}

// Parse 校验一份配置内容。
func Parse(data []byte) (*Config, error) {
	if err := checkDuplicateKeys(data); err != nil {
		return nil, err
	}

	var raw file
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, decodeError(err)
	}
	if dec.More() {
		return nil, errf("", "配置文件末尾有多余内容")
	}

	cfg := Config{
		RulesetVersion: raw.RulesetVersion,
		Output:         raw.Output,
		Fetch:          raw.Fetch,
		Geosite:        raw.Geosite,
	}
	if len(raw.Rulesets) == 0 {
		return nil, errf("rulesets", "不能为空")
	}
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(raw.Rulesets))
	for name := range raw.Rulesets {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		spec := raw.Rulesets[name]
		if spec == nil {
			return nil, errf("rulesets."+name, "内容为空")
		}
		spec.Name = name
		if err := validateRuleset(spec); err != nil {
			return nil, err
		}
		cfg.Rulesets = append(cfg.Rulesets, spec)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() error {
	if c.RulesetVersion == 0 {
		return errf("", "缺少 ruleset_version")
	}
	if c.RulesetVersion > emit.MaxSRSVersion {
		return errf("ruleset_version", "必须在 1-%d 之间，实际 %d。"+
			"这是本版 sing-box 认识的全部 rule-set 版本；上限跟着依赖走，不用手工同步",
			emit.MaxSRSVersion, c.RulesetVersion)
	}
	if c.Output.SRS == nil && c.Output.JSON == nil {
		return errf("output", "至少要有 srs 或 json 一种产物")
	}
	for name, a := range map[string]*Artifact{"srs": c.Output.SRS, "json": c.Output.JSON} {
		if a == nil {
			continue
		}
		if strings.TrimSpace(a.Dir) == "" {
			return errf("output."+name+".dir", "不能为空")
		}
	}

	if c.Fetch.Concurrency == 0 {
		c.Fetch.Concurrency = defaultConcurrency
	}
	if c.Fetch.Concurrency < 1 {
		return errf("fetch.concurrency", "必须 >= 1，实际 %d", c.Fetch.Concurrency)
	}
	if c.Fetch.Timeout == 0 {
		c.Fetch.Timeout = Duration(defaultTimeout)
	}
	if c.Fetch.Timeout <= 0 {
		return errf("fetch.timeout", "必须 > 0")
	}
	if c.Fetch.Retries == 0 {
		c.Fetch.Retries = defaultRetries
	}
	if c.Fetch.Retries < 0 {
		return errf("fetch.retries", "不能为负")
	}

	if c.Geosite != nil {
		// 不提供 passthrough：实测 1898 个 code、73321 条值，归一化拒绝 0 条 ——
		// 一个绕开归一管道的口子会破坏"进了集合的值必定已归一"这条契约，
		// 而收敛和去重都依赖它。换来的好处是零。
		switch c.Geosite.Normalize {
		case "", "lenient", "strict":
		default:
			return errf("geosite.normalize", "取值非法 %q，可选 lenient / strict", c.Geosite.Normalize)
		}
		if b := c.Geosite.Bulk; b != nil && len(b.Include) == 0 {
			return errf("geosite.bulk.include", "不能为空 —— 想要全量就显式写 [\"*\"]。"+
				"默认全量意味着一次手滑就往发布分支推上千个文件")
		}
	}
	return nil
}

func validateRuleset(r *Ruleset) error {
	where := "rulesets." + r.Name
	if !nameRe.MatchString(r.Name) {
		return errf(where, "规则集名非法。名字会直接作为输出文件名，"+
			"只允许字母、数字、点、下划线和连字符，且不能以点或连字符开头")
	}
	if r.Inputs.empty() {
		return errf(where, "一个输入都没有 —— 至少要有 sources / files / geosite / inline 之一")
	}

	format, err := parse.ParseFormat(r.Format)
	if err != nil {
		return errf(where+".format", "%v", err)
	}
	r.parsedFormat = format

	if err := validateInputs(r.Inputs, where); err != nil {
		return err
	}
	if r.Exclude != nil {
		if r.Exclude.empty() {
			return errf(where+".exclude", "写了但一个输入都没有")
		}
		if err := validateInputs(*r.Exclude, where+".exclude"); err != nil {
			return err
		}
	}
	return nil
}

func validateInputs(in Inputs, where string) error {
	for i, raw := range in.Sources {
		if err := checkURL(raw, fmt.Sprintf("%s.sources[%d]", where, i)); err != nil {
			return err
		}
	}
	for i, raw := range in.Files {
		if err := checkFile(raw, fmt.Sprintf("%s.files[%d]", where, i)); err != nil {
			return err
		}
	}
	for i, raw := range in.Geosite {
		if strings.TrimSpace(raw) == "" {
			return errf(fmt.Sprintf("%s.geosite[%d]", where, i), "geosite code 不能为空")
		}
	}
	for i, raw := range in.Inline {
		if strings.TrimSpace(raw) == "" {
			return errf(fmt.Sprintf("%s.inline[%d]", where, i), "内联规则不能为空")
		}
	}
	return nil
}

func checkURL(raw, where string) error {
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		return errf(where, "URL 协议必须是 http 或 https: %q", raw)
	}
	rest := raw[strings.Index(raw, "//")+2:]
	host, _, _ := strings.Cut(rest, "/")
	if host == "" {
		return errf(where, "URL 缺少主机名: %q", raw)
	}
	return nil
}

// checkFile 限制本地文件只能在工作目录内。
//
// 配置是可以从别处拿来的（改一行 URL 就能用），所以不能让它读到工作目录之外 ——
// files: ["../../.ssh/id_rsa"] 不该是一个能跑通的写法。
func checkFile(raw, where string) error {
	if raw == "" {
		return errf(where, "路径不能为空")
	}
	if filepath.IsAbs(raw) || strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, `\`) {
		return errf(where, "只能用工作目录内的相对路径: %q", raw)
	}
	clean := filepath.ToSlash(filepath.Clean(raw))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return errf(where, "路径不能跳出工作目录: %q", raw)
	}
	return nil
}

// checkDuplicateKeys 扫一遍 token 流找同层重复键。
//
// encoding/json 遇到重复键会静默取最后一个 —— 一份 56 条的手写配置里复制粘贴
// 出两个同名规则集，结果是安静地少一个产出，摘要上完全看不出来。这正是上一次
// 重构要消灭的那类问题（旧配置三段共用同一套输出文件名却互不知情）。
func checkDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var walk func(path string) error
	walk = func(path string) error {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case json.Delim:
			switch t {
			case '{':
				seen := map[string]bool{}
				for dec.More() {
					keyTok, err := dec.Token()
					if err != nil {
						return err
					}
					key, ok := keyTok.(string)
					if !ok {
						return fmt.Errorf("对象键不是字符串")
					}
					if seen[key] {
						return errf(strings.TrimPrefix(path+"."+key, "."),
							"重复的键 —— encoding/json 会静默取最后一个，安静少掉一条")
					}
					seen[key] = true
					if err := walk(path + "." + key); err != nil {
						return err
					}
				}
				_, err := dec.Token() // 吃掉 }
				return err
			case '[':
				i := 0
				for dec.More() {
					if err := walk(fmt.Sprintf("%s[%d]", path, i)); err != nil {
						return err
					}
					i++
				}
				_, err := dec.Token() // 吃掉 ]
				return err
			}
		}
		return nil
	}
	if err := walk(""); err != nil {
		if _, ok := err.(*Error); ok {
			return err
		}
		if err == io.EOF {
			return errf("", "配置文件是空的")
		}
		return errf("", "配置文件不是合法 JSON: %v", err)
	}
	return nil
}

// decodeError 把 encoding/json 的报错翻译成能照着改的话。
func decodeError(err error) error {
	msg := err.Error()
	if field, ok := strings.CutPrefix(msg, "json: unknown field "); ok {
		return errf("", "不认识的配置键 %s。\n  顶层认识: ruleset_version、output、fetch、geosite、rulesets\n"+
			"  规则集里认识: sources、files、geosite、inline、exclude、format、aggregate、collapse\n"+
			"  （schema 与 sing_box 已经不需要了，删掉即可）", field)
	}
	if typeErr, ok := err.(*json.UnmarshalTypeError); ok {
		where := typeErr.Field
		if where == "" {
			where = "配置"
		}
		return errf(where, "类型不对：期望 %s，实际是 %s", typeErr.Type, typeErr.Value)
	}
	return errf("", "配置文件不是合法 JSON: %v", err)
}
