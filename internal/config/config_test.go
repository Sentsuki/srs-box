package config

import (
	"strings"
	"testing"
	"time"
)

const minimal = `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`

func load(t *testing.T, body string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(body))
	if err != nil {
		t.Fatalf("加载失败: %v", err)
	}
	return cfg
}

func mustFail(t *testing.T, body, wantSubstr string) {
	t.Helper()
	_, err := Parse([]byte(body))
	if err == nil {
		t.Fatalf("应当报错，但通过了")
	}
	if wantSubstr != "" && !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("报错信息里应当含 %q，实际: %v", wantSubstr, err)
	}
}

// 验证生产配置 config.json 能正常加载且满足基本不变量。
func TestRealProductionConfig(t *testing.T) {
	cfg, err := Load("../../config.json")
	if err != nil {
		t.Fatalf("加载 config.json 失败: %v", err)
	}
	if len(cfg.Rulesets) == 0 {
		t.Fatal("config.json 规则集不能为空")
	}
	if cfg.Output.SRS == nil || cfg.Output.SRS.Branch != "srs_release" {
		t.Errorf("output.srs = %+v", cfg.Output.SRS)
	}
	if cfg.Output.JSON == nil || cfg.Output.JSON.Branch != "json_release" {
		t.Errorf("output.json = %+v", cfg.Output.JSON)
	}
	if cfg.Fetch.Timeout.Std() != 30*time.Second {
		t.Errorf("timeout = %s", cfg.Fetch.Timeout.Std())
	}
	// 排序必须稳定 —— 摘要顺序和处理顺序都依赖它
	for i := 1; i < len(cfg.Rulesets); i++ {
		if cfg.Rulesets[i-1].Name >= cfg.Rulesets[i].Name {
			t.Fatalf("规则集没按名字排序: %q 在 %q 之前",
				cfg.Rulesets[i-1].Name, cfg.Rulesets[i].Name)
		}
	}
}

// 验证文档示例 doc/config.example.json 语法正确且能正常解析，防止文档过期失修。
func TestExampleDocConfig(t *testing.T) {
	cfg, err := Load("../../doc/config.example.json")
	if err != nil {
		t.Fatalf("加载 doc/config.example.json 失败: %v", err)
	}
	if len(cfg.Rulesets) == 0 {
		t.Fatal("doc/config.example.json 规则集不能为空")
	}
}

// ---------------- 拒绝未知键（替代 schema 版本检查）----------------

func TestRejectsLeftoverSingBoxSection(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "sing_box": { "version": "1.14.0", "platform": "linux-amd64" },
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`
	mustFail(t, body, "sing_box")
}

func TestRejectsLeftoverSchemaField(t *testing.T) {
	body := `{
  "schema": 1,
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`
	mustFail(t, body, "schema")
}

// 拼错的键是"拒绝未知键"真正的价值所在 —— 版本号挡不住这个。
func TestRejectsTypo(t *testing.T) {
	body := `{
  "rulesets_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`
	mustFail(t, body, "rulesets_version")
}

func TestRejectsUnknownRulesetKey(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "source": ["https://example.com/a.json"] } }
}`
	mustFail(t, body, "source")
}

// base / items / prefix 这套已经删掉了，旧配置照搬过来要能得到明确提示。
func TestRejectsOldBaseItems(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "skk": { "base": "https://ruleset.skk.moe/", "items": { "ai": "ai.json" } } }
}`
	mustFail(t, body, "base")
}

// encoding/json 遇到重复键会静默取最后一个，安静少掉一条规则集。
func TestRejectsDuplicateKeys(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": {
    "a": ["https://example.com/a.json"],
    "a": ["https://example.com/b.json"]
  }
}`
	mustFail(t, body, "重复")
}

// ---------------- 简写与同构 ----------------

func TestShorthandForms(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": {
    "bare-string": "https://example.com/a.json",
    "bare-array": ["https://example.com/a.json", "https://example.com/b.json"],
    "object": { "sources": "https://example.com/c.json" },
    "scalar-geosite": { "geosite": "geolocation-!cn" }
  }
}`
	cfg := load(t, body)
	byName := map[string]*Ruleset{}
	for _, r := range cfg.Rulesets {
		byName[r.Name] = r
	}
	if got := byName["bare-string"].Sources; len(got) != 1 {
		t.Errorf("裸字符串简写 = %v", got)
	}
	if got := byName["bare-array"].Sources; len(got) != 2 {
		t.Errorf("裸数组简写 = %v", got)
	}
	if got := byName["object"].Sources; len(got) != 1 {
		t.Errorf("对象里的标量 sources = %v", got)
	}
	if got := byName["scalar-geosite"].Geosite; len(got) != 1 || got[0] != "geolocation-!cn" {
		t.Errorf("标量 geosite = %v", got)
	}
}

func TestMixedInputsAndExclude(t *testing.T) {
	body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": {
    "ai": {
      "files": ["data/claude.json"],
      "sources": ["https://example.com/ai.json"],
      "geosite": ["category-ai-chat-!cn"],
      "inline": ["DOMAIN-SUFFIX,anthropic.com"],
      "exclude": { "inline": ["DOMAIN,skip.example"] },
      "collapse": false,
      "aggregate": true
    }
  }
}`
	cfg := load(t, body)
	r := cfg.Rulesets[0]
	if len(r.Sources) != 1 || len(r.Files) != 1 || len(r.Geosite) != 1 || len(r.Inline) != 1 {
		t.Errorf("四种输入没都读到: %+v", r.Inputs)
	}
	if r.Exclude == nil || len(r.Exclude.Inline) != 1 {
		t.Errorf("exclude = %+v", r.Exclude)
	}
	if r.CollapseEnabled() {
		t.Error("显式写了 collapse:false，应当关闭")
	}
	if !r.Aggregate {
		t.Error("aggregate 没读到")
	}
}

// collapse 省略即开启 —— 用指针区分"没写"和"写了 false"。
func TestCollapseDefaultsOn(t *testing.T) {
	cfg := load(t, minimal)
	if !cfg.Rulesets[0].CollapseEnabled() {
		t.Error("collapse 省略时应当开启")
	}
}

func TestDurationForms(t *testing.T) {
	for _, pair := range []struct {
		raw  string
		want time.Duration
	}{
		{`"30s"`, 30 * time.Second},
		{`"1m30s"`, 90 * time.Second},
		{`30`, 30 * time.Second}, // 裸数字按秒，平滑迁移
	} {
		body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "fetch": { "timeout": ` + pair.raw + ` },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`
		cfg := load(t, body)
		if got := cfg.Fetch.Timeout.Std(); got != pair.want {
			t.Errorf("timeout %s → %s, want %s", pair.raw, got, pair.want)
		}
	}
	mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "fetch": { "timeout": "soon" },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`, "时长")
}

func TestDefaults(t *testing.T) {
	cfg := load(t, minimal)
	if cfg.Fetch.Concurrency != defaultConcurrency {
		t.Errorf("concurrency = %d", cfg.Fetch.Concurrency)
	}
	if cfg.Fetch.Timeout.Std() != defaultTimeout {
		t.Errorf("timeout = %s", cfg.Fetch.Timeout.Std())
	}
	if cfg.Fetch.Retries != defaultRetries {
		t.Errorf("retries = %d", cfg.Fetch.Retries)
	}
	// 省略 json 就是不产出 json
	if cfg.Output.JSON != nil {
		t.Error("没写 json 却产出了 json")
	}
}

// ---------------- 取值校验 ----------------

func TestRulesetVersionCeilingFollowsLibrary(t *testing.T) {
	mustFail(t, strings.Replace(minimal, `"ruleset_version": 4`, `"ruleset_version": 99`, 1), "ruleset_version")
	// 上限来自链接进来的 sing-box，不是硬编码的 4
	ok := strings.Replace(minimal, `"ruleset_version": 4`, `"ruleset_version": 5`, 1)
	load(t, ok)
}

func TestRejectsBadNames(t *testing.T) {
	for _, name := range []string{".hidden", "-leading", "with space", "with/slash", ""} {
		body := `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "` + name + `": ["https://example.com/a.json"] }
}`
		mustFail(t, body, "")
	}
}

func TestRejectsEmptyInputs(t *testing.T) {
	mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "aggregate": true } }
}`, "一个输入都没有")
}

func TestRejectsBadURL(t *testing.T) {
	for _, url := range []string{"ftp://example.com/a.json", "example.com/a.json", "https://"} {
		mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": ["`+url+`"] }
}`, "")
	}
}

// 配置是可以从别处拿来改一行就用的，不能让它读到工作目录之外。
func TestRejectsFileEscapingWorkdir(t *testing.T) {
	for _, path := range []string{"../../.ssh/id_rsa", "/etc/passwd", "..", "data/../../secret"} {
		mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "files": ["`+path+`"] } }
}`, "")
	}
	// 目录内的相对路径要能用，包括中间有 .. 但最终仍在目录内的
	load(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "files": ["data/x.json", "data/sub/../y.json"] } }
}`)
}

func TestRejectsOldFormatValues(t *testing.T) {
	for _, f := range []string{"singbox", "yaml", "text", "srs"} {
		mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "sources": ["https://example.com/a.json"], "format": "`+f+`" } }
}`, "format")
	}
	// 只剩这三个还需要写
	for _, f := range []string{"adguard", "cidr", "domainset"} {
		load(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": { "a": { "sources": ["https://example.com/a.json"], "format": "`+f+`" } }
}`)
	}
}

func TestRejectsNoOutput(t *testing.T) {
	mustFail(t, `{
  "ruleset_version": 4,
  "output": {},
  "rulesets": { "a": ["https://example.com/a.json"] }
}`, "至少要有")
}

func TestRejectsEmptyRulesets(t *testing.T) {
	mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "rulesets": {}
}`, "rulesets")
}

// bulk 不可省略 include：默认全量意味着一次手滑就推上千个文件。
func TestBulkRequiresInclude(t *testing.T) {
	mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "geosite": { "bulk": {} },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`, "include")

	load(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "geosite": { "bulk": { "include": ["*"], "exclude": ["*@*"] } },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`)
}

func TestRejectsBadNormalizeMode(t *testing.T) {
	mustFail(t, `{
  "ruleset_version": 4,
  "output": { "srs": { "dir": "output/srs" } },
  "geosite": { "normalize": "whatever" },
  "rulesets": { "a": ["https://example.com/a.json"] }
}`, "normalize")
}

func TestRejectsMalformedJSON(t *testing.T) {
	mustFail(t, `{"ruleset_version": 4,`, "")
	mustFail(t, ``, "")
	mustFail(t, `[]`, "")
}

// geosite.file 和 rulesets.*.files 走同一条边界 —— 配置可能是从别处拿来的，
// 留一个不受限的读文件口子就等于这条边界不存在。
func TestGeositeFileStaysInWorkdir(t *testing.T) {
	body := func(path string) string {
		return `{"ruleset_version": 4, "output": {"srs": {"dir": "out"}},
			"geosite": {"file": "` + path + `"},
			"rulesets": {"a": {"geosite": ["cn"]}}}`
	}
	for _, bad := range []string{"../../etc/passwd", "/etc/passwd", `C:\\Windows\\win.ini`} {
		mustFail(t, body(bad), "geosite.file")
	}
	cfg := load(t, body("data/dlc.dat"))
	if cfg.Geosite.File != "data/dlc.dat" {
		t.Errorf("工作目录内的相对路径应当原样保留，实际 %q", cfg.Geosite.File)
	}
}
