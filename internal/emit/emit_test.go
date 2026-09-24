package emit_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

// sample 覆盖全部五种归一策略 + 一条逻辑透传规则。
func sample(t *testing.T) *ruleset.RuleSet {
	t.Helper()
	s := ruleset.New("sample")
	add := func(f ruleset.Field, values ...string) {
		for _, v := range values {
			if !s.AddLenient(f, v) {
				t.Fatalf("AddLenient(%s, %q) 失败: %+v", f, v, s.Diag)
			}
		}
	}
	add(ruleset.FieldNetwork, "tcp")
	add(ruleset.FieldDomain, "a.example.com", "b.example.com")
	add(ruleset.FieldDomainSuffix, ".suffix.example", "bare.example")
	add(ruleset.FieldDomainKeyword, "keyword")
	add(ruleset.FieldDomainRegex, `^ad[0-9]{1,3}\.example\.com$`)
	add(ruleset.FieldIPCIDR, "1.2.3.0/24", "2001:db8::/32")
	add(ruleset.FieldSourceIPCIDR, "10.0.0.0/8")
	add(ruleset.FieldProcessName, "Telegram")
	add(ruleset.FieldProcessPath, "/usr/bin/Telegram")
	add(ruleset.FieldPackageName, "org.telegram.messenger")
	s.AddPort(ruleset.FieldPort, 443)
	s.AddPort(ruleset.FieldSourcePort, 1080)
	s.AddVerbatim(option.HeadlessRule{
		Type: C.RuleTypeLogical,
		LogicalOptions: option.LogicalHeadlessRule{
			Mode:   C.LogicalTypeAnd,
			Invert: true,
			Rules: []option.HeadlessRule{
				{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{
					Domain: badoption.Listable[string]{"nested.example.com"},
				}},
				{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{
					Port: badoption.Listable[uint16]{8443},
				}},
			},
		},
	})
	return s
}

// 第一阶段的出口条件：写出去的 .srs 能读回来，且语义相同。
//
// Python 版做不到这件事 —— 它对编译结果的全部认知就是子进程退出码为 0。
func TestSRSRoundTrip(t *testing.T) {
	for _, version := range []uint8{1, 2, 3, 4, emit.MaxSRSVersion} {
		set := sample(t)
		var buf bytes.Buffer
		if err := emit.SRS(&buf, set, version); err != nil {
			t.Fatalf("v%d: 写 .srs 失败: %v", version, err)
		}
		if buf.Len() == 0 {
			t.Fatalf("v%d: 产物为空", version)
		}

		// recover=true 才会把编译后的域名 matcher 反推回列表 —— domain 和
		// domain_suffix 在 .srs 里不是原样存的，是一个 succinct set。
		back, err := srs.Read(bytes.NewReader(buf.Bytes()), true)
		if err != nil {
			t.Fatalf("v%d: 读回失败: %v", version, err)
		}
		if back.Version != version {
			t.Errorf("v%d: 读回的版本是 %d", version, back.Version)
		}

		want := mustJSON(t, set.Options())
		got := mustJSON(t, option.PlainRuleSet{Rules: back.Options.Rules})
		if want != got {
			t.Errorf("v%d: 往返不一致\n写入: %s\n读回: %s", version, want, got)
		}
	}
}

func TestSRSRejectsBadVersion(t *testing.T) {
	set := sample(t)
	for _, version := range []uint8{0, emit.MaxSRSVersion + 1} {
		if err := emit.SRS(&bytes.Buffer{}, set, version); err == nil {
			t.Errorf("版本 %d 应当被拒绝", version)
		}
	}
}

// 同样的输入必须产出逐字节相同的文件，否则 diff 和幂等提交都做不了。
func TestOutputIsByteIdentical(t *testing.T) {
	render := func(kind string) []byte {
		var buf bytes.Buffer
		set := sample(t)
		var err error
		if kind == "json" {
			err = emit.JSON(&buf, set, 4)
		} else {
			err = emit.SRS(&buf, set, 4)
		}
		if err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	for _, kind := range []string{"json", "srs"} {
		first := render(kind)
		for i := 0; i < 10; i++ {
			if !bytes.Equal(first, render(kind)) {
				t.Fatalf("%s 产出不稳定", kind)
			}
		}
	}
}

// 单值字段必须仍然是数组。
//
// option 类型自带的序列化会把单元素列表塌成标量（{"domain":"a.com"}），
// 产物里标量和数组混着出现 —— 这正是 emit.JSON 不走那条路的原因。
func TestJSONAlwaysEmitsArrays(t *testing.T) {
	s := ruleset.New("one")
	s.AddLenient(ruleset.FieldDomain, "only.example.com")
	s.AddPort(ruleset.FieldPort, 443)

	var buf bytes.Buffer
	if err := emit.JSON(&buf, s, 4); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{`"domain": [`, `"port": [`} {
		if !strings.Contains(out, want) {
			t.Errorf("产物里缺 %q:\n%s", want, out)
		}
	}
	// 对照：库自带的序列化会塌成标量，确认这个坑是真的存在
	lib := mustJSON(t, s.Options())
	if !strings.Contains(lib, `"domain":"only.example.com"`) {
		t.Logf("注意：库的单值塌缩行为变了，emit.JSON 的理由需要复核；实际 %s", lib)
	}
}

// domain_regex 里 < > & 很常见。encoding/json 默认会把它们转成 \u003c，
// 产物虽然合法但没法读，也会让与旧版的对拍全线飘红。
func TestJSONDoesNotEscapeHTML(t *testing.T) {
	s := ruleset.New("re")
	s.AddLenient(ruleset.FieldDomainRegex, `^a<b>c&d$`)
	var buf bytes.Buffer
	if err := emit.JSON(&buf, s, 4); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), `\u003c`) {
		t.Errorf("HTML 转义没关掉:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), `^a<b>c&d$`) {
		t.Errorf("正则被改写了:\n%s", buf.String())
	}
}

// 产物必须能被 sing-box 自己读回去 —— 手写 JSON 编码器的唯一风险就在这里。
func TestJSONIsAcceptedBySingBox(t *testing.T) {
	set := sample(t)
	var buf bytes.Buffer
	if err := emit.JSON(&buf, set, 4); err != nil {
		t.Fatal(err)
	}
	var compat option.PlainRuleSetCompat
	if err := json.Unmarshal(buf.Bytes(), &compat); err != nil {
		t.Fatalf("sing-box 无法解析我们写的 JSON: %v\n%s", err, buf.String())
	}
	if compat.Version != 4 {
		t.Errorf("版本 = %d, want 4", compat.Version)
	}
	plain, err := compat.Upgrade()
	if err != nil {
		t.Fatalf("Upgrade 失败: %v", err)
	}
	if want, got := mustJSON(t, set.Options()), mustJSON(t, plain); want != got {
		t.Errorf("JSON 往返不一致\n写入: %s\n读回: %s", want, got)
	}
}

func TestFileWritesAreAtomic(t *testing.T) {
	dir := t.TempDir()
	set := sample(t)

	srsPath := filepath.Join(dir, "nested", "sample.srs")
	size, err := emit.SRSFile(srsPath, set, 4)
	if err != nil {
		t.Fatalf("SRSFile: %v", err)
	}
	assertFileSize(t, srsPath, size)

	jsonPath := filepath.Join(dir, "nested", "sample.json")
	size, err = emit.JSONFile(jsonPath, set, 4)
	if err != nil {
		t.Fatalf("JSONFile: %v", err)
	}
	assertFileSize(t, jsonPath, size)

	// 覆盖写：不能留下临时文件
	if _, err := emit.JSONFile(jsonPath, set, 4); err != nil {
		t.Fatalf("重写失败: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(jsonPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Errorf("留下了临时文件 %s", e.Name())
		}
	}
}

func assertFileSize(t *testing.T, path string, reported int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("产物不存在: %v", err)
	}
	if info.Size() != reported || info.Size() == 0 {
		t.Errorf("%s: 实际 %d 字节，报告 %d", path, info.Size(), reported)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
