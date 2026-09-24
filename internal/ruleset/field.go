// Package ruleset 是整个项目唯一的规则容器。
//
// 设计契约（继承自 Python 版 models.py，不可放松）：
//
//   - 规则值只能经 Add / AddLenient / AddVerbatim 进入，只能经 Options 离开。
//   - Add 负责字段白名单与取值归一，所以进了 strs/ports 的值不可能是 sing-box
//     不认识的字段、错误类型或非法 CIDR。
//   - AddVerbatim 走 option.HeadlessRule。类型系统在这里替代了 Python 版
//     "调用方必须自己记得 normalize" 那条只存在于注释里的约定。
package ruleset

// Field 是 sing-box headless rule 里我们会拆开合并的字段。
//
// 声明顺序**就是**输出顺序：同样的输入必须产出逐字节相同的文件，否则 diff
// 和幂等提交都没法做。所以不另设一张顺序表 —— 少一份可以写错的东西。
//
// 不含 query_type：没有任何解析路径能产出它，而留在这里是有害的 ——
// 归一会把 sing-box 允许的整数 query_type 压成字符串。带 query_type 的
// 上游规则走 AddVerbatim 原样透传，反而是对的。
type Field uint8

const (
	FieldNetwork Field = iota
	FieldDomain
	FieldDomainSuffix
	FieldDomainKeyword
	FieldDomainRegex
	FieldSourceIPCIDR
	FieldIPCIDR
	FieldSourcePort
	FieldPort
	FieldProcessName
	FieldProcessPath
	FieldPackageName

	fieldCount
)

// kind 决定归一策略。
type kind uint8

const (
	// kindLower 普通字符串，小写化。
	kindLower kind = iota
	// kindExact 大小写敏感，原样保留。
	//
	// 域名可以安全地小写（DNS 本就大小写不敏感），但进程名和路径在
	// Linux/macOS 上是大小写敏感的：把 Telegram 压成 telegram 会让规则永不命中。
	kindExact
	// kindHost 域名：小写 + punycode + 校验，保留前导点。
	kindHost
	// kindCIDR IP 或 CIDR，主机位归整。
	kindCIDR
	// kindPort 0-65535。
	kindPort
)

var fields = [fieldCount]struct {
	name string
	kind kind
}{
	FieldNetwork:       {"network", kindLower},
	FieldDomain:        {"domain", kindHost},
	FieldDomainSuffix:  {"domain_suffix", kindHost},
	FieldDomainKeyword: {"domain_keyword", kindLower},
	FieldDomainRegex:   {"domain_regex", kindExact},
	FieldSourceIPCIDR:  {"source_ip_cidr", kindCIDR},
	FieldIPCIDR:        {"ip_cidr", kindCIDR},
	FieldSourcePort:    {"source_port", kindPort},
	FieldPort:          {"port", kindPort},
	FieldProcessName:   {"process_name", kindExact},
	FieldProcessPath:   {"process_path", kindExact},
	FieldPackageName:   {"package_name", kindExact},
}

var byName = func() map[string]Field {
	m := make(map[string]Field, fieldCount)
	for f := Field(0); f < fieldCount; f++ {
		m[fields[f].name] = f
	}
	return m
}()

// String 返回 sing-box JSON 里的字段名。
func (f Field) String() string {
	if f >= fieldCount {
		return "invalid"
	}
	return fields[f].name
}

func (f Field) valid() bool { return f < fieldCount }

// IsPort 报告该字段的值是端口（uint16）而非字符串。
func (f Field) IsPort() bool { return f.valid() && fields[f].kind == kindPort }

// Lookup 按 sing-box 字段名查 Field。
func Lookup(name string) (Field, bool) {
	f, ok := byName[name]
	return f, ok
}

// AllFields 按输出顺序返回全部字段。
func AllFields() []Field {
	out := make([]Field, 0, fieldCount)
	for f := Field(0); f < fieldCount; f++ {
		out = append(out, f)
	}
	return out
}
