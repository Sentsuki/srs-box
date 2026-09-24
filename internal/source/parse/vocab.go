package parse

import "github.com/Sentsuki/srs-box/internal/ruleset"

// 规则类型词汇表 —— 纯数据，不含任何逻辑。
//
// 查表前一律转大写，所以 DOMAIN-SUFFIX / domain-suffix / Host-Suffix 自动命中
// 同一条，不必为每种大小写写一行。
//
// Clash / Surge / Quantumult X 的规则行丢掉策略列之后是同一种东西，差异只体现
// 在这张表的词汇上 —— 所以这个项目里不存在"方言"这个概念，支持新方言 = 加行。

// types 把规则类型映射到 sing-box headless rule 字段。
var types = map[string]ruleset.Field{
	"DOMAIN":         ruleset.FieldDomain,
	"HOST":           ruleset.FieldDomain,
	"DOMAIN-SUFFIX":  ruleset.FieldDomainSuffix,
	"HOST-SUFFIX":    ruleset.FieldDomainSuffix,
	"DOMAIN-KEYWORD": ruleset.FieldDomainKeyword,
	"HOST-KEYWORD":   ruleset.FieldDomainKeyword,
	"DOMAIN-REGEX":   ruleset.FieldDomainRegex,
	"HOST-REGEX":     ruleset.FieldDomainRegex,
	"IP-CIDR":        ruleset.FieldIPCIDR,
	"IP-CIDR6":       ruleset.FieldIPCIDR,
	"IP6-CIDR":       ruleset.FieldIPCIDR,
	"SRC-IP-CIDR":    ruleset.FieldSourceIPCIDR,
	"SOURCE-IP-CIDR": ruleset.FieldSourceIPCIDR,
	"DST-PORT":       ruleset.FieldPort,
	"PORT":           ruleset.FieldPort,
	"SRC-PORT":       ruleset.FieldSourcePort,
	"SOURCE-PORT":    ruleset.FieldSourcePort,
	"PROCESS-NAME":   ruleset.FieldProcessName,
	"PROCESS-PATH":   ruleset.FieldProcessPath,
	"PACKAGE-NAME":   ruleset.FieldPackageName,
	"NETWORK":        ruleset.FieldNetwork,
}

// skip 是认识、但 headless rule 表达不了的类型 —— 跳过并计数，绝不静默丢弃。
//
// GEOIP 和 URL-REGEX 特别值得留意：旧实现把它们分别映射成了根本不存在的 geoip
// 字段和语义不符的 domain_regex，会产出 sing-box 拒绝、或者永不命中的规则。
var skip = map[string]struct{}{
	"GEOIP":              {},
	"GEOSITE":            {},
	"IP-ASN":             {},
	"SRC-IP-ASN":         {},
	"IP-SUFFIX":          {},
	"SRC-IP-SUFFIX":      {},
	"URL-REGEX":          {},
	"USER-AGENT":         {},
	"HEADER":             {},
	"SCRIPT":             {},
	"RULE-SET":           {},
	"SUB-RULE":           {},
	"MATCH":              {},
	"FINAL":              {},
	"DSCP":               {},
	"UID":                {},
	"IN-TYPE":            {},
	"IN-USER":            {},
	"IN-NAME":            {},
	"IN-PORT":            {},
	"PROCESS-PATH-REGEX": {},
	"PROCESS-NAME-REGEX": {},
	"AND-SET":            {},
}

// logicalMode 是逻辑关键字 → (sing-box mode, 是否取反)。
//
// sing-box 的 logical rule 只有 and / or 两种 mode，NOT 通过 invert 表达。
var logicalMode = map[string]struct {
	mode   string
	invert bool
}{
	"AND": {"and", false},
	"OR":  {"or", false},
	"NOT": {"and", true},
}

// containerKeys 是结构化容器里常见的"装规则的那个键"。
//
// 一份 Clash YAML 走行提取器时，payload: 这行会变成一个条目；不认出来的话它会
// 被当成域名、判非法、记进诊断 —— 每个 YAML 源都白白多一条噪声。
var containerKeys = map[string]struct{}{
	"payload": {},
	"rules":   {},
	"domain":  {},
	"domains": {},
	"host":    {},
	"hosts":   {},
	"ip":      {},
	"version": {},
}
