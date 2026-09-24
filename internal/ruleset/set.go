package ruleset

import (
	"slices"
	"sort"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
)

// RuleSet 是一个具名规则集的规范化中间表示。
type RuleSet struct {
	Name string
	Diag Diagnostics

	strs  map[Field]map[string]struct{}
	ports map[Field]map[uint16]struct{}

	// verbatim 是无法拆成字段的规则（logical、带 invert）。绝不丢弃，
	// 也绝不把它们的值拆出来与别的规则混在一起去重 —— 那会改语义。
	verbatim []option.HeadlessRule
	seen     map[string]struct{}
}

// New 建一个空规则集。
func New(name string) *RuleSet {
	return &RuleSet{
		Name:  name,
		strs:  map[Field]map[string]struct{}{},
		ports: map[Field]map[uint16]struct{}{},
		seen:  map[string]struct{}{},
	}
}

// ---------------- 写入 ----------------

// Add 加入一条规则值。非法值返回 *InvalidValueError，由调用方决定记账还是中止。
func (s *RuleSet) Add(f Field, raw string) error {
	if !f.valid() {
		return invalid(f, raw, "不支持的字段")
	}
	if f.IsPort() {
		port, err := NormalizePort(raw)
		if err != nil {
			return invalid(f, raw, err.Error())
		}
		s.addPort(f, port)
		return nil
	}
	value, err := normalizeString(f, raw)
	if err != nil {
		return err
	}
	s.addStr(f, value)
	return nil
}

// AddLenient 加入一条规则值；非法则记进 Diag 并返回 false。
func (s *RuleSet) AddLenient(f Field, raw string) bool {
	err := s.Add(f, raw)
	if err == nil {
		return true
	}
	s.Diag.BadValue(f.String(), err.Error())
	return false
}

// AddPort 直接加入一个已经是数值的端口，跳过字符串解析。
func (s *RuleSet) AddPort(f Field, port uint16) {
	if f.IsPort() {
		s.addPort(f, port)
	}
}

// AddVerbatim 原样透传一条无法拆成字段的规则。
//
// 完全相同的规则只保留一条：多个源给出同一条逻辑规则是常事，plain 那边有集合
// 去重，这边没有就会重复输出。去重键用 JSON 序列化结果 —— 结构体字段顺序固定，
// 所以它是稳定的。
func (s *RuleSet) AddVerbatim(rule option.HeadlessRule) {
	key, err := marshalRule(rule)
	if err != nil {
		// 序列化不了的规则连产物都写不出去，当场记账而不是留到编译期。
		s.Diag.BadValue("verbatim", "规则无法序列化: "+err.Error())
		return
	}
	if _, dup := s.seen[string(key)]; dup {
		return
	}
	s.seen[string(key)] = struct{}{}
	s.verbatim = append(s.verbatim, rule)
}

func (s *RuleSet) addStr(f Field, value string) {
	m := s.strs[f]
	if m == nil {
		m = map[string]struct{}{}
		s.strs[f] = m
	}
	m[value] = struct{}{}
}

func (s *RuleSet) addPort(f Field, port uint16) {
	m := s.ports[f]
	if m == nil {
		m = map[uint16]struct{}{}
		s.ports[f] = m
	}
	m[port] = struct{}{}
}

// ---------------- 读出 ----------------

// Values 返回某个字符串字段排序后的值。Go 的 map 迭代顺序是随机的，
// 所有对外可见的顺序都必须在这里定死，否则产物无法逐字节复现。
func (s *RuleSet) Values(f Field) []string {
	m := s.strs[f]
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Ports 返回某个端口字段升序排列的值。
func (s *RuleSet) Ports(f Field) []uint16 {
	m := s.ports[f]
	if len(m) == 0 {
		return nil
	}
	out := make([]uint16, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

// Verbatim 返回透传规则。调用方不得修改。
func (s *RuleSet) Verbatim() []option.HeadlessRule { return s.verbatim }

// Total 是全部规则值加上透传规则的条数。
func (s *RuleSet) Total() int {
	n := len(s.verbatim)
	for _, m := range s.strs {
		n += len(m)
	}
	for _, m := range s.ports {
		n += len(m)
	}
	return n
}

// Empty 报告规则集是否一条规则都没有。
func (s *RuleSet) Empty() bool { return s.Total() == 0 }

// RuleObjects 是产物里会出现几条 rule 对象。
//
// 这个数必须是 1，规则集才是可合并的（见 Options）。不是 1 的原因只有两种：
// 有透传规则，或者用到了 rule 内 AND 项的字段 —— 两种都不是"做错了"，
// 但后果是引用方写 {"domain_suffix": [...], "rule_set": [...]} 时那一半失效，
// 而产物本身看不出来。所以它出现在摘要里。
//
// 不走 Options()：那会把几万个值排一遍序，只为数个数。两者的一致性由
// TestRuleObjectsMatchesOptions 守着。
func (s *RuleSet) RuleObjects() int {
	n := len(s.verbatim)
	dest := false
	for f := range fieldCount {
		if f.InDestinationGroup() {
			if len(s.strs[f]) > 0 {
				dest = true
			}
			continue
		}
		if len(s.strs[f]) > 0 || len(s.ports[f]) > 0 {
			n++
		}
	}
	if dest {
		n++
	}
	return n
}

// Options 是唯一的序列化出口。
//
// .json 和 .srs 都由它产出，两种产物因此不可能语义漂移 —— Python 版是先写
// JSON 再交给子进程编译，中间隔了一次文件往返。
//
// 目标地址组的字段（domain / domain_suffix / domain_keyword / domain_regex /
// ip_cidr）全部挤在**同一条** rule 里，这是产物形状唯一的硬要求。
//
// 因为 sing-box 遇到
//
//	{"domain_suffix": ["sagernet.org"], "rule_set": ["geosite-google"]}
//
// 会把规则集里的那条 rule 并进外层规则的匹配组，让 domain_suffix 与规则集成为
// OR；而这条合并只在规则集**恰好只有一条** rule 时生效（sing-box 1.14
// route/rule/rule_item_rule_set.go 的 mergeableRuleIn）。多出一条就退化成
// "domain_suffix 命中 **且** 规则集命中"，域名一个都匹配不上 —— 而产物本身
// 看不出任何异常。sing-geosite 的产物恒为一条，照着它来。
//
// 剩下的字段（network / port / process_name / package_name / source_*）是 rule
// 内的 AND 项，合进去就变成"必须同时命中"，那是改语义不是改形状，所以各自成条。
// 目前没有任何规则集用到它们，产物实际都是一条。
func (s *RuleSet) Options() option.PlainRuleSet {
	rules := make([]option.HeadlessRule, 0, int(fieldCount)+len(s.verbatim))

	var dest option.DefaultHeadlessRule
	destUsed := false
	for f := range fieldCount {
		if !f.InDestinationGroup() {
			continue
		}
		if values := s.Values(f); len(values) > 0 {
			setStrings(&dest, f, values)
			destUsed = true
		}
	}

	for f := range fieldCount {
		if f.InDestinationGroup() {
			// 整组一起出，位置取第一个有值的成员 —— 输出顺序仍由字段声明
			// 顺序决定。
			if destUsed {
				rules = append(rules, option.HeadlessRule{
					Type:           C.RuleTypeDefault,
					DefaultOptions: dest,
				})
				destUsed = false
			}
			continue
		}
		var def option.DefaultHeadlessRule
		if f.IsPort() {
			ports := s.Ports(f)
			if len(ports) == 0 {
				continue
			}
			setPorts(&def, f, ports)
		} else {
			values := s.Values(f)
			if len(values) == 0 {
				continue
			}
			setStrings(&def, f, values)
		}
		rules = append(rules, option.HeadlessRule{
			Type:           C.RuleTypeDefault,
			DefaultOptions: def,
		})
	}
	rules = append(rules, s.verbatim...)
	return option.PlainRuleSet{Rules: rules}
}

// setStrings / setPorts 把值装进 option 结构体对应的字段。
//
// 显式 switch 而不是反射：字段集是固定的十二个，反射换来的只是"编译期查不出
// 写错的字段名"。switch 漏掉一个 case 会在测试里立刻暴露（见 TestOptionsCoversAllFields）。
func setStrings(def *option.DefaultHeadlessRule, f Field, v []string) {
	list := badoption.Listable[string](v)
	switch f {
	case FieldNetwork:
		def.Network = list
	case FieldDomain:
		def.Domain = list
	case FieldDomainSuffix:
		def.DomainSuffix = list
	case FieldDomainKeyword:
		def.DomainKeyword = list
	case FieldDomainRegex:
		def.DomainRegex = list
	case FieldSourceIPCIDR:
		def.SourceIPCIDR = list
	case FieldIPCIDR:
		def.IPCIDR = list
	case FieldProcessName:
		def.ProcessName = list
	case FieldProcessPath:
		def.ProcessPath = list
	case FieldPackageName:
		def.PackageName = list
	}
}

func setPorts(def *option.DefaultHeadlessRule, f Field, v []uint16) {
	list := badoption.Listable[uint16](v)
	switch f {
	case FieldSourcePort:
		def.SourcePort = list
	case FieldPort:
		def.Port = list
	}
}

// sortedKeys 返回 map 的键，已排序。Go 的 map 迭代顺序随机，任何会影响产物
// 或删除决策的遍历都必须先定序 —— 否则收敛结果可能随运行而变。
func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SingleRule 把一个值归一之后装成一条只含该字段的 default 规则。
//
// 给解析层构造逻辑规则子项用。逻辑规则走 AddVerbatim，绕开了 Add 那条归一化
// 管道；把归一和装配绑在这一个函数里，调用方就没有"忘了 normalize"的机会 ——
// Python 版那里只有一句注释在提醒，而 DST-PORT,443 忘了归一会产出字符串端口，
// 让 sing-box 拒绝编译整个规则集。
func SingleRule(f Field, raw string) (option.HeadlessRule, error) {
	if !f.valid() {
		return option.HeadlessRule{}, invalid(f, raw, "不支持的字段")
	}
	var def option.DefaultHeadlessRule
	if f.IsPort() {
		port, err := NormalizePort(raw)
		if err != nil {
			return option.HeadlessRule{}, invalid(f, raw, err.Error())
		}
		setPorts(&def, f, []uint16{port})
	} else {
		value, err := normalizeString(f, raw)
		if err != nil {
			return option.HeadlessRule{}, err
		}
		setStrings(&def, f, []string{value})
	}
	return option.HeadlessRule{Type: C.RuleTypeDefault, DefaultOptions: def}, nil
}
