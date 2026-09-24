package ruleset

import (
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
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
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

// Counts 按输出顺序返回各字段条数，供摘要使用。
func (s *RuleSet) Counts() []struct {
	Name  string
	Count int
} {
	var out []struct {
		Name  string
		Count int
	}
	for f := Field(0); f < fieldCount; f++ {
		n := len(s.strs[f]) + len(s.ports[f])
		if n > 0 {
			out = append(out, struct {
				Name  string
				Count int
			}{f.String(), n})
		}
	}
	if len(s.verbatim) > 0 {
		out = append(out, struct {
			Name  string
			Count int
		}{"verbatim", len(s.verbatim)})
	}
	return out
}

// Options 是唯一的序列化出口。
//
// .json 和 .srs 都由它产出，两种产物因此不可能语义漂移 —— Python 版是先写
// JSON 再交给子进程编译，中间隔了一次文件往返。
//
// 每个字段单独成一条 rule：同一条 rule 内的字段按 match group 组合（组间 AND），
// 拆开成多条则是 OR，这才是"这些值任意命中一个就算命中"的意思。
func (s *RuleSet) Options() option.PlainRuleSet {
	rules := make([]option.HeadlessRule, 0, int(fieldCount)+len(s.verbatim))
	for f := Field(0); f < fieldCount; f++ {
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
