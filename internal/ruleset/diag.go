package ruleset

import "sort"

// MaxInvalidSamples 是每个规则集保留的非法值样例上限。
const MaxInvalidSamples = 5

// Diagnostics 是解析与变换过程里积累的"非致命异常"，最终出现在摘要里。
//
// Python 版之前把这些打成 INFO 日志，而配置里的日志级别会把它们整体过滤掉 ——
// 等于永远看不见。所以这里是结构化计数，由摘要直接输出，不经过 logger。
type Diagnostics struct {
	// Skipped 认识但 headless rule 表达不了的规则类型（GEOIP、URL-REGEX…）。
	Skipped map[string]int
	// Unknown 完全不认识的规则类型。
	Unknown map[string]int
	// Invalid 按字段计数的非法值。
	Invalid        map[string]int
	InvalidSamples []string

	// Dropped 被水印过滤删掉的条数。
	Dropped int
	// Collapsed 被等价收敛删掉的条数。单列出来是必要的：收敛会随源的可用性
	// 波动（贡献 domain_keyword 的源挂掉，被它压住的域名就全回来了），
	// 不单独记账的话"今天怎么多了三万条"会很难查。
	Collapsed int
	// Subtracted 被 exclude 差集整条删掉的条数。
	Subtracted int
	// Narrowed 被 exclude 改窄、但仍留在产物里的条数：无点 suffix 去掉 apex
	// 变成带点 suffix，或者一段 CIDR 被裁掉一块。
	Narrowed int
	// Unexpressible exclude 与产物部分重叠、而 headless rule 写不出差集的条数
	// （suffix:a.com 减 domain:x.a.com）。
	//
	// 单列出来是必要的：这是排除唯一会**不生效**的情形，不报出来的话它和
	// "本来就没有要排的东西"在摘要上长得一模一样 —— 而这个项目在别处已经把
	// 排除失效当成要让整个规则集失败的事故（见 pipeline 里 exclude 全挂那段）。
	Unexpressible int
	// Aggregated CIDR 聚合减少的条数。
	Aggregated int
}

func (d *Diagnostics) Skip(ruleType string) {
	if d.Skipped == nil {
		d.Skipped = map[string]int{}
	}
	d.Skipped[ruleType]++
}

func (d *Diagnostics) UnknownType(ruleType string) {
	if d.Unknown == nil {
		d.Unknown = map[string]int{}
	}
	d.Unknown[ruleType]++
}

// BadValue 记一个非法值，并保留前若干个样例供摘要展示。
func (d *Diagnostics) BadValue(field, sample string) {
	if d.Invalid == nil {
		d.Invalid = map[string]int{}
	}
	d.Invalid[field]++
	if len(d.InvalidSamples) < MaxInvalidSamples {
		d.InvalidSamples = append(d.InvalidSamples, sample)
	}
}

// InvalidTotal 是全部非法值条数。
func (d *Diagnostics) InvalidTotal() int {
	n := 0
	for _, v := range d.Invalid {
		n += v
	}
	return n
}

// Merge 把另一份诊断并进来。
func (d *Diagnostics) Merge(other *Diagnostics) {
	mergeCounts(&d.Skipped, other.Skipped)
	mergeCounts(&d.Unknown, other.Unknown)
	mergeCounts(&d.Invalid, other.Invalid)
	d.Dropped += other.Dropped
	d.Collapsed += other.Collapsed
	d.Subtracted += other.Subtracted
	d.Narrowed += other.Narrowed
	d.Unexpressible += other.Unexpressible
	d.Aggregated += other.Aggregated
	if room := MaxInvalidSamples - len(d.InvalidSamples); room > 0 {
		d.InvalidSamples = append(d.InvalidSamples, other.InvalidSamples[:min(room, len(other.InvalidSamples))]...)
	}
}

func mergeCounts(dst *map[string]int, src map[string]int) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]int, len(src))
	}
	for k, v := range src {
		(*dst)[k] += v
	}
}

// Top 按计数降序返回前 n 项，计数相同按名字排序 —— 摘要必须是确定性的。
func Top(counts map[string]int, n int) []struct {
	Name  string
	Count int
} {
	out := make([]struct {
		Name  string
		Count int
	}, 0, len(counts))
	for k, v := range counts {
		out = append(out, struct {
			Name  string
			Count int
		}{k, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}
