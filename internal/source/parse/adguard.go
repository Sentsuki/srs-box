package parse

import (
	"bytes"
	"fmt"

	"github.com/sagernet/sing-box/common/convertor/adguard"
)

// adguard 用 sing-box 自带的转换器处理 AdGuard 过滤器。
//
// 这是唯一必须在配置里显式声明的格式，因为它判不出来而且猜错的代价极大：
//
//   - 普通规则编译进**专属的 AdGuardDomain 字段**（srs 里是独立的 item 类型、
//     独立的 matcher），不是 domain / domain_suffix。
//   - @@ 例外规则编译成**嵌套 logical 规则 + Invert**。
//
// 所以拿通用提取器去"提取域名"对 AdGuard 不是不够准，是**语义反转**：
// @@||ok.com^ 会从"放行"变成"拦截"。产物合法、编译通过、条数正常，
// 要等到某个网站打不开才察觉。
//
// 转换结果整条走 verbatim：AdGuardDomain 不在我们拆开处理的十二个字段里，
// 而且带 invert 的嵌套结构本来就不该被拆散去重。
func (p *parser) adguard(data []byte) error {
	rules, err := adguard.ToOptions(bytes.NewReader(data), &diagLogger{p: p})
	if err != nil {
		return parseErr("", "AdGuard 过滤器解析失败: "+err.Error(),
			"确认这个源确实是 AdGuard 语法；不是的话把 format 去掉，按内容自动判定")
	}
	for _, rule := range rules {
		p.set.AddVerbatim(rule)
	}
	return nil
}

// diagLogger 把转换器的抱怨收进诊断，而不是丢进 /dev/null。
//
// 转换器会对表达不了的规则打 warn。用 logger.NOP() 的话这些信息全没了 ——
// 而"跳过了多少条"正是摘要该告诉人的东西。
type diagLogger struct {
	p *parser
}

func (l *diagLogger) record(args ...any) {
	l.p.set.Diag.Skip("ADGUARD-UNSUPPORTED")
	if len(l.p.set.Diag.InvalidSamples) < 1 {
		l.p.set.Diag.BadValue("adguard", fmt.Sprint(args...))
	}
}

func (l *diagLogger) Warn(args ...any)  { l.record(args...) }
func (l *diagLogger) Error(args ...any) { l.record(args...) }
func (l *diagLogger) Trace(args ...any) {}
func (l *diagLogger) Debug(args ...any) {}
func (l *diagLogger) Info(args ...any)  {}
func (l *diagLogger) Fatal(args ...any) {}
func (l *diagLogger) Panic(args ...any) {}
