package parse

import (
	"bytes"

	"github.com/Sentsuki/srs-box/internal/emit"
	"github.com/sagernet/sing-box/common/srs"
)

// srsMagic 是 .srs 文件头。紧跟其后的一个字节是版本号。
var srsMagic = []byte{'S', 'R', 'S'}

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// auto 按内容判定形态。两次确定性的字节检查加一个兜底，没有任何启发式。
//
// 前两步都是**硬提交**：魔数和 { / [ 都是强信号，后续解析失败就报错而不回退。
// 一个损坏的 .srs 或残缺的 JSON 若回退成文本，会解析出一堆垃圾域名，
// 而且产物合法、条数正常，看不出任何异常。
func (p *parser) auto(data []byte) error {
	if isSRS(data) {
		return p.srs(data)
	}
	if b, ok := firstMeaningfulByte(data); ok && (b == '{' || b == '[') {
		return p.tree(bytes.TrimPrefix(bytes.TrimSpace(data), utf8BOM))
	}
	return p.lines(data)
}

// isSRS 判断是不是编译好的 rule-set。
//
// 魔数三字节 + 版本字节 ≤ 上限。文本文件恰好以 "SRS" 开头、第四字节又是
// 0x01-0x05 这种控制字符，实际上不可能发生。
func isSRS(data []byte) bool {
	if len(data) < 4 || !bytes.HasPrefix(data, srsMagic) {
		return false
	}
	return data[3] >= 1 && data[3] <= emit.MaxSRSVersion
}

// srs 反编译一份已编译的规则集。
//
// 链接 sing-box 之后白捡的一种输入：不少上游只发 .srs 不发 JSON，以前这类源
// 根本用不上 —— Python 版要做到得先起子进程 decompile 再读回来。
//
// recover=true 才会把编译后的域名 matcher 反推回列表：domain 和 domain_suffix
// 在 .srs 里不是原样存的，是一个 succinct set。
func (p *parser) srs(data []byte) error {
	compat, err := srs.Read(bytes.NewReader(data), true)
	if err != nil {
		return parseErr("", "这是一份 .srs（魔数匹配）但读不出来: "+err.Error(),
			"文件可能被截断，或者是本版 sing-box 还不认识的 rule-set 版本")
	}
	plain, err := compat.Upgrade()
	if err != nil {
		return parseErr("", "rule-set 版本无法处理: "+err.Error(), "")
	}
	for _, rule := range plain.Rules {
		p.set.AddVerbatim(rule)
	}
	return nil
}

// firstMeaningfulByte 返回跳过 BOM 和前导空白之后的第一个字节。
func firstMeaningfulByte(data []byte) (byte, bool) {
	data = bytes.TrimPrefix(data, utf8BOM)
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n', '\v', '\f':
			continue
		default:
			return b, true
		}
	}
	return 0, false
}
