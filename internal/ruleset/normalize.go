package ruleset

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

// InvalidValueError 表示单个规则值非法。调用方决定是记账还是中止。
type InvalidValueError struct {
	Field  Field
	Raw    string
	Reason string
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("%s 的值非法 %q: %s", e.Field, e.Raw, e.Reason)
}

func invalid(f Field, raw, reason string) error {
	return &InvalidValueError{Field: f, Raw: raw, Reason: reason}
}

const (
	maxHostLen  = 253
	maxLabelLen = 63
)

// idnaProfile 对应 Python 版 label.encode("idna") 的位置，但行为不同：
// 那是 IDNA2003，这里是 IDNA2008 非过渡模式。这是一处已知的、有意的差异，
// 对拍阶段要重点看混合域名。StrictDomainName(false) 是因为校验由 validHost
// 统一做，不想让 idna 用另一套规则抢先拒绝。
var idnaProfile = idna.New(idna.MapForLookup(), idna.Transitional(false), idna.StrictDomainName(false))

// NormalizeHost 归一域名：去首尾空白、小写、去末尾根点、punycode。保留前导点。
//
// 前导点必须保留：sing-box 里 domain_suffix "example.com"（无点）匹配
// example.com 及其全部子域，而 ".example.com"（带点）只匹配子域，本身不命中。
// 两者不是一回事，归一掉就是改语义。
func NormalizeHost(raw string) (string, error) {
	host := strings.ToLower(strings.TrimSpace(raw))
	host = strings.TrimRight(host, ".")

	lead := ""
	if strings.HasPrefix(host, ".") {
		lead = "."
		host = strings.TrimLeft(host, ".")
	}
	if host == "" {
		return "", fmt.Errorf("空域名")
	}

	host = toASCII(host)
	if err := validHost(host); err != nil {
		return "", err
	}
	return lead + host, nil
}

// toASCII 逐 label 转 punycode；转不了的 label 原样保留。
//
// 逐 label 而非整串：整串失败会丢掉整个混合域名，而实际只有某一段有问题。
func toASCII(host string) string {
	if isASCII(host) {
		return host
	}
	labels := strings.Split(host, ".")
	for i, label := range labels {
		if isASCII(label) {
			continue
		}
		if ascii, err := idnaProfile.ToASCII(label); err == nil && ascii != "" {
			labels[i] = ascii
		}
	}
	return strings.Join(labels, ".")
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// validHost 手写校验，不用正则。
//
// Python 版那条正则用了前后瞻（(?!-) 和 (?<!-)）来禁止 label 以连字符开头结尾，
// 而 Go 的 RE2 两者都不支持 —— 硬翻会悄悄放宽校验。显式循环反而更清楚。
//
// 不接受 '*'：带通配符的值在 sing-box 的 domain / domain_suffix 里是字面量，
// 永不命中。通配符域名由解析层转成 domain_regex，到不了这里。
func validHost(host string) error {
	if len(host) > maxHostLen {
		return fmt.Errorf("超过 %d 字节", maxHostLen)
	}
	for label := range strings.SplitSeq(host, ".") {
		if label == "" {
			return fmt.Errorf("有空 label")
		}
		if len(label) > maxLabelLen {
			return fmt.Errorf("label %q 超过 %d 字节", label, maxLabelLen)
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("label %q 以连字符开头或结尾", label)
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
			default:
				return fmt.Errorf("label %q 含非法字符 %q", label, string(c))
			}
		}
	}
	return nil
}

// NormalizeCIDR 归一 IP/CIDR。裸 IP 补满掩码；主机位非零的网段按网络地址归整。
//
// netip.ParsePrefix 不像 Python 的 ip_network(..., strict=False) 会自动归整，
// 它把主机位原样留着 —— 不调 Masked 的话 1.2.3.4/24 和 1.2.3.0/24 会变成
// 两条不同的规则，去重失效。
func NormalizeCIDR(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if prefix, err := netip.ParsePrefix(text); err == nil {
		return prefix.Masked().String(), nil
	}
	if addr, err := netip.ParseAddr(text); err == nil {
		return netip.PrefixFrom(addr, addr.BitLen()).String(), nil
	}
	return "", fmt.Errorf("不是合法的 IP 或 CIDR")
}

// NormalizePort 归一端口。
func NormalizePort(raw string) (uint16, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 16)
	if err != nil {
		return 0, fmt.Errorf("不是 0-65535 的整数")
	}
	return uint16(n), nil
}

// normalizeString 按字段把原始值归一成可直接写进产物的字符串。
func normalizeString(f Field, raw string) (string, error) {
	switch fields[f].kind {
	case kindHost:
		v, err := NormalizeHost(raw)
		if err != nil {
			return "", invalid(f, raw, err.Error())
		}
		return v, nil
	case kindCIDR:
		v, err := NormalizeCIDR(raw)
		if err != nil {
			return "", invalid(f, raw, err.Error())
		}
		return v, nil
	case kindExact:
		v := strings.TrimSpace(raw)
		if v == "" {
			return "", invalid(f, raw, "值为空")
		}
		return v, nil
	default: // kindLower
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			return "", invalid(f, raw, "值为空")
		}
		return v, nil
	}
}
