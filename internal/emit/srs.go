package emit

import (
	"bufio"
	"fmt"
	"io"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/sagernet/sing-box/common/srs"
	C "github.com/sagernet/sing-box/constant"
)

// MaxSRSVersion 是链接进来的这版 sing-box 能写出的最高 rule-set 版本。
//
// 跟着库走而不是硬编码：Python 版把 4 写死在配置校验里，升级 sing-box 之后
// 要改两处才生效。
const MaxSRSVersion = uint8(C.RuleSetVersionCurrent)

// SRS 把规则集写成 .srs 二进制流。
func SRS(w io.Writer, set *ruleset.RuleSet, version uint8) error {
	if version < 1 || version > MaxSRSVersion {
		return fmt.Errorf("rule-set 版本 %d 越界，本版 sing-box 支持 1-%d", version, MaxSRSVersion)
	}
	buffered := bufio.NewWriter(w)
	if err := srs.Write(buffered, set.Options(), version); err != nil {
		return fmt.Errorf("写 .srs 失败: %w", err)
	}
	return buffered.Flush()
}

// SRSFile 把规则集原子地写到 path，返回产物字节数。
func SRSFile(path string, set *ruleset.RuleSet, version uint8) (int64, error) {
	return writeFileAtomic(path, func(w io.Writer) error {
		return SRS(w, set, version)
	})
}
