// Package emit 把规则集写成产物。
//
// 这一整包取代了 Python 版的 compiler.py：不再下载 sing-box 二进制、不再解压、
// 不再校验和自钉、不再起子进程、不再按输入大小放缩编译超时。.srs 由链接进来的
// sing-box 直接写出。
package emit

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// writeFileAtomic 先写同目录临时文件再改名。
//
// 直接往目标路径写的话，中途失败会在输出目录里留下半个文件，而发布流程分不清
// 它是本次产出还是上次残留。原子改名保证目标要么不存在，要么是完整的。
func writeFileAtomic(path string, write func(io.Writer) error) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, fmt.Errorf("建目录失败: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return 0, fmt.Errorf("建临时文件失败: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // 成功路径上已经改名走了，这里是清理失败残留
	}()

	if err := write(tmp); err != nil {
		return 0, err
	}
	if err := tmp.Sync(); err != nil {
		return 0, fmt.Errorf("落盘失败: %w", err)
	}
	info, err := tmp.Stat()
	if err != nil {
		return 0, fmt.Errorf("读临时文件大小失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("关闭临时文件失败: %w", err)
	}
	// Windows 上目标已存在时 rename 会失败，先删。
	_ = os.Remove(path)
	if err := os.Rename(tmpName, path); err != nil {
		return 0, fmt.Errorf("改名到 %s 失败: %w", path, err)
	}
	return info.Size(), nil
}
