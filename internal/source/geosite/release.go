package geosite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// DefaultRepo 是 dlc.dat 的上游。
const DefaultRepo = "v2fly/domain-list-community"

// maxDLCBytes 是 dlc.dat 的体积上限。实测 2.3MB，留足余量但不让误配吃光内存。
const maxDLCBytes = 64 << 20

const userAgent = "srs-box/0.3 (+https://github.com/Sentsuki/srs-box)"

// fetchDLC 下载并校验 dlc.dat。
//
// 不走 GitHub API：latest/download/<asset> 这个固定跳转就够了，于是不需要
// go-github 依赖、不需要 token、不受 API 限流。上游 sing-geosite 用 API 是为了
// 拿 release name 做"已是最新"的跳过判断，而我们每次都重新构建，不需要那个。
func fetchDLC(ctx context.Context, client *http.Client, repo string) ([]byte, error) {
	if repo == "" {
		repo = DefaultRepo
	}
	return fetchAt(ctx, client, "https://github.com/"+repo+"/releases/latest/download/")
}

// fetchAt 从一个 base 下拉 dlc.dat 与 dlc.dat.sha256sum 并比对。base 可注入，
// 测试就不必真的连 GitHub。
func fetchAt(ctx context.Context, client *http.Client, base string) ([]byte, error) {
	data, err := get(ctx, client, base+"dlc.dat", maxDLCBytes)
	if err != nil {
		return nil, fmt.Errorf("下载 dlc.dat 失败: %w", err)
	}
	sumRaw, err := get(ctx, client, base+"dlc.dat.sha256sum", 4096)
	if err != nil {
		return nil, fmt.Errorf("下载 dlc.dat.sha256sum 失败: %w", err)
	}
	if err := verifySHA256(data, sumRaw); err != nil {
		return nil, err
	}
	return data, nil
}

// verifySHA256 比对校验和。
//
// 这一步是必须的，而且比 sing-box 二进制那边容易：v2fly 的 release 里确实有
// dlc.dat.sha256sum，有官方摘要可比，不像 SagerNet 的 release 完全不发校验和、
// 只能靠"自钉"。
func verifySHA256(data, sumFile []byte) error {
	want := strings.TrimSpace(string(sumFile))
	// 格式是 "<hex>  <文件名>"，只取第一段。
	if idx := strings.IndexAny(want, " \t"); idx > 0 {
		want = want[:idx]
	}
	want = strings.ToLower(want)
	if len(want) != 64 {
		return fmt.Errorf("校验和文件格式不对，取到 %q", want)
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != want {
		return fmt.Errorf("dlc.dat 校验和不匹配，拒绝使用:\n  期望 %s\n  实际 %s", want, got)
	}
	return nil
}

func get(ctx context.Context, client *http.Client, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("响应超过 %d 字节上限", limit)
	}
	if len(data) == 0 {
		return nil, errors.New("响应为空")
	}
	return data, nil
}

// readLocalDLC 读本地 dlc.dat。
//
// 给两种场景用：离线跑，以及拿自己从 domain-list-community 源码树构建出来的
// dlc.dat 跑。用本地文件时不做校验和比对 —— 旁边没有 .sha256sum 可比，
// 而文件是你自己放的。
func readLocalDLC(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读 %s 失败: %w", path, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%s 是空文件", path)
	}
	return data, nil
}
