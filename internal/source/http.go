package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source/parse"
	"golang.org/x/sync/errgroup"
)

// UserAgent 让上游知道是谁在抓。
const UserAgent = "srs-box/0.3 (+https://github.com/Sentsuki/srs-box)"

// MaxBytes 是单个源的体积上限，防止误配一个巨大地址把内存吃光。
const MaxBytes = 64 << 20

// noRetry 是重试没有意义的状态码。
var noRetry = map[int]struct{}{
	400: {}, 401: {}, 403: {}, 404: {}, 405: {}, 410: {}, 451: {},
}

// HTTPOptions 是抓取层的设置。
type HTTPOptions struct {
	Concurrency int
	Timeout     time.Duration
	Retries     int
	// OnDone 每抓完一个地址回调一次，用于进度输出。可为 nil。
	OnDone func(url string, err error, done, total int)
}

// HTTP 是 URL 输入源。全程在内存里完成，不落临时文件。
//
// 不落盘省掉的是一整类问题：文件名冲突、断点续传把两个源首尾相接、清理阶段把
// 自己的缓存目录删掉。最大的源（几十万条 CIDR）也只有几 MB 文本。
type HTTP struct {
	opts   HTTPOptions
	client *http.Client

	mu      sync.Mutex
	results map[string]result
}

type result struct {
	body []byte
	err  error
}

func NewHTTP(opts HTTPOptions) *HTTP {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	return &HTTP{
		opts:    opts,
		results: map[string]result{},
		client: &http.Client{
			Timeout: opts.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: opts.Concurrency,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

func (h *HTTP) Kind() string { return "url" }

// Prepare 并发抓取全部地址。
//
// 地址先去重：同一个 URL 被多个规则集引用时只抓一次。
func (h *HTTP) Prepare(ctx context.Context, keys []string) error {
	pending := h.pending(keys)
	if len(pending) == 0 {
		return nil
	}

	// 信号量只包住真正在网络上的那一段，退避 sleep 不占并发位 —— 否则一个
	// 死链会霸着一个槽位睡完全部退避（1+2+4…秒），把其余源一起拖慢。
	// errgroup.SetLimit 做不到这件事：它按 goroutine 整个生命周期限流。
	slots := make(chan struct{}, h.opts.Concurrency)
	// 注意 gctx 而不是覆盖 ctx：errgroup 派生的 context 在 Wait 返回的那一刻
	// 就会被取消，拿它判断"是不是真的被取消了"永远为真。父 ctx 才是答案。
	group, gctx := errgroup.WithContext(ctx)

	var done int
	var progress sync.Mutex
	for _, url := range pending {
		group.Go(func() error {
			body, err := h.get(gctx, url, slots)
			h.mu.Lock()
			h.results[url] = result{body: body, err: err}
			h.mu.Unlock()

			progress.Lock()
			done++
			n := done
			progress.Unlock()
			if h.opts.OnDone != nil {
				h.opts.OnDone(url, err, n, len(pending))
			}
			// 单个地址失败不是整类输入失败，留给 Feed 报告。
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	// 只有取消才算整类输入失败 —— 那不是某个源的问题。
	return ctx.Err()
}

func (h *HTTP) pending(keys []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := map[string]struct{}{}
	var out []string
	for _, k := range keys {
		if _, ok := h.results[k]; ok {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out) // 抓取顺序稳定，日志才可读
	return out
}

func (h *HTTP) Feed(key string, opts Options, into *ruleset.RuleSet) error {
	h.mu.Lock()
	res, ok := h.results[key]
	h.mu.Unlock()
	if !ok {
		return feedErr(h.Kind(), key, errors.New("未抓取"))
	}
	if res.err != nil {
		return feedErr(h.Kind(), key, res.err)
	}
	if err := parse.Into(res.body, opts.Format, into); err != nil {
		return feedErr(h.Kind(), key, err)
	}
	return nil
}

func (h *HTTP) get(ctx context.Context, url string, slots chan struct{}) ([]byte, error) {
	var last error
	for attempt := 0; ; attempt++ {
		body, err, retriable := h.attempt(ctx, url, slots)
		if err == nil {
			return body, nil
		}
		last = err
		if !retriable || attempt >= h.opts.Retries {
			return nil, last
		}
		// 退避**不持有**信号量。
		backoff := time.Duration(1<<attempt) * time.Second
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func (h *HTTP) attempt(ctx context.Context, url string, slots chan struct{}) (body []byte, err error, retriable bool) {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err(), false
	}
	defer func() { <-slots }()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err, false
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := h.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err(), false
		}
		return nil, err, true
	}
	defer resp.Body.Close()

	if _, dead := noRetry[resp.StatusCode]; dead {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode), false
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode), true
	}

	raw, err := readCapped(resp.Body)
	if err != nil {
		return nil, err, !errors.Is(err, errTooLarge)
	}
	if len(trimSpace(raw)) == 0 {
		return nil, errors.New("响应为空"), true
	}
	return decode(raw), nil, false
}

var errTooLarge = fmt.Errorf("响应超过 %d MB 上限", MaxBytes>>20)

// readCapped 边收边数。
//
// 先把整个响应体读进内存再检查长度的话，内存已经花掉了 ——"防止误配一个巨大
// 地址把内存吃光"那句话就是假的。
func readCapped(r io.Reader) ([]byte, error) {
	limited := io.LimitReader(r, MaxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxBytes {
		return nil, errTooLarge
	}
	return data, nil
}

// decode 保证交给解析层的是合法 UTF-8。
//
// 规则列表几乎全是 ASCII，但偶尔有源用 GBK 之类编码写中文注释。非法字节替换成
// U+FFFD 而不是整份拒绝：注释坏掉不该让整个规则集失败。
func decode(data []byte) []byte {
	if utf8.Valid(data) {
		return data
	}
	return []byte(string([]rune(string(data))))
}

func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\v' || c == '\f'
}
