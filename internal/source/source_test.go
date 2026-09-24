package source

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source/parse"
)

func opts(t *testing.T) HTTPOptions {
	t.Helper()
	return HTTPOptions{Concurrency: 4, Timeout: 5 * time.Second, Retries: 2}
}

func TestHTTPFetchAndParse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/list":
			fmt.Fprint(w, "DOMAIN-SUFFIX,example.com\n# 注释\n+.plus.example\n")
		case "/json":
			fmt.Fprint(w, `{"version":4,"rules":[{"domain":["a.example.com"]}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	src := NewHTTP(opts(t))
	keys := []string{srv.URL + "/list", srv.URL + "/json"}
	if err := src.Prepare(context.Background(), keys); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	set := ruleset.New("t")
	for _, k := range keys {
		if err := src.Feed(k, Options{}, set); err != nil {
			t.Fatalf("Feed(%s): %v", k, err)
		}
	}
	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com", "plus.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	if got := set.Values(ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"a.example.com"}) {
		t.Errorf("domain = %v", got)
	}
}

// 同一个地址被多个规则集引用时只抓一次。
func TestHTTPDedupesURLs(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, "example.com\n")
	}))
	defer srv.Close()

	src := NewHTTP(opts(t))
	// 同一批里重复，以及分两批请求
	if err := src.Prepare(context.Background(), []string{srv.URL, srv.URL, srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("抓了 %d 次，应当只抓 1 次", n)
	}
}

func TestHTTPRetriesThenSucceeds(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "example.com\n")
	}))
	defer srv.Close()

	src := NewHTTP(opts(t))
	if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	set := ruleset.New("t")
	if err := src.Feed(srv.URL, Options{}, set); err != nil {
		t.Fatalf("重试之后应当成功: %v", err)
	}
	if n := hits.Load(); n != 3 {
		t.Errorf("请求了 %d 次, want 3", n)
	}
}

// 4xx 重试没有意义，不该浪费退避时间。
func TestHTTPDoesNotRetryDeadStatuses(t *testing.T) {
	for _, code := range []int{400, 403, 404, 410, 451} {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(code)
		}))
		src := NewHTTP(opts(t))
		if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
			t.Fatal(err)
		}
		if err := src.Feed(srv.URL, Options{}, ruleset.New("t")); err == nil {
			t.Errorf("HTTP %d 应当失败", code)
		}
		if n := hits.Load(); n != 1 {
			t.Errorf("HTTP %d 请求了 %d 次，不该重试", code, n)
		}
		srv.Close()
	}
}

// 单个地址失败不影响同批其他地址 —— 失败隔离是整个项目的硬规则。
func TestHTTPFailureIsIsolated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "example.com\n")
	}))
	defer srv.Close()

	src := NewHTTP(opts(t))
	good, bad := srv.URL+"/good", srv.URL+"/bad"
	if err := src.Prepare(context.Background(), []string{good, bad}); err != nil {
		t.Fatalf("单个地址失败不该让 Prepare 整体失败: %v", err)
	}
	set := ruleset.New("t")
	if err := src.Feed(good, Options{}, set); err != nil {
		t.Errorf("好地址应当成功: %v", err)
	}
	if err := src.Feed(bad, Options{}, set); err == nil {
		t.Error("坏地址应当报错")
	}
	if set.Total() != 1 {
		t.Errorf("好地址的内容应当照常进来，实际 %d 条", set.Total())
	}
}

// 边收边数：超限时不该把整个响应读进内存。
func TestHTTPRejectsOversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := strings.Repeat("a.example.com\n", 1024)
		for written := 0; written < MaxBytes+len(chunk); written += len(chunk) {
			if _, err := w.Write([]byte(chunk)); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	src := NewHTTP(HTTPOptions{Concurrency: 1, Timeout: 60 * time.Second, Retries: 0})
	if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	err := src.Feed(srv.URL, Options{}, ruleset.New("t"))
	if err == nil || !strings.Contains(err.Error(), "上限") {
		t.Errorf("超限应当被拒绝，实际: %v", err)
	}
}

func TestHTTPRejectsEmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "   \n\n  ")
	}))
	defer srv.Close()

	src := NewHTTP(HTTPOptions{Concurrency: 1, Timeout: 5 * time.Second, Retries: 0})
	if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := src.Feed(srv.URL, Options{}, ruleset.New("t")); err == nil {
		t.Error("空响应应当被拒绝")
	}
}

// 退避 sleep 不能占并发位 —— 否则一个死链会霸着槽位睡完全部退避，
// 把其余源一起拖慢。这里用"慢源在退避时，快源仍能跑完"来验证。
func TestBackoffDoesNotHoldConcurrencySlot(t *testing.T) {
	var (
		mu       sync.Mutex
		fastDone time.Time
		started  = make(chan struct{})
		once     sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/flaky" {
			once.Do(func() { close(started) })
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		<-started // 确保快源在慢源已经开始退避之后才响应
		mu.Lock()
		fastDone = time.Now()
		mu.Unlock()
		fmt.Fprint(w, "example.com\n")
	}))
	defer srv.Close()

	// 并发上限 1：退避若持有槽位，快源就永远等不到机会，测试会超时失败。
	src := NewHTTP(HTTPOptions{Concurrency: 1, Timeout: 5 * time.Second, Retries: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := src.Prepare(ctx, []string{srv.URL + "/flaky", srv.URL + "/fast"}); err != nil {
		t.Fatal(err)
	}
	if err := src.Feed(srv.URL+"/fast", Options{}, ruleset.New("t")); err != nil {
		t.Errorf("快源应当成功: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fastDone.IsZero() {
		t.Error("快源没跑成 —— 退避很可能占着并发槽位")
	}
}

func TestHTTPRespectsContextCancel(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	src := NewHTTP(HTTPOptions{Concurrency: 2, Timeout: 30 * time.Second, Retries: 3})
	start := time.Now()
	err := src.Prepare(ctx, []string{srv.URL})
	if err == nil {
		t.Error("取消之后 Prepare 应当返回错误")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("取消没有及时传播，耗时 %s", elapsed)
	}
}

func TestHTTPFormatAssertionPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// 上游返回了 HTTP 200 的 HTML 错误页 —— cidr 断言存在的理由
		fmt.Fprint(w, "<html><body>404 Not Found</body></html>\n")
	}))
	defer srv.Close()

	src := NewHTTP(opts(t))
	if err := src.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := src.Feed(srv.URL, Options{Format: parse.FormatCIDR}, ruleset.New("t")); err == nil {
		t.Error("cidr 断言应当挡下 HTML 错误页")
	}
}

// ---------------- 本地文件 ----------------

func TestFileSource(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data", "list.txt")
	if err := os.WriteFile(path, []byte("DOMAIN,a.example.com\n+.b.example\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewFile(dir)
	if err := src.Prepare(context.Background(), []string{"data/list.txt"}); err != nil {
		t.Fatal(err)
	}
	set := ruleset.New("t")
	if err := src.Feed("data/list.txt", Options{}, set); err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if got := set.Values(ruleset.FieldDomain); !reflect.DeepEqual(got, []string{"a.example.com"}) {
		t.Errorf("domain = %v", got)
	}
	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"b.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
}

func TestFileMissingIsPerKeyError(t *testing.T) {
	src := NewFile(t.TempDir())
	if err := src.Prepare(context.Background(), []string{"nope.txt"}); err != nil {
		t.Fatalf("缺文件不该让 Prepare 整体失败: %v", err)
	}
	if err := src.Feed("nope.txt", Options{}, ruleset.New("t")); err == nil {
		t.Error("缺文件应当在 Feed 时报错")
	}
}

// 配置层挡过一道，源这里再挡一道 —— 配置常常是从别处拿来改一行就用的。
func TestFileRejectsEscapingRoot(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "secret.txt")
	if err := os.WriteFile(outside, []byte("leak.example\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "work")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}

	src := NewFile(root)
	for _, key := range []string{"../secret.txt", "a/../../secret.txt"} {
		if err := src.Prepare(context.Background(), []string{key}); err != nil {
			t.Fatal(err)
		}
		err := src.Feed(key, Options{}, ruleset.New("t"))
		if err == nil {
			t.Errorf("%q 应当被拒绝", key)
		} else if !strings.Contains(err.Error(), "跳出") {
			t.Errorf("%q 报错信息不对: %v", key, err)
		}
	}
}

// ---------------- 内联 ----------------

func TestInline(t *testing.T) {
	src := NewInline()
	set := ruleset.New("t")
	for _, line := range []string{"DOMAIN-SUFFIX,anthropic.com", "DOMAIN-KEYWORD,openai", "+.bare.example"} {
		if err := src.Feed(line, Options{}, set); err != nil {
			t.Fatalf("Feed(%q): %v", line, err)
		}
	}
	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"anthropic.com", "bare.example"}) {
		t.Errorf("domain_suffix = %v", got)
	}
	if got := set.Values(ruleset.FieldDomainKeyword); !reflect.DeepEqual(got, []string{"openai"}) {
		t.Errorf("domain_keyword = %v", got)
	}
}

// ---------------- 混合 ----------------

// 项目真正的目标：任意来源混进一个规则集，去重后得到想要的那一份。
func TestMixedSourcesIntoOneRuleset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"version":4,"rules":[{"domain_suffix":["example.com"]}]}`)
	}))
	defer srv.Close()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "local.list"), []byte("DOMAIN-SUFFIX,example.com\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	set := ruleset.New("mixed")
	httpSrc := NewHTTP(opts(t))
	if err := httpSrc.Prepare(context.Background(), []string{srv.URL}); err != nil {
		t.Fatal(err)
	}
	fileSrc := NewFile(dir)
	if err := fileSrc.Prepare(context.Background(), []string{"local.list"}); err != nil {
		t.Fatal(err)
	}

	if err := httpSrc.Feed(srv.URL, Options{}, set); err != nil {
		t.Fatal(err)
	}
	if err := fileSrc.Feed("local.list", Options{}, set); err != nil {
		t.Fatal(err)
	}
	if err := NewInline().Feed("+.example.com", Options{}, set); err != nil {
		t.Fatal(err)
	}

	if got := set.Values(ruleset.FieldDomainSuffix); !reflect.DeepEqual(got, []string{"example.com"}) {
		t.Errorf("三种来源说同一件事，应当合并成一条，实际 %v", got)
	}
}
