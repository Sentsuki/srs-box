package source

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source/parse"
)

// File 是本地文件输入源。
//
// 有了它，仓库自带的数据文件就不必再绕一趟网络：旧配置里 apple-ai 指向
// github.com/Sentsuki/srs-box/raw/…/data/Apple_Intelligence.json —— 把自己仓库里
// 的文件下载回来。改成 files 之后不走网络、不会因 GitHub 抖动而失败、
// 还能立刻反映未提交的编辑。
type File struct {
	root string

	mu      sync.Mutex
	results map[string]result
}

// NewFile 建一个以 root 为根的文件源。root 之外的路径一律拒绝。
func NewFile(root string) *File {
	if root == "" {
		root = "."
	}
	return &File{root: root, results: map[string]result{}}
}

func (f *File) Kind() string { return "file" }

// Prepare 一次把用到的文件读进内存。
//
// 不并发：本地读盘快，而且规则文件只有几个。并发反而会让错误顺序不确定。
func (f *File) Prepare(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		if _, ok := f.results[key]; ok {
			continue
		}
		path, err := f.resolve(key)
		if err != nil {
			f.results[key] = result{err: err}
			continue
		}
		data, err := os.ReadFile(path)
		f.results[key] = result{body: data, err: err}
	}
	return nil
}

// resolve 把相对路径落到 root 里，并确认没有跳出去。
//
// 配置层已经挡过一道，这里再挡一道：配置可以是从别处拿来改一行就用的，
// 而符号链接和大小写差异这类事只有到了真实路径上才看得出来。
func (f *File) resolve(key string) (string, error) {
	if filepath.IsAbs(key) {
		return "", errors.New("只接受工作目录内的相对路径")
	}
	root, err := filepath.Abs(f.root)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, filepath.FromSlash(key))
	rel, err := filepath.Rel(root, full)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("路径跳出了工作目录")
	}
	return full, nil
}

func (f *File) Feed(key string, opts Options, into *ruleset.RuleSet) error {
	f.mu.Lock()
	res, ok := f.results[key]
	f.mu.Unlock()
	if !ok {
		return feedErr(f.Kind(), key, errors.New("未读取"))
	}
	if res.err != nil {
		return feedErr(f.Kind(), key, res.err)
	}
	if err := parse.Into(res.body, opts.Format, into); err != nil {
		return feedErr(f.Kind(), key, err)
	}
	return nil
}

// Inline 是配置里直写的规则行。
//
// key 就是那一行本身，所以不需要准备任何东西。留着这个源而不是让 pipeline
// 直接调解析层，是为了让"一个规则集的输入"在代码里是同一种东西 —— 错误记账、
// 失败隔离、摘要展示都只有一条路径。
type Inline struct{}

func NewInline() *Inline { return &Inline{} }

func (Inline) Kind() string { return "inline" }

func (Inline) Prepare(context.Context, []string) error { return nil }

func (Inline) Feed(key string, opts Options, into *ruleset.RuleSet) error {
	if err := parse.Into([]byte(key), opts.Format, into); err != nil {
		return feedErr("inline", key, err)
	}
	return nil
}
