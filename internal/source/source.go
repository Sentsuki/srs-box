// Package source 是输入层：把一类上游变成规则集里的值。
//
// geosite 和 HTTP 是**同一层的两种输入**，不是两条流水线。一个规则集 = 一个名字
// + 一组输入，输入可以来自 URL、本地文件、内联规则行，也可以来自 geosite code，
// 几种混写。所以没有 type 判别字段，也不需要 merge provider 和拓扑排序。
package source

import (
	"context"

	"github.com/Sentsuki/srs-box/internal/ruleset"
	"github.com/Sentsuki/srs-box/internal/source/parse"
)

// Source 是一类输入的解析器。
//
// 两段式：Prepare 做一次性的共享工作（把全部 URL 抓下来、把 dlc.dat 下载解析好），
// Feed 再按 key 把内容并进目标规则集。这样"同一个地址被多个规则集引用只抓一次"
// 是结构上保证的，不靠调用方自觉。
type Source interface {
	// Kind 是输入种类，用在错误信息里。
	Kind() string

	// Prepare 为这批 key 准备内容。返回 error 表示这一类输入**整体**不可用
	// （比如 dlc.dat 校验失败），引用它的规则集各自记账，互不牵连。
	// 单个 key 取不到不算错误，留给 Feed 报告。
	Prepare(ctx context.Context, keys []string) error

	// Feed 把一个输入并进规则集。key 取不到或内容解析失败时返回 error，
	// 由调用方记进 Result.failed_sources 而不是中止构建 —— 全部输入都失败了
	// 才判这个规则集失败。
	Feed(key string, opts Options, into *ruleset.RuleSet) error
}

// Options 是调用方知道、而源不一定用得上的那些信息。
//
// Format 只对文本类输入有意义，geosite 源会忽略它 —— 把它放在调用参数里而不是
// 源的构造参数里，是因为它属于**规则集**而不属于源：同一个 URL 可以被两个规则集
// 以不同断言引用。
type Options struct {
	Format parse.Format
}

// FeedError 说明是哪个输入出了问题，供摘要逐条列出。
type FeedError struct {
	Kind string
	Key  string
	Err  error
}

func (e *FeedError) Error() string { return e.Kind + " " + e.Key + ": " + e.Err.Error() }
func (e *FeedError) Unwrap() error { return e.Err }

func feedErr(kind, key string, err error) error {
	return &FeedError{Kind: kind, Key: key, Err: err}
}
