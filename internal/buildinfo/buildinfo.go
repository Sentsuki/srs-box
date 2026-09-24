// Package buildinfo 是版本号的唯一来源。
//
// 之前版本号散在三处（main 的 version、source 的 UserAgent、geosite 的
// userAgent），改一处忘两处的结果是 srs-box version 说 0.4、上游看到的 UA 还是
// 0.3 —— 出问题时按 UA 找人对不上账。放一起就不会再漂。
package buildinfo

import "strings"

const (
	// Version 是 srs-box 的版本号，srs-box version 输出的就是它。
	Version = "0.3.0"
	// Homepage 进 User-Agent，让被抓的一方知道是谁在抓、出问题找谁。
	Homepage = "https://github.com/Sentsuki/srs-box"
)

// UserAgent 是全部出站请求的 User-Agent。
//
// 只带 major.minor：patch 号对上游没有任何意义，却会让 UA 每发一个修订版就
// 换一次，把上游的日志和限流统计打散。
var UserAgent = "srs-box/" + majorMinor(Version) + " (+" + Homepage + ")"

func majorMinor(v string) string {
	major, rest, ok := strings.Cut(v, ".")
	if !ok {
		return v
	}
	minor, _, _ := strings.Cut(rest, ".")
	return major + "." + minor
}
