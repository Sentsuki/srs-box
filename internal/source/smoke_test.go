package source

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Sentsuki/srs-box/internal/config"
	"github.com/Sentsuki/srs-box/internal/ruleset"
)

func TestSmokeRealSources(t *testing.T) {
	if os.Getenv("SRSBOX_SMOKE") == "" {
		t.Skip("需要网络，设 SRSBOX_SMOKE=1 运行")
	}
	cfg, err := config.Load("../../config.json")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"block-ads": true, "block-dns": true, "riot": true,
		"normal-ai": true, "skk-reject": true, "telegram-ip": true, "apple-api-ip": true}

	var urls []string
	var picked []*config.Ruleset
	for _, r := range cfg.Rulesets {
		if want[r.Name] {
			picked = append(picked, r)
			urls = append(urls, r.Sources...)
		}
	}
	src := NewHTTP(HTTPOptions{Concurrency: 8, Timeout: 30 * time.Second, Retries: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := src.Prepare(ctx, urls); err != nil {
		t.Fatal(err)
	}
	for _, spec := range picked {
		set := ruleset.New(spec.Name)
		var failed int
		for _, u := range spec.Sources {
			if err := src.Feed(u, Options{Format: spec.ParsedFormat()}, set); err != nil {
				t.Errorf("  %s ← %v", spec.Name, err)
				failed++
			}
		}
		before := set.Total()
		agg := 0
		if spec.Aggregate {
			agg = set.AggregateCIDR()
		}
		col := 0
		if spec.CollapseEnabled() {
			col = set.Collapse()
		}
		t.Logf("%-14s %6d → %-6d (聚合 -%d 收敛 -%d) 源失败 %d  跳过=%v 非法=%d  %v",
			spec.Name, before, set.Total(), agg, col, failed,
			set.Diag.Skipped, set.Diag.InvalidTotal(), fieldCounts(set))
	}
}

// fieldCounts 按输出顺序列出各字段条数，只给这条日志用。
func fieldCounts(set *ruleset.RuleSet) []string {
	var out []string
	for _, f := range ruleset.AllFields() {
		if n := len(set.Values(f)) + len(set.Ports(f)); n > 0 {
			out = append(out, fmt.Sprintf("%s=%d", f, n))
		}
	}
	if n := set.RuleObjects(); n != 1 {
		out = append(out, fmt.Sprintf("对象=%d", n))
	}
	return out
}
