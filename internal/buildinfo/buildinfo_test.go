package buildinfo

import "strings"

import "testing"

// UA 必须跟着 Version 走 —— 这正是拆出这个包要解决的问题，
// 手写一份 UA 常量就等于又开了一个会漂的副本。
func TestUserAgentTracksVersion(t *testing.T) {
	major, rest, _ := strings.Cut(Version, ".")
	minor, _, _ := strings.Cut(rest, ".")
	want := "srs-box/" + major + "." + minor + " (+" + Homepage + ")"
	if UserAgent != want {
		t.Errorf("UserAgent = %q, want %q", UserAgent, want)
	}
	if strings.Count(UserAgent, ".") != 1+strings.Count(Homepage, ".") {
		t.Errorf("UA 里带上了 patch 号: %q", UserAgent)
	}
}

func TestMajorMinor(t *testing.T) {
	for in, want := range map[string]string{
		"0.3.0": "0.3", "1.10.2": "1.10", "2.0": "2.0", "3": "3",
	} {
		if got := majorMinor(in); got != want {
			t.Errorf("majorMinor(%q) = %q, want %q", in, got, want)
		}
	}
}
