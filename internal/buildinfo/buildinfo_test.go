package buildinfo

import (
	"strings"
	"testing"
)

func TestUserAgent(t *testing.T) {
	if !strings.HasPrefix(UserAgent, "Mozilla/5.0") {
		t.Errorf("UserAgent should start with Mozilla/5.0, got %q", UserAgent)
	}
}
