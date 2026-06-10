package pattern

import (
	"fmt"
	"testing"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{"metrics:*", "metrics:cpu", true},
		{"metrics:*", "metrics:", true},
		{"metrics:*", "logs:cpu", false},
		{"metrics:cpu", "metrics:cpu", true},
		{"metrics:cpu", "metrics:cpu2", false},
		{"metrics:cpu?", "metrics:cpu2", true},
		{"metrics:[ab]", "metrics:a", true},
		{"metrics:[ab]", "metrics:c", false},
		{"*", "anything", true},
		{"", "", true},
		{"", "x", false},
		{"metrics:[", "metrics:a", false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tt.pattern, tt.name), func(t *testing.T) {
			if got := Match(tt.pattern, tt.name); got != tt.want {
				t.Errorf("Match(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestIsPattern(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"metrics:*", true},
		{"metrics:cpu?", true},
		{"metrics:[ab]", true},
		{"metrics:cpu", false},
		{"", false},
		{"plain", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPattern(tt.name); got != tt.want {
				t.Errorf("IsPattern(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestCacheGetSet(t *testing.T) {
	c := NewCache[int](10)

	if _, ok := c.Get("missing"); ok {
		t.Fatal("Get on empty cache returned ok")
	}

	c.Set("a", 1)
	got, ok := c.Get("a")
	if !ok {
		t.Fatal("Get after Set returned not ok")
	}
	if got != 1 {
		t.Errorf("Get(a) = %d, want 1", got)
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCache[int](8)
	for i := range 9 {
		c.Set(fmt.Sprintf("key-%d", i), i)
	}

	if c.Len() > 8 {
		t.Errorf("cache size %d exceeds max 8", c.Len())
	}
	if c.Len() < 5 {
		t.Errorf("cache size %d, eviction removed more than ~25%%", c.Len())
	}
}

func TestCacheClear(t *testing.T) {
	c := NewCache[int](10)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Clear()

	if c.Len() != 0 {
		t.Errorf("Len after Clear = %d, want 0", c.Len())
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get after Clear returned ok")
	}
}
