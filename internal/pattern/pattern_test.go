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
		{"metrics:cpu?", "metrics:cpu2", false},
		{"metrics:cpu?", "metrics:cpu?", true},
		{"metrics:[ab]", "metrics:a", false},
		{"metrics:[ab]", "metrics:[ab]", true},
		{"metrics:[", "metrics:[", true},
		{"chat-*", "chat-a/b", true},
		{"*-end", "x-end", true},
		{"*-end", "x-ends", false},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "abc", true},
		{"a*b*c", "aXc", false},
		{"a*b", "ab", true},
		{"a*b", "aXXb", true},
		{"a*b", "aXbY", false},
		{"*", "anything", true},
		{"*", "", true},
		{"**", "anything", true},
		{"", "", true},
		{"", "x", false},
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
		{"*", true},
		{"a*b", true},
		{"metrics:cpu?", false},
		{"metrics:[ab]", false},
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

func TestHasReserved(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"metrics:cpu?", true},
		{"metrics:[ab]", true},
		{"metrics:a]b", true},
		{`metrics:a\b`, true},
		{"metrics:*", false},
		{"metrics:cpu", false},
		{"chat room", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasReserved(tt.name); got != tt.want {
				t.Errorf("HasReserved(%q) = %v, want %v", tt.name, got, tt.want)
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

func TestCacheTinyMaxSizeStillEvicts(t *testing.T) {
	c := NewCache[int](2)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	if got := c.Len(); got > 2 {
		t.Fatalf("Len = %d, want <= 2 (eviction must fire even when len/4 truncates to 0)", got)
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("newest entry must survive the eviction")
	}
}
