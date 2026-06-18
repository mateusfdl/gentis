package relay

import (
	"sync"
	"testing"
)

func TestRouterNoPatterns(t *testing.T) {
	r := NewRouter(nil)

	result := r.Route("any-channel")
	if result.Mode != RouteModeRelay {
		t.Errorf("expected RouteModeRelay as default, got %d", result.Mode)
	}
}

func TestRouterExactMatch(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "chat", Mode: RouteModeLocal},
	})

	result := r.Route("chat")
	if result.Mode != RouteModeLocal {
		t.Errorf("expected RouteModeLocal, got %d", result.Mode)
	}

	result = r.Route("other")
	if result.Mode != RouteModeRelay {
		t.Errorf("expected RouteModeRelay for non-matching, got %d", result.Mode)
	}
}

func TestRouterGlobPattern(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "chat-*", Mode: RouteModeLocal},
		{Pattern: "*.private", Mode: RouteModeBoth},
	})

	tests := []struct {
		channel  string
		expected RouteMode
	}{
		{"chat-room1", RouteModeLocal},
		{"chat-general", RouteModeLocal},
		{"news.private", RouteModeBoth},
		{"alerts.private", RouteModeBoth},
		{"other", RouteModeRelay},
	}

	for _, tt := range tests {
		result := r.Route(tt.channel)
		if result.Mode != tt.expected {
			t.Errorf("Route(%q): expected mode %d, got %d", tt.channel, tt.expected, result.Mode)
		}
	}
}

func TestRouterFirstMatchWins(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "chat-*", Mode: RouteModeLocal},
		{Pattern: "chat-*", Mode: RouteModeBoth},
	})

	result := r.Route("chat-room")
	if result.Mode != RouteModeLocal {
		t.Errorf("expected RouteModeLocal (first match), got %d", result.Mode)
	}
}

func TestRouterAllModes(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "relay-*", Mode: RouteModeRelay},
		{Pattern: "local-*", Mode: RouteModeLocal},
		{Pattern: "both-*", Mode: RouteModeBoth},
	})

	tests := []struct {
		channel  string
		expected RouteMode
	}{
		{"relay-ch", RouteModeRelay},
		{"local-ch", RouteModeLocal},
		{"both-ch", RouteModeBoth},
		{"unknown", RouteModeRelay},
	}

	for _, tt := range tests {
		result := r.Route(tt.channel)
		if result.Mode != tt.expected {
			t.Errorf("Route(%q): expected %d, got %d", tt.channel, tt.expected, result.Mode)
		}
	}
}

func TestRouterCacheHit(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "chat-*", Mode: RouteModeLocal},
	})

	r.Route("chat-room")

	result := r.Route("chat-room")
	if result.Mode != RouteModeLocal {
		t.Errorf("expected RouteModeLocal from cache, got %d", result.Mode)
	}

	if _, exists := r.cache.Get("chat-room"); !exists {
		t.Error("expected chat-room to be cached")
	}
}

func TestRouterConcurrentRoute(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "chat-*", Mode: RouteModeLocal},
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := r.Route("chat-room")
			if result.Mode != RouteModeLocal {
				t.Errorf("expected RouteModeLocal, got %d", result.Mode)
			}
		}()
	}

	wg.Wait()
}

func TestRouterQuestionMarkIsLiteral(t *testing.T) {
	r := NewRouter([]ChannelPattern{
		{Pattern: "ch?", Mode: RouteModeLocal},
	})

	result := r.Route("ch?")
	if result.Mode != RouteModeLocal {
		t.Errorf("expected RouteModeLocal for literal 'ch?', got %d", result.Mode)
	}

	result = r.Route("ch1")
	if result.Mode != RouteModeRelay {
		t.Errorf("expected RouteModeRelay for 'ch1' (? is not a wildcard), got %d", result.Mode)
	}
}
