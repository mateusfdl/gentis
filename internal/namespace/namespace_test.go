package namespace

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		name     string
		channel  string
		wantNS   string
		wantRest string
	}{
		{name: "no prefix is default namespace", channel: "orders", wantNS: "", wantRest: "orders"},
		{name: "prefixed channel", channel: "chat:room-1", wantNS: "chat", wantRest: "room-1"},
		{name: "only first colon splits", channel: "chat:room:1", wantNS: "chat", wantRest: "room:1"},
		{name: "empty namespace prefix", channel: ":weird", wantNS: "", wantRest: "weird"},
		{name: "empty rest", channel: "chat:", wantNS: "chat", wantRest: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, rest := Split(tt.channel)
			if ns != tt.wantNS || rest != tt.wantRest {
				t.Errorf("Split(%q) = (%q, %q), want (%q, %q)", tt.channel, ns, rest, tt.wantNS, tt.wantRest)
			}
		})
	}
}

func TestSplitDoesNotAllocate(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		Split("chat:room-1")
	})
	if allocs != 0 {
		t.Errorf("Split allocates %v times per run, want 0", allocs)
	}
}

func TestRegistryResolve(t *testing.T) {
	reg, err := NewRegistry(Config{
		Default: Settings{AllowPublish: true},
		Namespaces: map[string]Settings{
			"chat": {HistorySize: 128, HistoryTTL: 5 * time.Minute, AllowPublish: true},
			"feed": {AllowPublish: false, MaxSubscribers: 2},
		},
		Strict: true,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	tests := []struct {
		name    string
		channel string
		want    Settings
		wantOK  bool
	}{
		{
			name:    "default namespace channel",
			channel: "orders",
			want:    Settings{AllowPublish: true},
			wantOK:  true,
		},
		{
			name:    "known namespace",
			channel: "chat:room-1",
			want:    Settings{HistorySize: 128, HistoryTTL: 5 * time.Minute, AllowPublish: true},
			wantOK:  true,
		},
		{
			name:    "read only namespace",
			channel: "feed:news",
			want:    Settings{AllowPublish: false, MaxSubscribers: 2},
			wantOK:  true,
		},
		{
			name:    "unknown namespace rejected under strict",
			channel: "ghost:x",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := reg.Resolve(tt.channel)
			if ok != tt.wantOK {
				t.Fatalf("Resolve(%q) ok = %v, want %v", tt.channel, ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("Resolve(%q) = %+v, want %+v", tt.channel, got, tt.want)
			}
		})
	}
}

func TestNewRegistryValidatesNamespaces(t *testing.T) {
	_, err := NewRegistry(Config{
		Default: Settings{AllowPublish: true},
		Namespaces: map[string]Settings{
			"bad": {AllowPublish: true, AllowWildcard: true, Fanout: RoundRobin},
		},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRegistry with wildcard+round_robin err = %v, want ErrInvalidConfig", err)
	}
}

func TestNewRegistryValidatesDefault(t *testing.T) {
	_, err := NewRegistry(Config{
		Default: Settings{AllowPublish: true, QoS: AtLeastOnce},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewRegistry with at-least-once default and no history err = %v, want ErrInvalidConfig", err)
	}
}

func TestNewRegistryCopiesNamespaceMap(t *testing.T) {
	m := map[string]Settings{"chat": {HistorySize: 1, AllowPublish: true}}
	reg, err := NewRegistry(Config{Default: Settings{AllowPublish: true}, Namespaces: m})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	delete(m, "chat")
	m["chat"] = Settings{HistorySize: 999}

	got, ok := reg.Resolve("chat:x")
	if !ok || got.HistorySize != 1 {
		t.Fatalf("Resolve(chat:x) = (%+v, %v), want HistorySize 1: caller's map mutation must not reach the Registry", got, ok)
	}
}

func TestRegistryLenientFallsThroughToDefault(t *testing.T) {
	reg, err := NewRegistry(Config{
		Default:    Settings{AllowPublish: true, HistorySize: 4},
		Namespaces: map[string]Settings{"chat": {AllowPublish: true}},
		Strict:     false,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	got, ok := reg.Resolve("ghost:x")
	if !ok {
		t.Fatal("lenient registry must fall through to default, got ok=false")
	}
	if got != (Settings{AllowPublish: true, HistorySize: 4}) {
		t.Errorf("Resolve fell through to %+v, want default settings", got)
	}
}

func TestLoadFile(t *testing.T) {
	reg, err := LoadFile("testdata/gentis.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	got, ok := reg.Resolve("chat:room")
	if !ok {
		t.Fatal("chat namespace not loaded")
	}
	want := Settings{
		HistorySize:    256,
		HistoryTTL:     10 * time.Minute,
		AllowPublish:   true,
		MaxSubscribers: 5000,
	}
	if got != want {
		t.Errorf("chat settings = %+v, want %+v", got, want)
	}

	got, ok = reg.Resolve("feed:news")
	if !ok {
		t.Fatal("feed namespace not loaded")
	}
	if got.AllowPublish {
		t.Error("feed.AllowPublish = true, want false")
	}

	if _, ok := reg.Resolve("unknown:x"); ok {
		t.Error("strict fixture must reject unknown namespaces")
	}

	got, ok = reg.Resolve("plain")
	if !ok {
		t.Fatal("default namespace must always resolve")
	}
	if !got.AllowPublish || got.HistorySize != 16 {
		t.Errorf("default settings = %+v, want allow_publish true history_size 16", got)
	}
}

func TestLoadFileIdleReap(t *testing.T) {
	reg, err := LoadFile("testdata/idle_reap.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	got, ok := reg.Resolve("metrics:cpu")
	if !ok {
		t.Fatal("metrics namespace not loaded")
	}
	if got.IdleReap != 5*time.Minute {
		t.Errorf("metrics IdleReap = %v, want 5m", got.IdleReap)
	}
	if got.HistorySize != 64 {
		t.Errorf("metrics HistorySize = %d, want 64", got.HistorySize)
	}

	got, ok = reg.Resolve("jobs:x")
	if !ok {
		t.Fatal("jobs namespace not loaded")
	}
	if got.IdleReap != 0 {
		t.Errorf("jobs IdleReap = %v, want 0 (disabled)", got.IdleReap)
	}

	got, _ = reg.Resolve("plain")
	if got.IdleReap != 0 {
		t.Errorf("default IdleReap = %v, want 0 (disabled)", got.IdleReap)
	}
}

func TestLoadFileIdleReapNegative(t *testing.T) {
	if _, err := LoadFile("testdata/idle_reap_negative.yaml"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative idle_reap err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileRejectsUnknownKeys(t *testing.T) {
	_, err := LoadFile("testdata/unknown_key.yaml")
	if err == nil {
		t.Fatal("LoadFile with unknown key must fail")
	}
	if !strings.Contains(err.Error(), "histroy_size") {
		t.Errorf("error should name the offending key, got: %v", err)
	}
}

func TestLoadFileRejectsInvalidValues(t *testing.T) {
	_, err := LoadFile("testdata/negative_size.yaml")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("LoadFile with negative history_size: err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileMissing(t *testing.T) {
	if _, err := LoadFile("testdata/nope.yaml"); err == nil {
		t.Fatal("LoadFile on missing file must fail")
	}
}

func TestLoadFileQoS(t *testing.T) {
	reg, err := LoadFile("testdata/qos.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	got, ok := reg.Resolve("jobs:emails")
	if !ok {
		t.Fatal("jobs namespace not loaded")
	}
	if got.QoS != AtLeastOnce {
		t.Errorf("QoS = %v, want AtLeastOnce", got.QoS)
	}
	if got.RedeliveryTimeout != 5*time.Second {
		t.Errorf("RedeliveryTimeout = %v, want 5s", got.RedeliveryTimeout)
	}
	if got.MaxRedeliveries != 2 {
		t.Errorf("MaxRedeliveries = %d, want 2", got.MaxRedeliveries)
	}

	got, _ = reg.Resolve("defaults:x")
	if got.QoS != AtLeastOnce || got.RedeliveryTimeout != 30*time.Second || got.MaxRedeliveries != 3 {
		t.Errorf("defaults namespace = %+v, want at-least-once with 30s/3 defaults", got)
	}
}

func TestLoadFileQoSRequiresHistory(t *testing.T) {
	if _, err := LoadFile("testdata/qos_no_history.yaml"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("at-least-once without history: err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileQoSBadValue(t *testing.T) {
	if _, err := LoadFile("testdata/qos_bad.yaml"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad qos value: err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileFanoutMode(t *testing.T) {
	reg, err := LoadFile("testdata/fanout.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	tests := []struct {
		channel string
		want    FanoutMode
	}{
		{channel: "tasks:x", want: RoundRobin},
		{channel: "alerts:x", want: Priority},
		{channel: "plain", want: Broadcast},
	}
	for _, tt := range tests {
		got, ok := reg.Resolve(tt.channel)
		if !ok {
			t.Fatalf("Resolve(%q) failed", tt.channel)
		}
		if got.Fanout != tt.want {
			t.Errorf("Resolve(%q).Fanout = %v, want %v", tt.channel, got.Fanout, tt.want)
		}
	}
}

func TestLoadFileAllowWildcard(t *testing.T) {
	reg, err := LoadFile("testdata/wildcard.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	tests := []struct {
		channel string
		want    bool
	}{
		{channel: "metrics:x", want: true},
		{channel: "logs:x", want: false},
		{channel: "plain", want: false},
	}
	for _, tt := range tests {
		got, ok := reg.Resolve(tt.channel)
		if !ok {
			t.Fatalf("Resolve(%q) failed", tt.channel)
		}
		if got.AllowWildcard != tt.want {
			t.Errorf("Resolve(%q).AllowWildcard = %v, want %v", tt.channel, got.AllowWildcard, tt.want)
		}
	}
}

func TestLoadFileWildcardRequiresBroadcast(t *testing.T) {
	if _, err := LoadFile("testdata/wildcard_bad_fanout.yaml"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("allow_wildcard with round_robin: err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileFanoutBadValue(t *testing.T) {
	if _, err := LoadFile("testdata/fanout_bad.yaml"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad fanout_mode: err = %v, want ErrInvalidConfig", err)
	}
}

func TestLoadFileExplicitZeroMaxRedeliveries(t *testing.T) {
	reg, err := LoadFile("testdata/qos_zero_redeliveries.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	s, ok := reg.Resolve("jobs:x")
	if !ok {
		t.Fatal("Resolve(jobs:x) not known")
	}
	if s.MaxRedeliveries != 0 {
		t.Fatalf("MaxRedeliveries = %d, want 0 (explicit zero means poison on first timeout, not the default 3)", s.MaxRedeliveries)
	}
}

func TestLoadFileAllowsZeroRedeliveryTimeout(t *testing.T) {
	reg, err := LoadFile("testdata/redelivery_zero.yaml")
	if err != nil {
		t.Fatalf("redelivery_timeout: 0s must load (zero disables timeout redelivery): %v", err)
	}
	s, ok := reg.Resolve("jobs:x")
	if !ok {
		t.Fatal("jobs namespace not loaded")
	}
	if s.RedeliveryTimeout != 0 {
		t.Fatalf("RedeliveryTimeout = %v, want 0 (explicit zero preserved, not defaulted to 30s)", s.RedeliveryTimeout)
	}
}

func TestLoadFileEmptyConfigYieldsDefaults(t *testing.T) {
	reg, err := LoadFile("testdata/empty.yaml")
	if err != nil {
		t.Fatalf("LoadFile on empty config: %v", err)
	}
	s, ok := reg.Resolve("anything")
	if !ok {
		t.Fatal("empty config must resolve everything in lenient mode")
	}
	if !s.AllowPublish {
		t.Fatal("empty config default must allow publish")
	}
}
