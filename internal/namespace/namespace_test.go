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
	reg := NewRegistry(Config{
		Default: Settings{AllowPublish: true},
		Namespaces: map[string]Settings{
			"chat": {HistorySize: 128, HistoryTTL: 5 * time.Minute, AllowPublish: true},
			"feed": {AllowPublish: false, MaxSubscribers: 2},
		},
		Strict: true,
	})

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

func TestRegistryLenientFallsThroughToDefault(t *testing.T) {
	reg := NewRegistry(Config{
		Default:    Settings{AllowPublish: true, HistorySize: 4},
		Namespaces: map[string]Settings{"chat": {AllowPublish: true}},
		Strict:     false,
	})

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
