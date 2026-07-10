package config

import (
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
)

func wantFull() Config {
	return Config{
		Log:     Log{Level: slog.LevelDebug, Format: logs.FormatJSON},
		Metrics: Metrics{Enabled: false},
		Engine: Engine{
			Shards:          8,
			FanoutThreshold: 50000,
			FanoutWorkers:   8,
			HistorySize:     128,
			HistoryTTL:      5 * time.Minute,
		},
		GC:   GC{Pacer: true, MemLimit: 1073741824, SpikeGOGC: 300, NormalGOGC: 150},
		Auth: Auth{Secret: "topsecret", Disabled: false},
		WebSocket: WebSocket{
			Addr:         "0.0.0.0:9080",
			ReadLimit:    131072,
			WriteTimeout: 20 * time.Second,
			SendBuffer:   512,
		},
		Server: Server{
			Addr:             "0.0.0.0:9100",
			MetricsAddr:      ":9090",
			DebugAddr:        ":6060",
			Arena:            true,
			MaxSessions:      4096,
			PingInterval:     15 * time.Second,
			AuthDeadline:     45 * time.Second,
			TLS:              TLS{Cert: "/etc/gentis/tls.crt", Key: "/etc/gentis/tls.key"},
			MaxMessageSize:   131072,
			MaxSubscriptions: 32,
		},
		Relay: Relay{
			Addr:             "0.0.0.0:9200",
			Upstream:         Upstream{Addr: "upstream:9000", AuthToken: "relaytoken", TLS: true, CA: "/etc/gentis/ca.pem"},
			MetricsAddr:      ":9091",
			Reconnect:        Reconnect{Initial: 200 * time.Millisecond, Max: time.Minute, Multiplier: 3.0, MaxRetries: 5},
			BufferSize:       512,
			IncomingBuffer:   8192,
			FanoutWorkers:    16,
			Arena:            true,
			MaxSessions:      8192,
			PingInterval:     10 * time.Second,
			AuthDeadline:     60 * time.Second,
			TLS:              TLS{Cert: "/etc/gentis/relay.crt", Key: "/etc/gentis/relay.key"},
			MaxMessageSize:   262144,
			MaxSubscriptions: 64,
		},
	}
}

func TestLoadFull(t *testing.T) {
	got, err := Load("testdata/full.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Namespaces == nil {
		t.Fatal("Namespaces registry must be built from the namespace section")
	}
	chat, ok := got.Namespaces.Resolve("chat:room")
	if !ok {
		t.Fatal("chat namespace must resolve")
	}
	if chat.HistorySize != 256 || chat.HistoryTTL != 10*time.Minute || chat.MaxSubscribers != 5000 {
		t.Fatalf("chat settings = %+v", chat)
	}
	got.Namespaces = nil
	if !reflect.DeepEqual(*got, wantFull()) {
		t.Fatalf("Load mismatch\n got: %+v\nwant: %+v", *got, wantFull())
	}
}

func TestLoadOmittedSectionsUseDefaults(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "")
	got, err := Load("testdata/minimal.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Namespaces != nil {
		t.Fatal("no namespace section must leave Namespaces nil")
	}
	want := Defaults()
	want.Auth.Disabled = true
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("defaults mismatch\n got: %+v\nwant: %+v", *got, want)
	}
}

func TestLoadResolvesAuthSecretFromEnv(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "from-env")
	got, err := Load("testdata/env_auth.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Auth.Secret != "from-env" || got.Auth.Disabled {
		t.Fatalf("auth = %+v, want secret resolved from env", got.Auth)
	}
}

func TestLoadInlineSecretWinsOverEnv(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "from-env")
	got, err := Load("testdata/full.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Auth.Secret != "topsecret" {
		t.Fatalf("inline hmac_secret must win over env, got %q", got.Auth.Secret)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "")
	tests := []struct {
		name string
		file string
		want error
	}{
		{"unknown top-level key", "testdata/unknown_top.yaml", nil},
		{"unknown nested key", "testdata/unknown_nested.yaml", nil},
		{"bad log level", "testdata/log_bad_level.yaml", ErrInvalid},
		{"tls incomplete", "testdata/tls_incomplete.yaml", ErrTLSIncomplete},
		{"auth conflict", "testdata/auth_conflict.yaml", ErrAuthConflict},
		{"engine bad", "testdata/engine_bad.yaml", ErrInvalid},
		{"gc bad", "testdata/gc_bad.yaml", ErrInvalid},
		{"relay bad", "testdata/relay_bad.yaml", ErrInvalid},
		{"bad namespace", "testdata/bad_namespace.yaml", namespace.ErrInvalidConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(tt.file)
			if err == nil {
				t.Fatalf("Load(%s) must fail", tt.file)
			}
			if cfg != nil {
				t.Fatalf("Load(%s) must return nil config on error, got %+v", tt.file, cfg)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("Load(%s) err = %v, want %v", tt.file, err, tt.want)
			}
		})
	}
}

func TestLoadNamespaceOnlyFileFailsAuth(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "")
	cfg, err := Load("../namespace/testdata/gentis.yaml")
	if !errors.Is(err, ErrAuthNotConfigured) {
		t.Fatalf("namespace-only file err = %v, want ErrAuthNotConfigured", err)
	}
	if cfg != nil {
		t.Fatalf("must return nil config on error, got %+v", cfg)
	}
}

func TestLoadNamespaceFileBackwardCompatible(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "x")
	got, err := Load("../../test/k6/gentis-qos.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Namespaces == nil {
		t.Fatal("namespace registry must be built")
	}
	jobs, ok := got.Namespaces.Resolve("jobs:build")
	if !ok {
		t.Fatal("jobs namespace must resolve")
	}
	if jobs.QoS != namespace.AtLeastOnce || jobs.HistorySize != 64 {
		t.Fatalf("jobs settings = %+v", jobs)
	}
}

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load("testdata/nope.yaml")
	if err == nil {
		t.Fatal("Load on missing file must fail")
	}
	if cfg != nil {
		t.Fatalf("must return nil config on error, got %+v", cfg)
	}
}

func TestRelayReady(t *testing.T) {
	t.Setenv("GENTIS_AUTH_HMAC_SECRET", "")
	ready, err := Load("testdata/full.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := ready.RelayReady(); err != nil {
		t.Fatalf("RelayReady with upstream = %v", err)
	}
	notReady, err := Load("testdata/minimal.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := notReady.RelayReady(); !errors.Is(err, ErrUpstreamRequired) {
		t.Fatalf("RelayReady without upstream = %v, want ErrUpstreamRequired", err)
	}
}
