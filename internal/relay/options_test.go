package relay

import "testing"

func TestWithUpstreamPreservesTLS(t *testing.T) {
	cfg := defaultConfig()
	WithUpstreamTLS("ca.pem")(cfg)
	WithUpstream("origin:9000", "tok")(cfg)

	want := UpstreamConfig{Address: "origin:9000", AuthToken: "tok", TLS: true, CAFile: "ca.pem"}
	if cfg.Upstream != want {
		t.Fatalf("Upstream = %+v, want %+v (option order must not wipe TLS)", cfg.Upstream, want)
	}
}

func TestDefaultMaxSubscriptionsIsUnlimited(t *testing.T) {
	if got := defaultConfig().MaxSubscriptions; got != 0 {
		t.Fatalf("default MaxSubscriptions = %d, want 0 (documented as unlimited)", got)
	}
}
