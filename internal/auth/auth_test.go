package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	testSecret  = []byte("test-secret-key")
	wrongSecret = []byte("wrong-secret-key")
	testNow     = time.Unix(1700000000, 0)
)

func newTestVerifier() *HMACVerifier {
	v := NewHMACVerifier(testSecret)
	v.now = func() time.Time { return testNow }
	return v
}

func TestVerifyValidToken(t *testing.T) {
	claims := Claims{
		Subject:   "user-42",
		ExpiresAt: testNow.Add(time.Hour),
		Channels:  []string{"chat-*", "news"},
		Pub:       []string{"chat-42"},
	}
	token := SignHS256(testSecret, claims)

	got, err := newTestVerifier().Verify(token)
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got.Subject != "user-42" {
		t.Errorf("Subject = %q, want %q", got.Subject, "user-42")
	}
	if !got.ExpiresAt.Equal(testNow.Add(time.Hour)) {
		t.Errorf("ExpiresAt = %v, want %v", got.ExpiresAt, testNow.Add(time.Hour))
	}
	if !slices.Equal(got.Channels, []string{"chat-*", "news"}) {
		t.Errorf("Channels = %v, want %v", got.Channels, []string{"chat-*", "news"})
	}
	if !slices.Equal(got.Pub, []string{"chat-42"}) {
		t.Errorf("Pub = %v, want %v", got.Pub, []string{"chat-42"})
	}
}

func TestVerifyValidTokenWithoutAllowlists(t *testing.T) {
	token := SignHS256(testSecret, Claims{Subject: "user-1", ExpiresAt: testNow.Add(time.Minute)})

	got, err := newTestVerifier().Verify(token)
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got.Channels != nil {
		t.Errorf("Channels = %v, want nil", got.Channels)
	}
	if got.Pub != nil {
		t.Errorf("Pub = %v, want nil", got.Pub)
	}
}

func TestVerifyErrors(t *testing.T) {
	valid := SignHS256(testSecret, Claims{Subject: "user-1", ExpiresAt: testNow.Add(time.Hour)})
	segments := strings.Split(valid, ".")

	tests := []struct {
		name    string
		token   string
		wantErr error
	}{
		{
			name:    "empty token",
			token:   "",
			wantErr: ErrMalformedToken,
		},
		{
			name:    "two segments",
			token:   segments[0] + "." + segments[1],
			wantErr: ErrMalformedToken,
		},
		{
			name:    "four segments",
			token:   valid + ".extra",
			wantErr: ErrMalformedToken,
		},
		{
			name:    "header not base64",
			token:   "!!!." + segments[1] + "." + segments[2],
			wantErr: ErrMalformedToken,
		},
		{
			name:    "payload not base64",
			token:   segments[0] + ".!!!." + segments[2],
			wantErr: ErrMalformedToken,
		},
		{
			name:    "signature not base64",
			token:   segments[0] + "." + segments[1] + ".!!!",
			wantErr: ErrMalformedToken,
		},
		{
			name:    "header not JSON",
			token:   signRaw(testSecret, "not json", `{"sub":"u","exp":1700003600}`),
			wantErr: ErrMalformedToken,
		},
		{
			name:    "alg none",
			token:   signRaw(testSecret, `{"alg":"none","typ":"JWT"}`, `{"sub":"u","exp":1700003600}`),
			wantErr: ErrUnsupportedAlg,
		},
		{
			name:    "alg RS256",
			token:   signRaw(testSecret, `{"alg":"RS256","typ":"JWT"}`, `{"sub":"u","exp":1700003600}`),
			wantErr: ErrUnsupportedAlg,
		},
		{
			name:    "wrong secret",
			token:   SignHS256(wrongSecret, Claims{Subject: "user-1", ExpiresAt: testNow.Add(time.Hour)}),
			wantErr: ErrBadSignature,
		},
		{
			name:    "tampered payload",
			token:   segments[0] + "." + rawURLEncode(`{"sub":"admin","exp":1700003600}`) + "." + segments[2],
			wantErr: ErrBadSignature,
		},
		{
			name:    "payload not JSON",
			token:   signRaw(testSecret, `{"alg":"HS256","typ":"JWT"}`, "not json"),
			wantErr: ErrInvalidClaims,
		},
		{
			name:    "empty subject",
			token:   SignHS256(testSecret, Claims{Subject: "", ExpiresAt: testNow.Add(time.Hour)}),
			wantErr: ErrInvalidClaims,
		},
		{
			name:    "missing expiry",
			token:   signRaw(testSecret, `{"alg":"HS256","typ":"JWT"}`, `{"sub":"user-1"}`),
			wantErr: ErrInvalidClaims,
		},
		{
			name:    "expired token",
			token:   SignHS256(testSecret, Claims{Subject: "user-1", ExpiresAt: testNow.Add(-time.Second)}),
			wantErr: ErrTokenExpired,
		},
		{
			name:    "expiry exactly now",
			token:   SignHS256(testSecret, Claims{Subject: "user-1", ExpiresAt: testNow}),
			wantErr: ErrTokenExpired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestVerifier().Verify(tt.token)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Verify() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func rawURLEncode(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func signRaw(secret []byte, header, payload string) string {
	signing := rawURLEncode(header) + "." + rawURLEncode(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestCanSubscribe(t *testing.T) {
	tests := []struct {
		name     string
		channels []string
		channel  string
		want     bool
	}{
		{name: "nil allowlist allows all", channels: nil, channel: "anything", want: true},
		{name: "empty allowlist denies all", channels: []string{}, channel: "anything", want: false},
		{name: "exact match", channels: []string{"news"}, channel: "news", want: true},
		{name: "exact mismatch", channels: []string{"news"}, channel: "news2", want: false},
		{name: "prefix wildcard match", channels: []string{"chat-*"}, channel: "chat-42", want: true},
		{name: "prefix wildcard bare prefix", channels: []string{"chat-*"}, channel: "chat-", want: true},
		{name: "prefix wildcard mismatch", channels: []string{"chat-*"}, channel: "chats-42", want: false},
		{name: "star matches everything", channels: []string{"*"}, channel: "anything", want: true},
		{name: "second pattern matches", channels: []string{"news", "chat-*"}, channel: "chat-1", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Claims{Subject: "u", Channels: tt.channels}
			if got := c.CanSubscribe(tt.channel); got != tt.want {
				t.Errorf("CanSubscribe(%q) = %v, want %v", tt.channel, got, tt.want)
			}
		})
	}
}

func TestCanPublish(t *testing.T) {
	tests := []struct {
		name    string
		pub     []string
		channel string
		want    bool
	}{
		{name: "nil allowlist allows all", pub: nil, channel: "anything", want: true},
		{name: "empty allowlist denies all", pub: []string{}, channel: "anything", want: false},
		{name: "exact match", pub: []string{"chat-42"}, channel: "chat-42", want: true},
		{name: "wildcard match", pub: []string{"events-*"}, channel: "events-eu", want: true},
		{name: "mismatch", pub: []string{"chat-42"}, channel: "chat-43", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Claims{Subject: "u", Pub: tt.pub}
			if got := c.CanPublish(tt.channel); got != tt.want {
				t.Errorf("CanPublish(%q) = %v, want %v", tt.channel, got, tt.want)
			}
		})
	}
}

func TestSignRoundTripsEmptyAllowlists(t *testing.T) {
	token := SignHS256(testSecret, Claims{
		Subject:   "user-1",
		ExpiresAt: testNow.Add(time.Hour),
		Channels:  []string{},
		Pub:       []string{},
	})

	got, err := newTestVerifier().Verify(token)
	if err != nil {
		t.Fatalf("Verify() error = %v, want nil", err)
	}
	if got.Channels == nil || len(got.Channels) != 0 {
		t.Errorf("Channels = %v, want non-nil empty", got.Channels)
	}
	if got.Pub == nil || len(got.Pub) != 0 {
		t.Errorf("Pub = %v, want non-nil empty", got.Pub)
	}
	if got.CanSubscribe("anything") {
		t.Error("CanSubscribe with empty allowlist: want false")
	}
	if got.CanPublish("anything") {
		t.Error("CanPublish with empty allowlist: want false")
	}
}
