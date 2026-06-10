// Package auth implements stateless token verification for transport
// sessions. Tokens are RFC 7519 JWTs signed with HS256; verification is
// pure stdlib (crypto/hmac, crypto/sha256, encoding/base64).
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrMalformedToken = errors.New("auth: malformed token")
	ErrUnsupportedAlg = errors.New("auth: unsupported algorithm")
	ErrBadSignature   = errors.New("auth: bad signature")
	ErrInvalidClaims  = errors.New("auth: invalid claims")
	ErrTokenExpired   = errors.New("auth: token expired")
)

// Claims is the verified identity of a session. A nil allowlist grants
// access to every channel; an empty non-nil allowlist grants none.
type Claims struct {
	Subject   string
	ExpiresAt time.Time
	Channels  []string
	Pub       []string
}

func (c *Claims) CanSubscribe(channel string) bool {
	return matchAny(c.Channels, channel)
}

func (c *Claims) CanPublish(channel string) bool {
	return matchAny(c.Pub, channel)
}

func matchAny(patterns []string, channel string) bool {
	if patterns == nil {
		return true
	}
	for _, p := range patterns {
		if matchPattern(p, channel) {
			return true
		}
	}
	return false
}

func matchPattern(pattern, channel string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(channel, prefix)
	}
	return pattern == channel
}

// Verifier is the boundary transports depend on. Concrete schemes
// (HMAC, insecure) live behind it.
type Verifier interface {
	Verify(token string) (Claims, error)
}

// InsecureVerifier accepts any token and grants full access. Used when
// authentication is explicitly disabled.
type InsecureVerifier struct{}

func (InsecureVerifier) Verify(string) (Claims, error) {
	return Claims{Subject: "anonymous"}, nil
}

type headerJSON struct {
	Alg string `json:"alg"`
}

// claimsJSON deliberately avoids omitempty on the allowlists: an explicit
// empty list means deny-all and must survive the wire round-trip, while
// absent (null) means allow-all.
type claimsJSON struct {
	Sub      string   `json:"sub"`
	Exp      int64    `json:"exp"`
	Channels []string `json:"channels"`
	Pub      []string `json:"pub"`
}

type HMACVerifier struct {
	secret []byte
	now    func() time.Time
}

func NewHMACVerifier(secret []byte) *HMACVerifier {
	return &HMACVerifier{secret: secret, now: time.Now}
}

func (v *HMACVerifier) Verify(token string) (Claims, error) {
	headerB64, rest, ok := strings.Cut(token, ".")
	if !ok {
		return Claims{}, ErrMalformedToken
	}
	payloadB64, sigB64, ok := strings.Cut(rest, ".")
	if !ok || strings.Contains(sigB64, ".") {
		return Claims{}, ErrMalformedToken
	}

	headerRaw, err := base64.RawURLEncoding.DecodeString(headerB64)
	if err != nil {
		return Claims{}, ErrMalformedToken
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return Claims{}, ErrMalformedToken
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return Claims{}, ErrMalformedToken
	}

	var header headerJSON
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return Claims{}, ErrMalformedToken
	}
	if header.Alg != "HS256" {
		return Claims{}, ErrUnsupportedAlg
	}

	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(token[:len(headerB64)+1+len(payloadB64)]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return Claims{}, ErrBadSignature
	}

	var payload claimsJSON
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return Claims{}, ErrInvalidClaims
	}
	if payload.Sub == "" || payload.Exp <= 0 {
		return Claims{}, ErrInvalidClaims
	}

	claims := Claims{
		Subject:   payload.Sub,
		ExpiresAt: time.Unix(payload.Exp, 0),
		Channels:  payload.Channels,
		Pub:       payload.Pub,
	}
	if !claims.ExpiresAt.After(v.now()) {
		return Claims{}, ErrTokenExpired
	}
	return claims, nil
}

// SignHS256 mints a token for the given claims. It exists for tests and
// tooling; the server only ever verifies.
func SignHS256(secret []byte, c Claims) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body, err := json.Marshal(claimsJSON{
		Sub:      c.Subject,
		Exp:      c.ExpiresAt.Unix(),
		Channels: c.Channels,
		Pub:      c.Pub,
	})
	if err != nil {
		panic(err)
	}
	signing := header + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
