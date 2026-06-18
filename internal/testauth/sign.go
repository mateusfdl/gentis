package testauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/mateusfdl/gentis/internal/auth"
)

type claimsJSON struct {
	Sub      string   `json:"sub"`
	Exp      int64    `json:"exp"`
	Channels []string `json:"channels"`
	Pub      []string `json:"pub"`
}

func SignHS256(secret []byte, c auth.Claims) string {
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
