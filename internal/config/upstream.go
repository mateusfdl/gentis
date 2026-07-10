package config

type Upstream struct {
	Addr      string
	AuthToken string
	TLS       bool
	CA        string
}
