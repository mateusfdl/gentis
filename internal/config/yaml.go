package config

import (
	"fmt"
	"os"
	"time"

	"github.com/mateusfdl/gentis/internal/logs"
	"github.com/mateusfdl/gentis/internal/namespace"
)

type logYAML struct {
	Level  *string `yaml:"level"`
	Format *string `yaml:"format"`
}

type metricsYAML struct {
	Enabled *bool `yaml:"enabled"`
}

type engineYAML struct {
	Shards          *int           `yaml:"shards"`
	FanoutThreshold *int           `yaml:"fanout_threshold"`
	FanoutWorkers   *int           `yaml:"fanout_workers"`
	HistorySize     *int           `yaml:"history_size"`
	HistoryTTL      *time.Duration `yaml:"history_ttl"`
}

type gcYAML struct {
	Pacer      *bool  `yaml:"pacer"`
	MemLimit   *int64 `yaml:"mem_limit"`
	SpikeGOGC  *int   `yaml:"spike_gogc"`
	NormalGOGC *int   `yaml:"normal_gogc"`
}

type authYAML struct {
	HMACSecret    *string `yaml:"hmac_secret"`
	HMACSecretEnv *string `yaml:"hmac_secret_env"`
	Disabled      *bool   `yaml:"disabled"`
}

type tlsYAML struct {
	Cert *string `yaml:"cert"`
	Key  *string `yaml:"key"`
}

type wsYAML struct {
	Addr         *string        `yaml:"addr"`
	ReadLimit    *int64         `yaml:"read_limit"`
	WriteTimeout *time.Duration `yaml:"write_timeout"`
	SendBuffer   *int           `yaml:"send_buffer"`
}

type serverYAML struct {
	Addr             *string        `yaml:"addr"`
	MetricsAddr      *string        `yaml:"metrics_addr"`
	DebugAddr        *string        `yaml:"debug_addr"`
	Arena            *bool          `yaml:"arena"`
	MaxSessions      *int           `yaml:"max_sessions"`
	PingInterval     *time.Duration `yaml:"ping_interval"`
	AuthDeadline     *time.Duration `yaml:"auth_deadline"`
	TLS              *tlsYAML       `yaml:"tls"`
	MaxMessageSize   *int           `yaml:"max_message_size"`
	MaxSubscriptions *int           `yaml:"max_subscriptions"`
}

type upstreamYAML struct {
	Addr      *string `yaml:"addr"`
	AuthToken *string `yaml:"auth_token"`
	TLS       *bool   `yaml:"tls"`
	CA        *string `yaml:"ca"`
}

type reconnectYAML struct {
	Initial    *time.Duration `yaml:"initial"`
	Max        *time.Duration `yaml:"max"`
	Multiplier *float64       `yaml:"multiplier"`
	MaxRetries *int           `yaml:"max_retries"`
}

type relayYAML struct {
	Addr             *string        `yaml:"addr"`
	Upstream         *upstreamYAML  `yaml:"upstream"`
	MetricsAddr      *string        `yaml:"metrics_addr"`
	Reconnect        *reconnectYAML `yaml:"reconnect"`
	BufferSize       *int           `yaml:"buffer_size"`
	IncomingBuffer   *int           `yaml:"incoming_buffer"`
	FanoutWorkers    *int           `yaml:"fanout_workers"`
	Arena            *bool          `yaml:"arena"`
	MaxSessions      *int           `yaml:"max_sessions"`
	PingInterval     *time.Duration `yaml:"ping_interval"`
	AuthDeadline     *time.Duration `yaml:"auth_deadline"`
	TLS              *tlsYAML       `yaml:"tls"`
	MaxMessageSize   *int           `yaml:"max_message_size"`
	MaxSubscriptions *int           `yaml:"max_subscriptions"`
}

type fileYAML struct {
	Log       *logYAML     `yaml:"log"`
	Metrics   *metricsYAML `yaml:"metrics"`
	Engine    *engineYAML  `yaml:"engine"`
	GC        *gcYAML      `yaml:"gc"`
	Auth      *authYAML    `yaml:"auth"`
	WebSocket *wsYAML      `yaml:"websocket"`
	Server    *serverYAML  `yaml:"server"`
	Relay     *relayYAML   `yaml:"relay"`

	namespace.ConfigYAML `yaml:",inline"`
}

func (f fileYAML) apply(c *Config) error {
	if err := applyLog(f.Log, &c.Log); err != nil {
		return err
	}

	applyMetrics(f.Metrics, &c.Metrics)
	applyEngine(f.Engine, &c.Engine)
	applyGC(f.GC, &c.GC)
	applyWebSocket(f.WebSocket, &c.WebSocket)
	applyServer(f.Server, &c.Server)
	applyRelay(f.Relay, &c.Relay)

	return applyAuth(f.Auth, &c.Auth)
}

func applyLog(y *logYAML, l *Log) error {
	if y == nil {
		return nil
	}

	if y.Level != nil {
		level, err := logs.ParseLevel(*y.Level)
		if err != nil {
			return fmt.Errorf("%w: log.level: %v", ErrInvalid, err)
		}

		l.Level = level
	}

	if y.Format != nil {
		format, err := logs.ParseFormat(*y.Format)
		if err != nil {
			return fmt.Errorf("%w: log.format: %v", ErrInvalid, err)
		}

		l.Format = format
	}

	return nil
}

func applyMetrics(y *metricsYAML, m *Metrics) {
	if y == nil {
		return
	}

	setBool(y.Enabled, &m.Enabled)
}

func applyEngine(y *engineYAML, e *Engine) {
	if y == nil {
		return
	}

	setInt(y.Shards, &e.Shards)
	setInt(y.FanoutThreshold, &e.FanoutThreshold)
	setInt(y.FanoutWorkers, &e.FanoutWorkers)
	setInt(y.HistorySize, &e.HistorySize)
	setDuration(y.HistoryTTL, &e.HistoryTTL)
}

func applyGC(y *gcYAML, g *GC) {
	if y == nil {
		return
	}

	setBool(y.Pacer, &g.Pacer)
	setInt64(y.MemLimit, &g.MemLimit)
	setInt(y.SpikeGOGC, &g.SpikeGOGC)
	setInt(y.NormalGOGC, &g.NormalGOGC)
}

func applyAuth(y *authYAML, a *Auth) error {
	secretEnv := defaultHMACSecretEnv
	if y != nil {
		setBool(y.Disabled, &a.Disabled)
		setString(y.HMACSecretEnv, &secretEnv)
		setString(y.HMACSecret, &a.Secret)
	}

	if a.Secret == "" && secretEnv != "" {
		a.Secret = os.Getenv(secretEnv)
	}

	return nil
}

func applyWebSocket(y *wsYAML, w *WebSocket) {
	if y == nil {
		return
	}

	setString(y.Addr, &w.Addr)
	setInt64(y.ReadLimit, &w.ReadLimit)
	setDuration(y.WriteTimeout, &w.WriteTimeout)
	setInt(y.SendBuffer, &w.SendBuffer)
}

func applyServer(y *serverYAML, s *Server) {
	if y == nil {
		return
	}

	setString(y.Addr, &s.Addr)
	setString(y.MetricsAddr, &s.MetricsAddr)
	setString(y.DebugAddr, &s.DebugAddr)
	setBool(y.Arena, &s.Arena)
	setInt(y.MaxSessions, &s.MaxSessions)
	setDuration(y.PingInterval, &s.PingInterval)
	setDuration(y.AuthDeadline, &s.AuthDeadline)
	setInt(y.MaxMessageSize, &s.MaxMessageSize)
	setInt(y.MaxSubscriptions, &s.MaxSubscriptions)
	applyTLS(y.TLS, &s.TLS)
}

func applyRelay(y *relayYAML, r *Relay) {
	if y == nil {
		return
	}

	setString(y.Addr, &r.Addr)
	setString(y.MetricsAddr, &r.MetricsAddr)
	setInt(y.BufferSize, &r.BufferSize)
	setInt(y.IncomingBuffer, &r.IncomingBuffer)
	setInt(y.FanoutWorkers, &r.FanoutWorkers)
	setBool(y.Arena, &r.Arena)
	setInt(y.MaxSessions, &r.MaxSessions)
	setDuration(y.PingInterval, &r.PingInterval)
	setDuration(y.AuthDeadline, &r.AuthDeadline)
	setInt(y.MaxMessageSize, &r.MaxMessageSize)
	setInt(y.MaxSubscriptions, &r.MaxSubscriptions)
	applyTLS(y.TLS, &r.TLS)
	applyUpstream(y.Upstream, &r.Upstream)
	applyReconnect(y.Reconnect, &r.Reconnect)
}

func applyUpstream(y *upstreamYAML, u *Upstream) {
	if y == nil {
		return
	}

	setString(y.Addr, &u.Addr)
	setString(y.AuthToken, &u.AuthToken)
	setBool(y.TLS, &u.TLS)
	setString(y.CA, &u.CA)
}

func applyReconnect(y *reconnectYAML, rc *Reconnect) {
	if y == nil {
		return
	}

	setDuration(y.Initial, &rc.Initial)
	setDuration(y.Max, &rc.Max)
	setFloat64(y.Multiplier, &rc.Multiplier)
	setInt(y.MaxRetries, &rc.MaxRetries)
}

func applyTLS(y *tlsYAML, t *TLS) {
	if y == nil {
		return
	}

	setString(y.Cert, &t.Cert)
	setString(y.Key, &t.Key)
}

func namespaceSectionPresent(c namespace.ConfigYAML) bool {
	if c.Strict || len(c.Namespaces) > 0 {
		return true
	}

	return defaultSettingsSet(c.Default)
}

func defaultSettingsSet(d namespace.SettingsYAML) bool {
	present := []bool{
		d.HistorySize != nil, d.HistoryTTL != nil, d.AllowPublish != nil,
		d.MaxSubscribers != nil, d.QoS != nil, d.Fanout != nil,
		d.AllowWildcard != nil, d.RedeliveryTimeout != nil,
		d.MaxRedeliveries != nil, d.IdleReap != nil,
	}

	for _, set := range present {
		if set {
			return true
		}
	}

	return false
}

func setString(src, dst *string) {
	if src != nil {
		*dst = *src
	}
}

func setInt(src, dst *int) {
	if src != nil {
		*dst = *src
	}
}

func setInt64(src, dst *int64) {
	if src != nil {
		*dst = *src
	}
}

func setBool(src, dst *bool) {
	if src != nil {
		*dst = *src
	}
}

func setFloat64(src, dst *float64) {
	if src != nil {
		*dst = *src
	}
}

func setDuration(src, dst *time.Duration) {
	if src != nil {
		*dst = *src
	}
}
