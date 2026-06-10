// Package namespace maps channel name prefixes to per-class settings.
// Channels follow the syntax "ns:rest"; a name without a prefix belongs to
// the default namespace. Resolution happens once per channel lifecycle
// (creation, subscribe), never per message.
package namespace

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

var ErrInvalidConfig = errors.New("namespace: invalid config")

type QoSLevel int

const (
	AtMostOnce QoSLevel = iota
	AtLeastOnce
)

type FanoutMode int

const (
	// Broadcast delivers every publication to every subscriber.
	Broadcast FanoutMode = iota
	// RoundRobin delivers each publication to exactly one subscriber,
	// rotating through them: a work queue.
	RoundRobin
	// Priority delivers to the highest-priority cohort of subscribers
	// only; lower cohorts are standby and take over on disconnect.
	Priority
)

type Settings struct {
	HistorySize    int
	HistoryTTL     time.Duration
	AllowPublish   bool
	MaxSubscribers int

	// QoS selects the delivery guarantee consumers may request in this
	// namespace. AtLeastOnce requires history, which backs redelivery.
	QoS               QoSLevel
	RedeliveryTimeout time.Duration
	MaxRedeliveries   int

	// Fanout selects the delivery strategy for channels in this
	// namespace.
	Fanout FanoutMode
}

type Config struct {
	Default    Settings
	Namespaces map[string]Settings
	Strict     bool
}

type Registry struct {
	namespaces map[string]Settings
	def        Settings
	strict     bool
}

func NewRegistry(cfg Config) *Registry {
	return &Registry{
		namespaces: cfg.Namespaces,
		def:        cfg.Default,
		strict:     cfg.Strict,
	}
}

// All returns every configured namespace's settings, default included.
// Used for engine startup decisions (e.g. sweep cadence), not per-message.
func (r *Registry) All() []Settings {
	out := make([]Settings, 0, len(r.namespaces)+1)
	out = append(out, r.def)
	for _, s := range r.namespaces {
		out = append(out, s)
	}
	return out
}

// Split separates a channel name into its namespace prefix and remainder.
// Substring slicing only, no allocation.
func Split(channel string) (ns, rest string) {
	if idx := strings.IndexByte(channel, ':'); idx >= 0 {
		return channel[:idx], channel[idx+1:]
	}
	return "", channel
}

// Resolve returns the settings governing the channel. ok is false only for
// an unknown namespace under strict mode; lenient registries fall through
// to the default settings.
func (r *Registry) Resolve(channel string) (Settings, bool) {
	ns, _ := Split(channel)
	if ns == "" {
		return r.def, true
	}
	if s, ok := r.namespaces[ns]; ok {
		return s, true
	}
	if r.strict {
		return Settings{}, false
	}
	return r.def, true
}

type settingsYAML struct {
	HistorySize       *int           `yaml:"history_size"`
	HistoryTTL        *time.Duration `yaml:"history_ttl"`
	AllowPublish      *bool          `yaml:"allow_publish"`
	MaxSubscribers    *int           `yaml:"max_subscribers"`
	QoS               *string        `yaml:"qos"`
	Fanout            *string        `yaml:"fanout_mode"`
	RedeliveryTimeout *time.Duration `yaml:"redelivery_timeout"`
	MaxRedeliveries   *int           `yaml:"max_redeliveries"`
}

type configYAML struct {
	Strict     bool                    `yaml:"strict"`
	Default    settingsYAML            `yaml:"default"`
	Namespaces map[string]settingsYAML `yaml:"namespaces"`
}

// LoadFile parses a YAML namespace config. Unknown keys and invalid values
// fail loudly; a misspelled setting must never silently become a default.
func LoadFile(path string) (*Registry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("namespace: read config: %w", err)
	}

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)

	var cfg configYAML
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("namespace: parse config %s: %w", path, err)
	}

	def, err := toSettings("default", cfg.Default)
	if err != nil {
		return nil, err
	}

	namespaces := make(map[string]Settings, len(cfg.Namespaces))
	for name, raw := range cfg.Namespaces {
		if name == "" {
			return nil, fmt.Errorf("%w: empty namespace name", ErrInvalidConfig)
		}
		if strings.Contains(name, ":") {
			return nil, fmt.Errorf("%w: namespace %q must not contain ':'", ErrInvalidConfig, name)
		}
		s, err := toSettings(name, raw)
		if err != nil {
			return nil, err
		}
		namespaces[name] = s
	}

	return NewRegistry(Config{
		Default:    def,
		Namespaces: namespaces,
		Strict:     cfg.Strict,
	}), nil
}

func toSettings(name string, raw settingsYAML) (Settings, error) {
	s := Settings{AllowPublish: true}
	if raw.HistorySize != nil {
		if *raw.HistorySize < 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q history_size must be >= 0", ErrInvalidConfig, name)
		}
		s.HistorySize = *raw.HistorySize
	}
	if raw.HistoryTTL != nil {
		if *raw.HistoryTTL < 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q history_ttl must be >= 0", ErrInvalidConfig, name)
		}
		s.HistoryTTL = *raw.HistoryTTL
	}
	if raw.AllowPublish != nil {
		s.AllowPublish = *raw.AllowPublish
	}
	if raw.MaxSubscribers != nil {
		if *raw.MaxSubscribers < 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q max_subscribers must be >= 0", ErrInvalidConfig, name)
		}
		s.MaxSubscribers = *raw.MaxSubscribers
	}
	if raw.QoS != nil {
		switch *raw.QoS {
		case "at-most-once":
			s.QoS = AtMostOnce
		case "at-least-once":
			s.QoS = AtLeastOnce
		default:
			return Settings{}, fmt.Errorf("%w: namespace %q qos must be at-most-once or at-least-once, got %q", ErrInvalidConfig, name, *raw.QoS)
		}
	}
	if raw.Fanout != nil {
		switch *raw.Fanout {
		case "broadcast":
			s.Fanout = Broadcast
		case "round_robin":
			s.Fanout = RoundRobin
		case "priority":
			s.Fanout = Priority
		default:
			return Settings{}, fmt.Errorf("%w: namespace %q fanout_mode must be broadcast, round_robin, or priority, got %q", ErrInvalidConfig, name, *raw.Fanout)
		}
	}
	if raw.RedeliveryTimeout != nil {
		if *raw.RedeliveryTimeout <= 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q redelivery_timeout must be > 0", ErrInvalidConfig, name)
		}
		s.RedeliveryTimeout = *raw.RedeliveryTimeout
	}
	if raw.MaxRedeliveries != nil {
		if *raw.MaxRedeliveries < 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q max_redeliveries must be >= 0", ErrInvalidConfig, name)
		}
		s.MaxRedeliveries = *raw.MaxRedeliveries
	}
	if s.QoS == AtLeastOnce {
		if s.HistorySize <= 0 {
			return Settings{}, fmt.Errorf("%w: namespace %q at-least-once qos requires history_size > 0", ErrInvalidConfig, name)
		}
		if s.RedeliveryTimeout == 0 {
			s.RedeliveryTimeout = 30 * time.Second
		}
		if s.MaxRedeliveries == 0 {
			s.MaxRedeliveries = 3
		}
	}
	return s, nil
}
