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

type Settings struct {
	HistorySize    int
	HistoryTTL     time.Duration
	AllowPublish   bool
	MaxSubscribers int
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
	HistorySize    *int           `yaml:"history_size"`
	HistoryTTL     *time.Duration `yaml:"history_ttl"`
	AllowPublish   *bool          `yaml:"allow_publish"`
	MaxSubscribers *int           `yaml:"max_subscribers"`
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
	return s, nil
}
