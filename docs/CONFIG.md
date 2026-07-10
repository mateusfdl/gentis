# Gentis Configuration

Every server setting lives in a single YAML file, passed with `--config`:

```sh
gentis serve --config gentis.yaml
gentis relay --config gentis.yaml
```

When `--config` is omitted, the built-in defaults apply and the auth secret is
read from `GENTIS_AUTH_HMAC_SECRET`. Unknown keys and out-of-range or
contradictory values fail loudly at startup; a misspelled key never silently
falls back to a default.

The `health` and `version` commands take no config file: `health` probes a
remote endpoint (`--addr`, `--timeout`) and `version` prints build info.

## Sections

The file has these top-level sections. `server` applies to `serve`; `relay`
applies to `relay`; the rest are shared. Any section or key may be omitted to
take its default.

### `log`
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `level` | string | `info` | `debug`, `info`, `warn`, `error` |
| `format` | string | `text` | `text` or `json` |

### `metrics`
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | serve Prometheus metrics |

### `engine`
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `shards` | int | `0` | shard count; `0` = auto, rounded to power-of-2 |
| `fanout_threshold` | int | `100000` | subscriber count that triggers parallel fanout |
| `fanout_workers` | int | `4` | parallel fanout goroutines (> 0) |
| `history_size` | int | `0` | global per-channel history ring; `0` disables |
| `history_ttl` | duration | `0` | history entry TTL; requires `history_size > 0` |

A `namespaces` section (below) overrides the global `history_*` per channel class.

### `gc`
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `pacer` | bool | `false` | enable automatic GC tuning |
| `mem_limit` | int64 | `0` | soft memory limit in bytes; `0` = none |
| `spike_gogc` | int | `400` | GOGC during activity spikes (> 0) |
| `normal_gogc` | int | `100` | GOGC during normal operation (> 0) |

### `auth`
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `hmac_secret_env` | string | `GENTIS_AUTH_HMAC_SECRET` | env var holding the HS256 secret |
| `hmac_secret` | string | `""` | inline secret (prefer the env var) |
| `disabled` | bool | `false` | accept any token without verification (dev only) |

Exactly one of a resolved secret or `disabled: true` must hold: a secret with
`disabled: true` is an error, and neither is an error. `hmac_secret` wins over
`hmac_secret_env` when both resolve.

### `websocket`
Shared by `serve` and `relay`. Empty `addr` disables the WebSocket transport.
The transport tunables (`ping_interval`, `auth_deadline`, `max_message_size`,
`max_subscriptions`, TLS) are inherited from the hosting `server`/`relay` section.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `addr` | string | `""` | listen address (host:port); empty disables |
| `read_limit` | int64 | `65536` | max message size in bytes |
| `write_timeout` | duration | `10s` | write deadline |
| `send_buffer` | int | `256` | per-session send buffer size |

### `server` (serve)
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `addr` | string | `0.0.0.0:9000` | gRPC listen address |
| `metrics_addr` | string | `:8080` | metrics/health HTTP address |
| `debug_addr` | string | `""` | pprof/debug HTTP address; empty disables |
| `arena` | bool | `false` | mmap arena session state (Linux only) |
| `max_sessions` | int | `16384` | arena session capacity (with `arena`) |
| `ping_interval` | duration | `25s` | transport keepalive ping; `0` disables |
| `auth_deadline` | duration | `30s` | close unauthenticated sessions after; `0` disables |
| `tls.cert` / `tls.key` | string | `""` | TLS pair; both or neither |
| `max_message_size` | int | `65536` | max publish payload in bytes |
| `max_subscriptions` | int | `16` | subscriptions per session; `0` = unlimited |

### `relay` (relay)
| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `addr` | string | `127.0.0.1:9001` | relay gRPC listen address |
| `upstream.addr` | string | `""` | upstream server address (**required**) |
| `upstream.auth_token` | string | `""` | token presented to the upstream |
| `upstream.tls` | bool | `false` | dial the upstream over TLS |
| `upstream.ca` | string | `""` | CA bundle for upstream TLS; empty = system roots |
| `metrics_addr` | string | `:8081` | metrics/health HTTP address |
| `reconnect.initial` | duration | `100ms` | backoff initial delay |
| `reconnect.max` | duration | `30s` | backoff maximum delay |
| `reconnect.multiplier` | float | `2.0` | backoff multiplier (> 0) |
| `reconnect.max_retries` | int | `0` | max reconnect retries; `0` = unlimited |
| `buffer_size` | int | `256` | per-session send buffer (> 0) |
| `incoming_buffer` | int | `4096` | incoming buffer from upstream (> 0) |
| `fanout_workers` | int | `4` | relay-local fanout goroutines (> 0) |
| `arena` | bool | `false` | mmap arena session state (Linux only) |
| `max_sessions` | int | `16384` | arena session capacity (with `arena`) |
| `ping_interval` | duration | `25s` | transport keepalive ping; `0` disables |
| `auth_deadline` | duration | `30s` | close unauthenticated sessions after; `0` disables |
| `tls.cert` / `tls.key` | string | `""` | TLS pair; both or neither |
| `max_message_size` | int | `65536` | max publish payload in bytes |
| `max_subscriptions` | int | `16` | subscriptions per session; `0` = unlimited |

### `namespaces`
Channel classes keyed by name prefix (`orders:...`). See
[the namespace package](../internal/namespace/namespace.go) for the per-namespace
keys (`history_size`, `history_ttl`, `allow_publish`, `max_subscribers`, `qos`,
`fanout_mode`, `allow_wildcard`, `redelivery_timeout`, `max_redeliveries`,
`idle_reap`) and the `strict` / `default` keys. A namespace file written for the
old `--config` flag remains valid: it is a subset of this schema.

## Example

```yaml
log:
  level: info
  format: json
auth:
  disabled: true
server:
  addr: 0.0.0.0:9000
  metrics_addr: :8080
websocket:
  addr: 0.0.0.0:9080
namespaces:
  chat:
    history_size: 256
    history_ttl: 10m
    max_subscribers: 5000
```

See [`gentis.yaml`](../gentis.yaml) for a working example.
