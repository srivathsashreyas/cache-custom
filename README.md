# Overview

Multi-tenant cache server in Go. The long-term goal is a **Redis-protocol-compatible** cache with **per-tenant** memory limits and eviction policies (a differentiator vs Redis/Valkey namespaces).

**Current wire protocol: RESP2** (same family as Redis). Connect with `redis-cli` or any Redis client.

Architecture freeze and milestones: [docs/architecture.md](docs/architecture.md), [docs/decisions.md](docs/decisions.md), [plan.md](plan.md).

## Status (M1 — RESP foundation)

| Working now | Not yet over RESP |
|-------------|-------------------|
| `PING`, `ECHO`, `QUIT` | `GET` / `SET` / `DEL` (cache still in-tree for later milestones) |
| `COMMAND`, `INFO` (minimal) | Multi-tenant `AUTH` |
| Pipelining, inline + RESP arrays | Pub/Sub, persistence |

## Build and run

```bash
# from repo root
go build -o go_cache ./cmd/cache-custom

# config.json must be readable (cwd or pass -config)
./go_cache
# or:
./go_cache -addr :9001 -config config.json
```

Requires Go **1.22+** (see `go.mod`).

## Tenant config (`config.json`)

Still used to load in-process tenant caches for upcoming milestones:

```json
[
  {
    "Name": "App1",
    "AppId": 1,
    "MaxMemory": 20,
    "Lru": true,
    "Lfu": false,
    "MaxTTL": 3600
  }
]
```

Only one of `Lru` / `Lfu` may be true per tenant.

## Talk to the server

### redis-cli

```bash
redis-cli -p 9001 PING
# PONG

redis-cli -p 9001 PING hello
# hello

redis-cli -p 9001 ECHO "hi there"
# hi there

redis-cli -p 9001 COMMAND COUNT
# (integer) 5

redis-cli -p 9001 INFO server

redis-cli -p 9001 QUIT
```

Interactive:

```bash
redis-cli -p 9001
> PING
PONG
> ECHO world
world
```

### Manual RESP / inline (optional)

```bash
# inline (telnet-style)
printf 'PING\r\n' | nc localhost 9001

# RESP array: *1\r\n$4\r\nPING\r\n
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 9001
```

## Tests

```bash
# unit + TCP integration + go-redis language client
go test ./...

# packages
go test ./internal/protocol -v
go test ./internal/command -v
go test ./internal/server -v

# language client only (github.com/redis/go-redis/v9)
go test ./internal/server -run GoRedis -v
```

M1 acceptance covers both **raw RESP/TCP** tests and a real **Go Redis client** (`TestGoRedisClient*`).

## Project layout

```text
cmd/cache-custom/     # main, existing LRU/LFU/TTL store (not yet on RESP path)
internal/protocol/    # RESP2 encode/decode
internal/command/     # command registry + M1 handlers
internal/server/      # TCP accept loop, per-connection RESP session
docs/                 # architecture + decisions
config.json
```

## Roadmap (short)

1. **Done (M1):** RESP2 + connectivity commands  
2. **Next:** string/`GET`/`SET`/TTL on RESP, then multi-tenant `AUTH`, Pub/Sub, persistence, GKE  

See [plan.md](plan.md) for the full milestone list.
