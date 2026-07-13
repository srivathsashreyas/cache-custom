# Overview

Multi-tenant cache server in Go. Goal: **Redis-protocol-compatible** cache with **per-tenant** memory limits, eviction, and **configurable intra-node sharding**.

**Wire protocol: RESP2.** Use `redis-cli` or any Redis client.

Docs: [docs/architecture.md](docs/architecture.md) · [docs/decisions.md](docs/decisions.md) · [plan.md](plan.md)

## Status (M2 — strings + sharded store)

| Working | Later |
|---------|--------|
| `PING`, `ECHO`, `QUIT`, `COMMAND`, `INFO` | Multi-tenant `AUTH` (M3) |
| `GET`/`SET`/`DEL`/`EXISTS`, `MGET`/`MSET` | Pub/Sub |
| `INCR`/`DECR`/`INCRBY`/`DECRBY` | Persistence modes |
| `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`/`PERSIST` | GKE |
| `TYPE`, `DBSIZE` | |
| Lazy **and** periodic TTL expiry | |
| 3 per-tenant sharding strategies | |

M2 serves a **default keyspace** from the **first** tenant in `config.json`. Full AUTH multi-tenancy is M3.

## Build and run

```bash
go build -o go_cache ./cmd/cache-custom
./go_cache -addr :9001 -config config.json
```

Go **1.22+**.

## Tenant config (`config.json`)

```json
[
  {
    "Name": "App1",
    "AppId": 1,
    "MaxMemory": 1048576,
    "Lru": true,
    "Lfu": false,
    "MaxTTL": 3600,
    "ShardCount": 4,
    "ShardingStrategy": 1
  }
]
```

| Field | Meaning |
|-------|---------|
| `MaxMemory` | Tenant memory budget (bytes) |
| `MaxTTL` | Max per-key TTL in **seconds** (0 = no ceiling) |
| `ShardCount` | Intra-node shards (default 4 if omitted/0) |
| `ShardingStrategy` | `1` / `2` / `3` (see below) |
| `Lru` / `Lfu` | Legacy flags (only one true); eviction order is LRU in the new store for M2 |

### Sharding strategies (per tenant; “global” = within tenant)

1. **Global eviction track** — sharded maps; eviction order is tenant-wide against `MaxMemory`.
2. **Steal** — when tenant `MaxMemory` is hit, evict from the target shard first; if empty, steal victims from other shards.
3. **Per-shard budget** — each shard ≈ `MaxMemory/N`; local eviction only; OOM if the entry cannot fit in that shard.

## Talk to the server

```bash
redis-cli -p 9001 PING
redis-cli -p 9001 SET mykey hello
redis-cli -p 9001 GET mykey
redis-cli -p 9001 SET t val PX 500
redis-cli -p 9001 TTL t
redis-cli -p 9001 INCR counter
redis-cli -p 9001 MSET a 1 b 2
redis-cli -p 9001 MGET a b missing
```

## Tests

```bash
go test ./...
go test ./internal/store -v
go test ./internal/command -run String -v
go test ./internal/server -run GoRedis -v
```

## Layout

```text
cmd/cache-custom/     # main + config (+ legacy LRU sources behind //go:build legacy)
internal/protocol/    # RESP2
internal/command/     # registry + connectivity + string commands
internal/server/      # TCP RESP server
internal/store/       # sharded string store, TTL, eviction strategies
docs/
```
