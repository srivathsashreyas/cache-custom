# Overview

Multi-tenant cache server in Go. Goal: **Redis-protocol-compatible** cache with **per-tenant** memory limits, eviction, and **configurable intra-node sharding**.

**Wire protocol: RESP2.** Use `redis-cli` or any Redis client.

Docs: [docs/architecture.md](docs/architecture.md) · [docs/decisions.md](docs/decisions.md) · [plan.md](plan.md)

## Status (M3 — multi-tenant AUTH)

| Working | Later |
|---------|--------|
| `AUTH <Name> <Password>` → tenant bind | Full ACL |
| Per-tenant isolated keyspaces + limits | Pub/Sub |
| String/TTL commands (require AUTH) | Persistence modes |
| Lazy + periodic expiry; 3 shard strategies | GKE |
| Per-tenant eviction policies (LRU/LFU/random/FIFO/…) | |
| `INFO tenants` per-tenant stats | |

Data commands require **`AUTH`**. Connectivity (`PING`/`ECHO`/`QUIT`/`COMMAND`/`INFO`) works without AUTH.

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
    "Password": "secret1",
    "AppId": 1,
    "MaxMemory": 1048576,
    "MaxTTL": 3600,
    "ShardCount": 4,
    "ShardingStrategy": 1
  }
]
```

| Field | Meaning |
|-------|---------|
| `Name` | AUTH **username** (tenant id) |
| `Password` | AUTH secret (**required**) |
| `MaxMemory` | Tenant memory budget (bytes) |
| `MaxTTL` | Max per-key TTL in **seconds** (0 = no ceiling) |
| `ShardCount` | Intra-node shards (default 4 if omitted/0) |
| `ShardingStrategy` | `1` / `2` / `3` (see below) |
| `EvictionPolicy` | e.g. `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `allkeys-fifo`, `noeviction`, `volatile-*` |
| `Disabled` | If true, AUTH rejected for this tenant |
| `Lru` / `Lfu` | Legacy flags; eviction is LRU until M4 |

### Sharding strategies (per tenant; “global” = within tenant)

1. **Global eviction track** — sharded maps; eviction order is tenant-wide against `MaxMemory`.
2. **Steal** — when tenant `MaxMemory` is hit, evict from the target shard first; if empty, steal victims from other shards.
3. **Per-shard budget** — each shard ≈ `MaxMemory/N`; local eviction only; OOM if the entry cannot fit in that shard.

## Talk to the server

```bash
redis-cli -p 9001 PING
redis-cli -p 9001 AUTH App1 secret1
redis-cli -p 9001 SET mykey hello
redis-cli -p 9001 GET mykey
redis-cli -p 9001 INFO tenants
# second tenant (isolated keyspace)
redis-cli -p 9001 AUTH App2 secret2
redis-cli -p 9001 GET mykey   # empty/nil — different tenant
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
internal/command/     # registry, AUTH, string commands
internal/server/      # TCP RESP server (per-conn AUTH context)
internal/store/       # sharded string store per tenant
internal/tenant/      # tenant registry + authenticate
docs/
```
