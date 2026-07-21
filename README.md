# Overview

Multi-tenant cache server in Go. Goal: **Redis-protocol-compatible** cache with **per-tenant** memory limits, eviction, and **configurable intra-node sharding**.

**Wire protocol: RESP2.** Use `redis-cli` or any Redis client.

Docs: [docs/architecture.md](docs/architecture.md) · [docs/decisions.md](docs/decisions.md) · [plan.md](plan.md)

## Status (M7 — security & connection controls)

| Working | Later |
|---------|--------|
| `AUTH <Name> <Password>` → tenant bind | Full ACL |
| Per-tenant isolated keyspaces + limits | Observability (M8) |
| String/TTL + Pub/Sub + persistence modes | GKE (M9) |
| Eviction policies (LRU/LFU/random/FIFO/volatile-*) | |
| Security profiles (`local` / `protected`), TLS, max clients | |
| Command denylist (`DenyCommands`) | |
| `INFO tenants` per-tenant stats | |

**Protected profile (default for object configs):** data and Pub/Sub require **`AUTH`**. Unauthenticated allowlist: `AUTH`, `PING`, `ECHO`, `QUIT`, `COMMAND`, `INFO`.  
**Local profile:** optional open data path via auto-bind to the first tenant (`RequireAuth: false`).  
Bare tenant-array `config.json` still requires AUTH (backward compatible).

While subscribed, only `(P)SUBSCRIBE` / `(P)UNSUBSCRIBE` / `PING` / `QUIT` are allowed (Redis subscribe-mode).

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
| `MaxClients` | Max simultaneous AUTH-bound connections for this tenant (`0` = unlimited) |

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
# Pub/Sub (two terminals): AUTH then SUBSCRIBE / PUBLISH
# second tenant (isolated keyspace)
redis-cli -p 9001 AUTH App2 secret2
redis-cli -p 9001 GET mykey   # empty/nil — different tenant
```

## Persistence

Server config may be a tenant array (persistence `none`) or:

```json
{
  "Persistence": {
    "Mode": "snapshot",
    "Dir": "data",
    "AOFFsync": "everysec",
    "SnapshotIntervalSec": 0
  },
  "Tenants": [ ... ]
}
```

| Mode | RPO (approx) |
|------|----------------|
| `none` | process loss loses all data |
| `snapshot` | last `SAVE`/`BGSAVE` (or interval) |
| `aof` | last fsync (`always` / `everysec` ≈1s / `no`) |
| `snapshot+aof` | snapshot base + AOF delta since last SAVE; SAVE writes a new snapshot and compacts AOF to the concurrent-write catchup buffer (not a blind truncate) |

Commands (AUTH required): `SAVE`, `BGSAVE`, `LASTSAVE`, `FLUSHDB` (current tenant).  
Corruption: invalid snapshot magic/version/CRC **refuses load**. Unknown tenant names in snapshot/AOF are skipped.  
Ready-made configs: `configs/persist-none.json`, `configs/persist-snapshot.json`, `configs/persist-aof.json`, `configs/persist-snapshot-aof.json` (each uses its own `data-*` dir). Also `config.persist.example.json`.

## Security (M7)

Object-form config may include a `Security` section:

```json
{
  "Security": {
    "Profile": "protected",
    "RequireAuth": true,
    "MaxClients": 1000,
    "IdleTimeoutSec": 300,
    "DenyCommands": ["FLUSHDB"],
    "TLSCertFile": "certs/server.pem",
    "TLSKeyFile": "certs/server.key",
    "TLSMinVersion": "1.2"
  },
  "Tenants": [ ... ]
}
```

| Field | Meaning |
|-------|---------|
| `Profile` | `local` or `protected` (object-form default: `protected`) |
| `RequireAuth` | Override profile default; `false` auto-binds first tenant for data commands |
| `MaxClients` | Global connection limit (`0` = unlimited). If a tenant’s `MaxClients` is higher, it is **capped** to this value at load (warning logged). |
| `IdleTimeoutSec` | Close idle connections (`0` = none) |
| `DenyCommands` | Disabled commands even when authenticated (`AUTH`/`PING`/`QUIT` never deniable) |
| `TLSCertFile` / `TLSKeyFile` | Enable TLS when both set |
| `TLSClientCA` | Optional mTLS client CA PEM |
| `TLSMinVersion` | `1.2` (default) or `1.3` |

Ready-made: `configs/security-local.json`, `configs/security-protected.json`, `config.security.example.json`.

### TLS quickstart

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:2048 -keyout certs/server.key -out certs/server.pem \
  -days 365 -nodes -subj "/CN=localhost"
# set TLSCertFile/TLSKeyFile in config, then:
./go_cache -addr :9001 -config config.security.example.json
redis-cli --tls --insecure -p 9001 PING
redis-cli --tls --insecure -p 9001 AUTH App1 secret1
```

## Tests

```bash
go test ./...
go test ./internal/store -v
go test ./internal/command -run String -v
go test ./internal/command -run Policy -v
go test ./internal/server -run 'TLS|MaxClients|Idle|Protected' -v
go test ./internal/server -run GoRedis -v
# go-redis Pub/Sub integration (Subscribe/Publish, PSubscribe, isolation)
go test ./internal/server -run GoRedisPubSub -v
```

## Layout

```text
cmd/cache-custom/     # main + config (+ legacy LRU sources behind //go:build legacy)
internal/protocol/    # RESP2
internal/command/     # registry, AUTH, string commands
internal/server/      # TCP RESP server (per-conn AUTH context)
internal/store/       # sharded string store per tenant
internal/tenant/      # tenant registry + authenticate
internal/pubsub/      # per-tenant Pub/Sub hubs
docs/
```
