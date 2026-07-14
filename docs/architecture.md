# Architecture — Multi-Tenant Redis-Compatible Cache

**Status:** Milestone 0 freeze  
**Audience:** implementers starting at M1  
**Related:** [decisions.md](./decisions.md), [plan.md](../plan.md)

This document freezes the target architecture. It does not implement features. Implementation order and acceptance criteria live in `plan.md`.

---

## 1. Product shape

A single Go process speaks **RESP** on TCP and hosts **many tenants**. Each tenant has:

- Isolated keyspace
- Isolated Pub/Sub channel namespace
- Own `maxmemory` and eviction policy (core differentiator vs Redis/Valkey namespaces)
- Own max TTL ceiling
- Own credentials (`AUTH username password`, username = tenant)

**Not goals for early releases:** full Redis command parity, Lua, MULTI/EXEC, Streams, modules, byte-identical RDB, Redis Cluster protocol, multi-region active-active.

**Pod-per-tenant** is an ops choice, not a special code path: run the same binary with a config that defines one tenant.

---

## 2. Process model

```text
                    ┌─────────────────────────────────────────┐
  clients (RESP) ──►│  TCP accept loop                        │
                    │    └── per-connection goroutine         │
                    │         decode RESP → dispatch command  │
                    │         resolve tenant from conn state  │
                    │         execute under tenant store      │
                    └─────────────────────────────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
     Tenant A                    Tenant B                    Tenant C
   sharded maps                sharded maps                sharded maps
   LRU/LFU + TTL               LRU/LFU + TTL               LRU/LFU + TTL
   pub/sub hub                 pub/sub hub                 pub/sub hub
```

### Connections

- One goroutine (or equivalent session) per client connection.
- Connection state includes: authenticated tenant (or unauthenticated), Pub/Sub mode flags, client name/id for ops later.
- Unauthenticated connections may only run a minimal set (e.g. `AUTH`, `PING`, `QUIT`) once auth is required by config profile; exact enforcement lands with security work.
- Idle timeouts and max clients (global, later per-tenant) are planned; not required for first RESP bring-up.

### Concurrency (v1 requirement)

- **Intra-node sharding** is required for v1: partition each tenant’s data (and locks) so concurrent commands on different keys do not serialize on a single global or single per-tenant mutex.
- Suggested approach (detail at implementation): N shards per tenant (power of two), key → shard via hash; each shard owns map + eviction structures + mutex (or equivalent).
- Pub/Sub hub per tenant must be safe under concurrent `PUBLISH` / subscribe / unsubscribe.
- No distributed (cross-node) sharding in v1.

### Workers

- Prefer simple connection-owned execution for M1–M2.
- Optional shared worker pools later if profiling warrants; not an M0 requirement.

---

## 3. Tenant identity and auth

| Item | Decision |
|------|----------|
| Binding | Redis-style `AUTH <username> <password>` |
| Username | Tenant identifier (name/id string as configured) |
| Password | Per-tenant secret |
| Admin user | **None** for now |
| Control plane | Config file (and later optional admin APIs — out of early scope) |
| DB/`SELECT` | Not used as multi-tenancy; single logical DB per tenant keyspace |

After successful `AUTH`, all data and Pub/Sub commands run in that tenant’s scope. Cross-tenant access is a hard error / invisible keyspace (no leakage).

Dynamic tenant create/update at runtime is desirable later; M0 assumes **boot-time config** is sufficient for the first milestones.

---

## 4. Key model

- Public keys and values are **opaque, binary-safe byte strings** (Redis semantics).
- **No** public `uint64` key-id scheme.
- Internal representation may use more efficient structures; the wire and command API stay string/bytes.

---

## 5. Memory, TTL, eviction

### Per-tenant limits (required differentiator)

Each tenant configures at least:

- `maxmemory` (bytes)
- Eviction policy per tenant: Redis-like names (`noeviction`, `allkeys-lru`/`lfu`/`random`, `volatile-*`) plus extensions `allkeys-fifo` / `volatile-fifo` (FIFO is not a stock Redis policy; random is)
- Max TTL ceiling (tenant-wide upper bound on per-key TTL)

### Per-key TTL

- Keys may carry individual TTLs (`EXPIRE` / `SET EX` / `PX`, etc.).
- **Lazy expiry** on access (GET/SET/… treat expired keys as missing and delete them).
- **Periodic (active) expiry** via a background worker + min-heap of deadlines (not lazy-only).
- Tenant max TTL clamps requested TTLs.

### Memory accounting

- Charge `len(key) + len(value) + EntryOverhead` (currently **24** bytes) toward tenant `maxmemory` (and per-shard budget under strategy 3).
- Global process guardrails (sum of tenants vs host) come with later ops milestones.

### Eviction vs TTL

- TTL expiry removes keys independently of eviction policy.
- When `maxmemory` is exceeded on write, the tenant’s eviction policy runs (or `noeviction` returns OOM-style error).

---

## 6. Protocol

- **RESP2 only** for v1 (arrays, bulk strings, simple strings, errors, integers, nulls).
- RESP3 / full `HELLO` negotiation deferred unless a required client forces it.
- Custom line protocol (`tenantId GET key`) is **removed**; break freely.
- Command registry: name → handler, arity checks, Redis-like error strings (`-ERR`, `-WRONGTYPE`, …).

---

## 7. Command tiers and early allowlist

Tiers are product contracts. Pre-deploy “usable cache + messaging” targets **T0 + Pub/Sub**, then platform work (persistence, metrics, GKE).

### T0 — Connectivity + strings/keys

| Group | Commands (initial allowlist) |
|-------|------------------------------|
| Connectivity | `PING`, `ECHO`, `QUIT` |
| Meta (stubs OK early) | `COMMAND` (list supported), `INFO` (minimal sections) |
| Auth | `AUTH` (username + password) |
| Strings / keys | `GET`, `SET` (prioritize `EX`, `PX`, `NX`, `XX`), `DEL`, `EXISTS` |
| Multi / counters | `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| TTL | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` |
| Introspection | `TYPE`, `DBSIZE` (tenant-scoped) |

Unsupported commands return a clear error (not silent no-op).

### Pub/Sub (before GKE; still single-node)

| Commands |
|----------|
| `SUBSCRIBE`, `UNSUBSCRIBE` |
| `PSUBSCRIBE`, `PUNSUBSCRIBE` |
| `PUBLISH` |
| `PUBSUB` basics (`CHANNELS`, `NUMSUB`, `NUMPAT`) as practical |

**Rules:**

- Channels and patterns are **tenant-scoped**.
- In-process fan-out only on one node (v1).
- Messages are not persisted; if no subscriber, publish completes with receiver count 0 (Redis-like).
- After entering subscribe mode, connection command rules follow Redis conventions (pub/sub commands + limited others as we document).

### Later tiers (not pre-deploy)

- **T1 platform:** fuller eviction matrix, persistence modes, TLS/auth hardening, metrics, GKE.
- **T2:** hashes, lists, sets, sorted sets.
- **T3 remainder:** MULTI/EXEC, Lua (explicitly deferred).
- **T4:** HA replication, inter-node tenant-aware sharding.

---

## 8. Persistence (direction only)

Configurable modes (Redis-like product surface):

| Mode | Meaning |
|------|---------|
| `none` | No durability; pure cache. Sensible **default for dev**. |
| `snapshot` | Periodic/point-in-time dump (RDB-style). Loss since last snapshot possible. |
| `aof` | Append-only log of writes; replay on startup. |
| `snapshot+aof` | Hybrid: snapshot base + log of subsequent writes (Redis RDB+AOF / RDB-preamble style). |

Format need **not** be byte-compatible with Redis RDB/AOF files. Semantic durability and multi-tenant restore matter more.

Default recommendation: `none` in development; document production toward `aof` or `snapshot+aof` depending on RPO needs.

HA/replication is **out of v1**; persistence is single-node disk recovery.

---

## 9. Sharding strategy

| Scope | v1 | Later |
|-------|----|--------|
| **Intra-node** | **Yes** — partition tenant data/locks for multi-core concurrency | Refine based on benchmarks |
| **Inter-node** | **No** | **Tenant-aware** placement (tenant lives on a shard/node); rebalance later |
| **Redis Cluster API** | No | Only if product later requires cluster-mode clients |
| **Pod-per-tenant** | Ops pattern via one-tenant config | Unchanged |

Intra-node sharding must preserve tenant isolation: never share a data shard across tenants in a way that allows cross-tenant visibility or coupled eviction.

### Per-tenant sharding strategies (config)

Keys hash to a shard. **Global** always means *within one tenant* (not across tenants). Config field `ShardingStrategy`:

| ID | Name | Behavior |
|----|------|----------|
| **1** | Global eviction track | Data is sharded; eviction order is tracked **tenant-wide**. When tenant `maxmemory` is exceeded, evict by global LRU until the write fits. |
| **2** | Steal across shards | Eviction runs only when tenant `maxmemory` is hit. Prefer evicting from the **target shard**; if that shard has no victims left, **steal** (evict) from other shards until there is room for the entry. |
| **3** | Per-shard budget | Each shard gets roughly `maxmemory / ShardCount`. Evict **only inside the target shard** when its budget is exceeded. If the entry cannot fit in that shard after local eviction, **reject** (OOM) even if other shards have free space. |

`ShardCount` is configurable per tenant (default 4).

---

## 10. Target repo layout (create at M1)

Packages are planned now; empty modules are created when M1 starts.

```text
cmd/
  cache-custom/          # main (existing; will thin out)
internal/
  protocol/              # RESP encode/decode
  server/                # TCP, connections, pipeline loop
  command/               # registry + handlers
  store/                 # keyspace, strings, TTL, eviction, shards
  tenant/                # tenant registry, auth, limits
  pubsub/                # per-tenant hubs
  persist/               # snapshot / aof / hybrid
  metrics/               # later
deploy/                  # Helm/Kustomize later
docs/
  architecture.md        # this file
  decisions.md
plan.md
```

Public API surface for library consumers is not a goal; this is a server binary first.

---

## 11. Compatibility tier summary

| Tier | Name | Client expectation |
|------|------|--------------------|
| **T0** | Wire + strings | RESP clients: auth, GET/SET/DEL, TTL |
| **T0+** | + messaging | Tenant-scoped Pub/Sub on single node |
| **T1** | Cache platform | Eviction fairness, persistence modes, security, metrics, GKE |
| **T2** | Data structures | Hash/list/set/zset |
| **T3** | Scripts / tx | Deferred (Lua, MULTI) |
| **T4** | Distributed | HA + inter-node tenant sharding |

**Pre-GKE bar:** T0 + Pub/Sub (+ enough tenancy/memory that multi-tenant use is real).  
**1.0-oriented bar:** that path plus persistence options, basic security, observability, deploy artifacts, intra-node sharding proven under load.

---

## 12. Non-goals (until explicitly pulled in)

- Full Redis command parity  
- Lua / Redis Functions  
- `MULTI` / `EXEC` / `WATCH`  
- Streams  
- Redis modules API  
- Byte-identical Redis RDB/AOF  
- Redis Cluster protocol (`MOVED`/`ASK`, 16384 slots)  
- Multi-region active-active  
- Full RESP3 unless a required client demands it  

---

## 13. Implementation order (see plan.md)

High level after M0:

1. RESP foundation  
2. String store + key semantics  
3. Multi-tenancy + AUTH  
4. Memory/eviction hardening (incl. intra-node shards if not already in store)  
5. Pub/Sub  
6. Persistence modes  
7. Security / observability as needed  
8. GKE deployability  
9. Performance baselines  
10. T2 structures and beyond  

Exact milestone IDs and acceptance criteria: [plan.md](../plan.md).
