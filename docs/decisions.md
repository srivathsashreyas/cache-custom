# Decision log (Milestone 0)

Decisions locked for architecture freeze. Newer entries override older ones only when explicitly superseded.

Format: **ID**, date, status, context, decision, consequences.

---

## D001 — Wire protocol

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Existing server uses a custom text protocol (`tenantId CMD key …`). Real clients expect Redis RESP. |
| **Decision** | **RESP2 only.** Drop the custom protocol; breaking change is acceptable. |
| **Consequences** | M1 implements RESP encode/decode and a Redis-shaped command loop. No dual-protocol migration layer. RESP3 deferred. |

---

## D002 — Tenant binding via AUTH

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Need multi-tenant isolation compatible with standard Redis clients. |
| **Decision** | **`AUTH <username> <password>`** where **username identifies the tenant** and password is that tenant’s secret. |
| **Consequences** | Connection-scoped tenant after auth. No per-command tenant id. Config must define tenant username + secret. |

---

## D003 — No admin user (initial)

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Need control plane for tenants without overbuilding ACL. |
| **Decision** | **No admin user** for now. Tenant lifecycle and limits come from **config file**. |
| **Consequences** | No privileged Redis user that can see all tenants. Admin APIs (if any) are later and out-of-band. Dangerous global ops stay config/process level. |

---

## D004 — Process model: multi-tenant single process

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Could run one pod per tenant or many tenants per process. |
| **Decision** | **One process hosts many tenants.** Pod-per-tenant is supported only by deploying with a **one-tenant config**, not a separate code path. |
| **Consequences** | Software isolation (keyspace, memory, pub/sub, auth) is mandatory. Ops can still dedicate hardware via config. |

---

## D005 — Key model

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Internal designs sometimes use uint64 key ids; Redis clients use arbitrary byte keys. |
| **Decision** | Public API uses **opaque binary-safe string/byte keys**. Drop uint64 key ids from the public surface. |
| **Consequences** | Store and RESP handlers treat keys as `[]byte`/string. Any internal indexing is an implementation detail. |

---

## D006 — Per-tenant memory and eviction

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Primary product differentiator vs shared Redis DB/namespace. |
| **Decision** | Keep **per-tenant `maxmemory`** and **per-tenant eviction policy** (LRU/LFU minimum). Per-key TTL with tenant **max TTL ceiling**. |
| **Consequences** | Eviction and stats are tenant-scoped. Memory accounting must be defined per tenant. Neighbor tenants must not absorb each other’s pressure. |

---

## D007 — Persistence modes

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Need durability options without requiring full Redis file compatibility. Industry practice (Redis/Valkey): RDB snapshots, AOF logs, or both/hybrid. |
| **Decision** | Config selects one of: **`none`**, **`snapshot`**, **`aof`**, **`snapshot+aof`** (hybrid, Redis-like). Dev default recommendation: **`none`**. |
| **Consequences** | Persistence milestone implements four modes and documents RPO. Formats may be custom; semantics matter more than Redis byte compatibility. |

---

## D008 — Sharding

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Need multi-core scale on one node now; multi-node scale later. |
| **Decision** | **v1: intra-node sharding** (partition maps/locks within each tenant for concurrency). **Later: inter-node tenant-aware sharding** (place tenants on nodes). No Redis Cluster protocol for now. |
| **Consequences** | Store design must not assume a single mutex per tenant for all keys. Distributed routing and rebalance are post-v1. |

---

## D009 — Pub/Sub before deploy

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Context** | Original plan placed Pub/Sub after GKE (T3). Product preference: basic commands + Pub/Sub before deployment milestone. |
| **Decision** | **Pub/Sub is on the pre-GKE critical path.** Commands: `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, plus basic `PUBSUB` introspection as practical. |
| **Consequences** | Plan reorders milestones. Pub/Sub is tenant-scoped, single-node, in-process, non-durable messages, concurrent-safe. |

---

## D010 — Explicit non-goals

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Decision** | Out of scope until explicitly scheduled: full Redis parity, Lua/Functions, MULTI/EXEC/WATCH, Streams, modules, byte-identical RDB/AOF, Redis Cluster API, multi-region active-active. |
| **Consequences** | Unsupported commands return clear errors. Scope reviews reject these unless a new decision supersedes this entry. |

---

## D011 — M0 artifact scope

| | |
|--|--|
| **Date** | 2026-07-10 |
| **Status** | Accepted |
| **Decision** | M0 delivers **docs + decision log + updated plan only**. Go package skeleton starts with **M1**. |
| **Consequences** | No large code restructure in M0. M1 creates `internal/*` layout from architecture.md. |

---

## Supersession

None yet. To change a decision, add a new ID that references the old one and set the old status to **Superseded**.
