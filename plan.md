# Milestone Plan: Multi-Tenant Redis-Compatible Cache

This plan turns the current Go TCP cache into a Redis-protocol-compatible, multi-tenant, GKE-deployable system. Milestones are ordered by dependency. Each one should leave the product usable, testable, and better than the previous state. There are no timelines.

**Architecture freeze:** [docs/architecture.md](./docs/architecture.md)  
**Decision log:** [docs/decisions.md](./docs/decisions.md)

---

## Guiding principles

1. **Ship compatibility in tiers**, not full Redis parity.
2. **Keep multi-tenancy first-class** in every milestone after the protocol foundation.
3. **Every milestone has acceptance criteria** you can verify with tests and benchmarks.
4. **Do not start clustering or full HA** until single-node correctness, persistence, and tenancy are solid.
5. **Performance is continuous**, but formal performance gates appear after the core data plane works.
6. **Basic commands + Pub/Sub before GKE** — messaging is on the pre-deploy path (D009).

---

## Compatibility tiers (reference for all milestones)

Use these as product contracts, not vague goals.

| Tier | Name | Client expectation |
|------|------|--------------------|
| **T0** | Wire + strings | Common cache clients: `AUTH`, `GET`/`SET`/`DEL`, TTL |
| **T0+** | + messaging | Tenant-scoped Pub/Sub (`SUBSCRIBE` / `PSUBSCRIBE` / `PUBLISH`) on one node |
| **T1** | Cache platform | Eviction, persistence modes, security, metrics, GKE |
| **T2** | Core data structures | Hashes, lists, sets, sorted sets |
| **T3** | Transactions & scripts | MULTI/EXEC, Lua/functions (**deferred non-goals**) |
| **T4** | Distributed | HA + inter-node tenant-aware sharding |

---

## Milestone 0 — Baseline and architecture freeze

**Goal:** Lock the target architecture before large rewrites so later work does not thrash.

**Status:** Complete (docs freeze).

**Deliverables**

- [docs/architecture.md](./docs/architecture.md) — process model, tenancy, keys, memory, persistence direction, sharding, allowlists, repo layout plan.
- [docs/decisions.md](./docs/decisions.md) — decision log D001–D011.
- This plan updated for Pub/Sub-before-GKE and frozen choices.

**Frozen decisions (summary)**

| Topic | Choice |
|-------|--------|
| Protocol | RESP2 only; custom protocol dropped |
| Tenant bind | `AUTH username password` (username = tenant) |
| Admin user | None; config-file control plane |
| Process | One process, many tenants; one-tenant config = dedicated pod |
| Keys | Opaque binary-safe strings |
| Pre-deploy | T0 strings + Pub/Sub (incl. pattern subscribe) |
| Memory | Per-tenant maxmemory + eviction policy |
| Persistence | `none` \| `snapshot` \| `aof` \| `snapshot+aof` |
| Sharding | v1 intra-node; later inter-node tenant-aware |
| M0 code | None; packages start at M1 |

**Acceptance criteria**

- Team can answer T0 vs T0+ vs T1 without ambiguity.
- Public API and tenancy model are written and agreed.
- No large feature work starts that conflicts with those decisions.

**Depends on:** nothing.

---

## Milestone 1 — RESP protocol foundation

**Goal:** Speak Redis on the wire so real clients can connect, even if only a tiny command set works.

**Deliverables**

- Create package layout from architecture (`internal/protocol`, `server`, `command`, …).
- RESP2 encoder/decoder (arrays, bulk strings, simple strings, errors, integers, nulls).
- TCP server that accepts connections, reads pipelined commands, writes correct replies.
- Command registry/dispatch (name → handler, arity checks, `-ERR` style errors).
- Minimal commands:
  - `PING`, `ECHO`, `QUIT`
  - `COMMAND` (minimal list of supported commands)
  - `INFO` (server section stub is fine)
- Connection lifecycle: clean close; idle/max-clients can be stubs or simple globals.
- Integration tests using a standard Redis client library and/or `redis-cli`.

**Out of scope**

- Full RESP3.
- ACL/TLS.
- Pub/Sub push mode (M6 in this plan).
- Real multi-tenant data plane (auth may stub or no-op until M3).

**Acceptance criteria**

- `redis-cli` and at least one language client can `PING` and get a valid reply.
- Pipelining of multiple commands on one connection works.
- Malformed RESP is rejected safely without crashing the process.

**Depends on:** M0.

**Tier progress:** foundation for T0.

---

## Milestone 2 — String store and Redis key semantics

**Goal:** Redis-compatible string and key operations on an in-memory store (default tenant or light tenancy OK).

**Deliverables**

- In-memory store for string keys with binary-safe keys and values (`internal/store`).
- **Intra-node sharding** with **three per-tenant strategies** (D012):
  1. Sharded data + **global (tenant-wide)** eviction tracking against `maxmemory`
  2. Evict only when global limit hit; prefer target shard; **steal** from other shards if needed
  3. **Per-shard budget** + local eviction only; reject if the entry cannot fit in-shard
- Commands (T0 string/key set):
  - `GET`, `SET` (`EX`, `PX`, `NX`, `XX`, `KEEPTTL`), `DEL`, `EXISTS`
  - `MGET`, `MSET`
  - `INCR`, `DECR`, `INCRBY`, `DECRBY`
  - `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
  - `TYPE`, `DBSIZE`
- Per-key TTL: **lazy expiry + periodic active expiry** (D013); tenant `MaxTTL` ceiling.
- Remove public dependence on internal `uint64` key IDs.
- Unit tests for commands, expiry (lazy + periodic), and all three sharding strategies.

**Design notes**

- Memory charge: `len(key)+len(value)+EntryOverhead` (24).
- Tenant routing may still be “default tenant only” if M3 is next (first `config.json` entry).

**Acceptance criteria**

- Standard client: set with TTL, get, expire, delete, incr.
- Missing keys / TTL behavior matches T0 contract; periodic expiry reclaims untouched keys.
- Strategy 1–3 behave as specified under memory pressure.
- Custom line protocol gone from the serve path.

**Depends on:** M1.

**Tier progress:** T0 data plane (tenancy binding completes in M3).

---

## Milestone 3 — Multi-tenancy v2 (core product differentiator)

**Goal:** Production-shaped tenancy: isolated, configurable, enforceable on every command.

**Deliverables**

- Tenant entity: id/name (AUTH username), max memory, eviction policy, max TTL ceiling, secret, status.
- `AUTH username password` binds the connection to a tenant (D002).
- All data commands execute inside the bound tenant’s keyspace.
- Per-tenant:
  - Memory limit enforcement
  - Eviction policy (LRU and LFU minimum)
  - Max TTL ceiling
  - Stats: used memory, keys, hits/misses, evictions
- Control plane: **config file** create/list limits (no admin Redis user — D003).
- Isolation tests: tenant A cannot read/write B; memory pressure on A does not grow B’s dataset.
- Per-tenant sharded stores (intra-node).

**Retain from current design**

- Per-tenant isolation and configurable eviction.
- Per-tenant memory caps.

**Improve vs current design**

- AUTH-based binding instead of `tenantId` on every command.
- Per-key TTL + tenant max TTL ceiling.
- Documented memory accounting model.

**Acceptance criteria**

- Two tenants on one process with different maxmemory and different eviction policies.
- AUTH scopes all subsequent data commands.
- Config-driven tenants without code changes.
- Metrics exist per tenant for used memory, keys, and evictions (even if only logs/`INFO` stubs).

**Depends on:** M2.

**Tier progress:** T0 multi-tenant complete.

---

## Milestone 4 — Memory, eviction, and fairness

**Goal:** Predictable memory under load; fair multi-tenant behavior; solid intra-node concurrency.

**Deliverables**

- Documented memory accounting (what is charged).
- Eviction policies per tenant, Redis-like names where useful:
  - `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`
  - `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`
  - **Extensions (not in stock Redis):** `allkeys-fifo`, `volatile-fifo`
- Correct interaction: TTL expiry vs eviction under `maxmemory`.
- Global process memory guardrails (sum of tenants vs host).
- Optional fairness: max concurrent commands or simple rate limit per tenant.
- Stress tests: fill tenant to cap; neighbor unaffected.
- Validate intra-node sharding under concurrent multi-key load.

**Acceptance criteria**

- Under maxmemory, policy matches the written contract.
- `noeviction` returns Redis-like OOM on writes when full.
- Neighbor tenants stay within caps when one is hot.
- STATS/`INFO`-style fields reflect used memory and evictions accurately enough for ops.

**Depends on:** M3.

**Tier progress:** T1 memory subsystem.

---

## Milestone 5 — Pub/Sub (pre-deploy messaging)

**Goal:** Tenant-scoped Redis Pub/Sub so apps can use messaging before GKE work.

**Deliverables**

- Per-tenant Pub/Sub hub (in-process, single node).
- Commands: `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`.
- Basic `PUBSUB` (`CHANNELS`, `NUMSUB`, `NUMPAT`) as practical.
- Connection subscribe-mode rules (Redis-like: limited commands while subscribed).
- Concurrent-safe publish and subscription changes (D009).
- Integration tests with a standard client (subscribe in one conn, publish from another).

**Out of scope**

- Cross-node fan-out / cluster bus.
- Message persistence / replay.
- Sharded Pub/Sub across processes.

**Acceptance criteria**

- Subscriber in tenant A receives publishes on subscribed channels; tenant B cannot.
- Pattern subscribe works.
- Publish with zero subscribers returns 0; no crash.
- Concurrent publishers/subscribers do not race-corrupt hub state.

**Depends on:** M3 (tenant binding). Benefits from M1 connection model.

**Tier progress:** T0+ complete.

---

## Milestone 6 — Persistence (single node)

**Goal:** Configurable durability; survive process restart when enabled.

**Deliverables**

- Modes (D007): `none`, `snapshot`, `aof`, `snapshot+aof`.
- Snapshot format (versioned, multi-tenant metadata + keys); `SAVE` / `BGSAVE` or periodic snapshots.
- AOF: append writes; load/replay on startup; rewrite strategy (design + implement as needed).
- Hybrid: snapshot base + log tail (Redis-like).
- Load-on-startup path; corruption policy documented (refuse load vs skip tenant vs checksum).
- Per-tenant flush vs full-node snapshot defined.
- Persistence can be `none` for pure-cache deployments.

**Acceptance criteria**

- Each mode documented with expected RPO.
- Kill -9 after snapshot (snapshot mode): last snapshot restored.
- AOF/hybrid: documented max loss window under default fsync policy.
- Multi-tenant restore preserves isolation and per-tenant limits/policies.
- Pub/Sub state is not required to persist (ephemeral), consistent with Redis.

**Depends on:** M3 (tenant-aware dataset). Strings required; Pub/Sub optional for persistence contents.

**Tier progress:** T1 durability.

---

## Milestone 7 — Security and connection controls

**Goal:** Safe enough for shared environments and later GKE.

**Deliverables**

- `AUTH` fully enforced with tenant users when auth required by profile.
- TLS for client connections (config for certs).
- Protected defaults: no open write access without auth in non-local profiles.
- Command restrictions where useful (deny dangerous ops without config access).
- Max clients global and per tenant.

**Acceptance criteria**

- Unauthenticated clients cannot access tenant data when auth is required.
- TLS works with a standard client configuration.
- No admin Redis user required (config remains control plane).

**Depends on:** M3. Can partially overlap M4–M6.

**Tier progress:** T1 security.

---

## Milestone 8 — Observability and operations commands

**Goal:** Operate the service without reading logs alone.

**Deliverables**

- `INFO` sections: server, memory, stats, clients, persistence, tenants summary.
- Per-tenant stats command or `INFO` subsection.
- Prometheus metrics endpoint (or exporter): QPS, latency, hit ratio, memory, evictions, connections — global and per-tenant labels.
- Structured logging with tenant id and connection id.
- Health/readiness semantics defined (see M9).

**Acceptance criteria**

- A dashboard can show per-tenant memory and request rate.
- Operators can identify a hot tenant without custom debug builds.

**Depends on:** M3; improves with M4–M6 fields.

**Tier progress:** T1 ops.

---

## Milestone 9 — GKE deployability

**Goal:** Run cleanly on Google Kubernetes Engine as a service (cache and/or durable).

**Deliverables**

- Dockerfile and container image (minimal, non-root, configurable via env/flags/mounted config).
- Helm chart (or Kustomize) with:
  - Deployment and/or StatefulSet (StatefulSet when persistence needs stable identity/volumes)
  - Service (ClusterIP; optional internal TCP)
  - ConfigMap/Secret for config and auth material
  - PVC templates when persistence ≠ `none`
  - Resource requests/limits
  - Liveness and readiness probes (`PING` or HTTP health from M8)
  - PodDisruptionBudget basics
- Deploy env template (`.env.example`) for non-secret knobs (`PROJECT_ID`, region, cluster name, Artifact Registry repo/image, namespace, release name); real `.env` gitignored; no long-lived GCP keys or tenant secrets in git.
- Separate operator scripts (or make targets), each re-runnable where sensible:
  - Enable required GCP APIs (e.g. container, artifactregistry)
  - Create Artifact Registry repository for images
  - Create GKE cluster (Autopilot or small Standard)
  - Fetch cluster credentials into kubeconfig
  - Build image and push to Artifact Registry
  - Helm install/upgrade the release
- Deployment documentation (dedicated doc preferred, or README section if small): deploy config, required GitHub Actions variables/secrets, `.env` knobs, how to deploy, and how to test the GKE setup end-to-end.
- Docs also cover: from-scratch GKE path, existing-cluster path (skip create), in-cluster expose, tenants/config, persistence disk, upgrades; local non-container run remains `go build` / binary as today.
- GitHub Actions:
  - Deploy/CD path runs only on merge to `master` (not feature branches).
  - Auto-deploy is gated by a repository variable (e.g. `RUN_DEPLOY`); when unset or `false`, merge to `master` does not deploy; when `true`, deploy may run automatically.
  - Deploy job: GCP auth via Workload Identity Federation preferred, GKE credentials, image push as needed, `helm upgrade --install`.
  - Optional: `workflow_dispatch` for manual deploy when auto-deploy is off.
- Optional: NetworkPolicy examples.

**Acceptance criteria**

- Few-step install yields a reachable Redis-protocol endpoint.
- With persistence enabled + PVC, pod restart restores data.
- Probes do not incorrectly kill the pod under heavy load (readiness vs liveness split if needed).
- Single-tenant config path documented for dedicated pods.
- From-scratch path covers APIs, registry, cluster, credentials, image push, and Helm deploy via env template + scripts.
- Day-2 path (existing cluster) is credentials + image push + Helm only.
- GKE setup is tested end-to-end (cluster reachable, release healthy, basic RESP checks such as `PING`/`AUTH`/`SET`/`GET` as applicable).
- Deployment doc explains config, GitHub variables, `.env`, deploy steps, and how to test.
- Merge to `master` does not auto-deploy unless `RUN_DEPLOY` (or equivalent) is `true`; feature branches do not trigger deploy CI.
- Documented Actions deploy path works with non-interactive GCP auth when the gate allows it.

**Depends on:** M1–M3 + M5 (Pub/Sub) minimum for feature-complete image; M6 for meaningful stateful deploy; M7/M8 strongly preferred.

**Tier progress:** T1 “deployable.”

---

## Milestone 10 — Performance baseline and multi-core data path

**Goal:** Measure against Redis/Valkey; prove intra-node sharding; remove obvious ceilings.

**Deliverables**

- Benchmark suite using `redis-benchmark` (and scripts for multi-tenant AUTH where applicable).
- **Local baseline:** `go_cache` and Redis on the same machine; fair cells (same `maxmemory` + eviction policy); all 3 sharding strategies × all eviction policies; SET/GET + pipeline.
- **GKE baseline:** same matrix with pods **pinned to the same node** (affinity); in-cluster client for cloud comparison.
- Profiles: CPU, allocations, lock contention notes (pprof / documented hot path locks); harden sharding/pipeline only if benchmarks show a clear ceiling.
- Published baseline numbers in docs (local + GKE same-node).
- Optional short local smoke (`./bench/run-local.sh --smoke`).

**Acceptance criteria**

- Documented comparison numbers for a defined workload matrix (local and GKE same-node).
- No single global mutex on the multi-tenant data-plane hot path (per-shard locking remains).
- Fairness and isolation tests still pass after any concurrency changes.

**Depends on:** M2–M4 for meaningful numbers; M9 for GKE same-node path.

**Tier progress:** T1 performance track.

---

## Milestone 11 — T2 data structures (incremental)

**Goal:** Mainstream application patterns beyond string cache (T2).

**Compatibility baseline:** Redis Open Source command semantics (RESP2 replies). Command syntax and return shapes follow [redis.io command reference](https://redis.io/docs/latest/commands/). Where Redis has many optional flags, **M11 implements the core form first**; deferred flags are listed per command so scope is explicit.

**Shared rules (all types)**

| Rule | Behavior |
|------|----------|
| Missing key | Type-specific: usually empty/null/`0`, not an error (matches Redis). |
| Wrong type | Reply error containing `WRONGTYPE` if key exists but is not the expected type. |
| Key TTL | `EXPIRE` / `PEXPIRE` / `TTL` / `PTTL` / `PERSIST` apply to the **whole key** (hash/list/set/zset), not individual fields/members. |
| Empty after last remove | Deleting the last field/element/member removes the key (like Redis). |
| Tenancy | All commands are per-AUTH tenant; multi-key set ops only accept keys in the **same tenant**. |
| Memory | Field/member/value bytes count toward tenant `MaxMemory`; eviction policies apply to these keys. |
| Persistence | Snapshot + AOF encode all four types; restore round-trip preserves type + content + TTL. |
| `TYPE` | Returns `hash` / `list` / `set` / `zset` (or `none` if missing). |

---

### 11a — Hashes

Map of field → string value at one key. Ref: [Hashes](https://redis.io/docs/latest/develop/data-types/hashes/).

| Command | Syntax (M11) | Reply | Semantics (must match Redis) |
|---------|----------------|-------|------------------------------|
| `HSET` | `HSET key field value [field value ...]` | Integer: number of **new** fields added (updates do not count) | Create hash if missing; overwrite existing fields. |
| `HGET` | `HGET key field` | Bulk string value, or null if key/field missing | O(1) field lookup. |
| `HMGET` | `HMGET key field [field ...]` | Array of values (null for missing fields); same order as fields | Missing key → array of nulls, not error. |
| `HDEL` | `HDEL key field [field ...]` | Integer: fields actually removed | Ignore unknown fields; delete key if hash empty. |
| `HGETALL` | `HGETALL key` | Flat array `[field, value, ...]` (even length); empty array if missing | Field order need not be stable across restarts unless we document otherwise. |
| `HINCRBY` | `HINCRBY key field increment` | Integer: value after increment | Create key/field as `0` if missing; `increment` is signed 64-bit int; non-integer field value → error; overflow beyond int64 → error. |
| `HEXISTS` | `HEXISTS key field` | Integer `1` or `0` | `0` if key or field missing. |

**Out of scope for 11a (unless pulled in later):** `HINCRBYFLOAT`, `HSETNX`, `HLEN`, `HKEYS`, `HVALS`, `HSCAN`, `HRANDFIELD`, field-level expire (`HEXPIRE` family).

**11a acceptance**

- Unit + redis-cli/go-redis tests for every row above (happy path, missing key/field, multi-field `HSET`/`HMGET`/`HDEL`, `HINCRBY` create-from-zero and negative delta, `WRONGTYPE` on a string key).
- After `HDEL` of last field, `EXISTS`/`TYPE` show key gone.
- Persistence: write hash → restart/reload → `HGETALL` equal; TTL on hash key survives when set.
- Tenant A cannot see tenant B’s hash at the same key name.

---

### 11b — Lists

Ordered string sequence (left = head, right = tail). Ref: [Lists](https://redis.io/docs/latest/develop/data-types/lists/).

| Command | Syntax (M11) | Reply | Semantics (must match Redis) |
|---------|----------------|-------|------------------------------|
| `LPUSH` | `LPUSH key element [element ...]` | Integer: length after push | Create list if missing; left-most arg ends closest to head (same multi-arg order as Redis). |
| `RPUSH` | `RPUSH key element [element ...]` | Integer: length after push | Create list if missing; multi-arg append order matches Redis. |
| `LPOP` | `LPOP key` | Bulk string element, or null if empty/missing | Remove and return head. |
| `RPOP` | `RPOP key` | Bulk string element, or null if empty/missing | Remove and return tail. |
| `LRANGE` | `LRANGE key start stop` | Array of elements (may be empty) | Inclusive indices; negative indexes from end (`-1` = last); out-of-range clamped like Redis. |
| `LLEN` | `LLEN key` | Integer length, or `0` if missing | |

**Optional in 11b (implement if cheap; otherwise defer):** `LPOP key count` / `RPOP key count` (Redis ≥6.2 multi-pop → array reply).

**Out of scope for 11b:** blocking pops (`BLPOP`/`BRPOP`/`BLMOVE`), `LINSERT`, `LSET`, `LTRIM`, `LINDEX`, `LMOVE`, `LMPOP`, `LREM`, `LPOS`.

**11b acceptance**

- Tests: multi-arg push order, pop empty → null, `LRANGE` with positive/negative bounds, `LLEN` after mixed push/pop, `WRONGTYPE`.
- Last pop removes the key.
- Persistence + TTL round-trip for a non-empty list.
- Tenant isolation for list keys.

---

### 11c — Sets

Unordered unique string members. Ref: [Sets](https://redis.io/docs/latest/develop/data-types/sets/).

| Command | Syntax (M11) | Reply | Semantics (must match Redis) |
|---------|----------------|-------|------------------------------|
| `SADD` | `SADD key member [member ...]` | Integer: members **newly** added | Create set if missing; duplicates ignored. |
| `SREM` | `SREM key member [member ...]` | Integer: members actually removed | Delete key if set empty. |
| `SISMEMBER` | `SISMEMBER key member` | Integer `1` or `0` | `0` if key missing. |
| `SMEMBERS` | `SMEMBERS key` | Array of all members (empty if missing) | Order not guaranteed. |
| `SCARD` | `SCARD key` | Integer cardinality, or `0` if missing | |

**Multi-key set algebra (in scope for 11c — previously “as needed”):**

| Command | Syntax (M11) | Reply | Semantics |
|---------|----------------|-------|-----------|
| `SINTER` | `SINTER key [key ...]` | Array of intersection | Missing keys treated as empty sets. |
| `SUNION` | `SUNION key [key ...]` | Array of union | Missing keys treated as empty. |
| `SDIFF` | `SDIFF key [key ...]` | Array of first-minus-rest | Missing keys treated as empty. |

**Out of scope for 11c:** `SINTERSTORE` / `SUNIONSTORE` / `SDIFFSTORE`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SSCAN`, `SMISMEMBER`, `SINTERCARD`.

**11c acceptance**

- Tests for add/rem/idempotent add, membership, card, full members dump.
- `SINTER` / `SUNION` / `SDIFF` with 1–3 keys including a missing key (empty contribution).
- All keys in multi-key ops must resolve in the **same tenant**; wrong type on any key → `WRONGTYPE`.
- Persistence + TTL + isolation.

---

### 11d — Sorted sets

Members unique; each has a double score; ordered by score then member. Ref: [Sorted sets](https://redis.io/docs/latest/develop/data-types/sorted-sets/).

| Command | Syntax (M11) | Reply | Semantics (must match Redis) |
|---------|----------------|-------|------------------------------|
| `ZADD` | `ZADD key score member [score member ...]` | Integer: number of **new** members added | Create zset if missing; existing member’s score is updated (not counted as new). Scores are float64 (string form of double; `+inf`/`-inf` allowed if we accept Redis-compatible parsing). |
| `ZRANGE` | `ZRANGE key start stop [WITHSCORES]` | Array of members; with `WITHSCORES`, flat `[member, score, ...]` | Rank range, ascending score order; indices like lists (inclusive, negative from end). |
| `ZRANK` | `ZRANK key member` | Integer rank (0-based, low score first), or null if missing | No `WITHSCORE` required in M11 (Redis 7.2+ option deferred). |
| `ZSCORE` | `ZSCORE key member` | Bulk string of score, or null if missing | |
| `ZINCRBY` | `ZINCRBY key increment member` | Bulk string of new score | Create member with score `0` then apply increment if missing; create zset if needed. |
| `ZREM` | `ZREM key member [member ...]` | Integer: members removed | Delete key if zset empty. |

**Range-by-score (in scope for 11d — previously “as needed”):**

| Command | Syntax (M11) | Reply | Semantics |
|---------|----------------|-------|-----------|
| `ZRANGEBYSCORE` | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` | Array of members (or member/score pairs) | Inclusive `min`/`max` by default; support Redis-style `(1` exclusive bounds and `-inf`/`+inf`. Optional `LIMIT`. (Redis 6.2+ prefers `ZRANGE … BYSCORE`; either form is fine if behavior matches.) |

**Out of scope for 11d:** `ZADD` options `NX`/`XX`/`GT`/`LT`/`CH`/`INCR`; `ZREVRANGE` / `ZREVRANK` / `ZREVRANGEBYSCORE`; `ZCOUNT`; `ZREMRANGEBYRANK` / `ZREMRANGEBYSCORE`; `ZMSCORE`; `ZPOPMIN`/`ZPOPMAX`; lex ranges; blocking zset pops; `ZUNION`/`ZINTER` family.

**11d acceptance**

- Tests: multi-member `ZADD`, score update does not inflate add count, order by score then member, `ZRANGE`/`WITHSCORES`, `ZRANK`/`ZSCORE` missing → null, `ZINCRBY`, `ZREM`, `ZRANGEBYSCORE` with inclusive/exclusive and `-inf`/`+inf`.
- Persistence + TTL + isolation + `WRONGTYPE`.
- Memory: large member sets remain accountable under tenant maxmemory (smoke under eviction optional but recommended).

---

### Cross-cutting deliverables (all of M11)

- Store type tag per key; wire `TYPE` for new types.
- Command dispatch table / allowlist updated; unknown commands still Redis-style error.
- AOF/snapshot codecs for hash, list, set, zset (and mix with strings in one tenant).
- Metrics: optional counters per type (nice-to-have); INFO section may list keys-by-type if cheap.
- Docs: short command matrix in README or `docs/` — implemented vs deferred options above.

**Milestone-level acceptance criteria**

- **11a–11d each complete** against their tables and sub-acceptance blocks (tests green).
- **Mixed-type tenant:** one tenant holds string + hash + list + set + zset keys concurrently; isolation and eviction still sane; no cross-type corruption.
- **Persistence:** with `Mode=aof` or snapshot, kill process / reload; all five kinds round-trip (including empty-vs-missing where Redis distinguishes).
- **WRONGTYPE matrix:** for each new type command, operating on a key of every other type returns `WRONGTYPE`.
- **No regression:** existing string/TTL/AUTH/Pub/Sub/`INFO` tests still pass.
- **Memory:** `INFO memory` / tenant accounting does not go negative or ignore structure payloads under a mixed-type smoke.

**Depends on:** M2–M6 (store + persistence encodings before calling T2 done); M3 tenancy.

**Tier progress:** T2.

---

## Milestone 12 — High availability (single primary + replicas)

**Goal:** Survive node loss without relying only on disk.

**Deliverables**

- Async replication of write stream or snapshots + backlog.
- Replica read mode (optional `READONLY`).
- Failover: K8s-native election or Sentinel-like approach.
- Lag metrics; replication status in `INFO`.
- Runbook for failover and rejoin.
- Pub/Sub cross-node behavior explicitly documented (likely not fully replicated fan-out in first HA cut — decide at design time).

**Acceptance criteria**

- Primary kill: documented failover; clients reconnect with documented behavior.
- Tenant metadata and data both replicate.
- Split-brain policy documented.

**Depends on:** M6 (persistence helps full resync), M9 (deployment topology).

**Tier progress:** T4 HA subset.

---

## Milestone 13 — Scale-out (inter-node)

**Goal:** Grow beyond one primary’s memory/CPU via **tenant-aware** placement (D008).

**Deliverables**

- Control plane assigns tenants (or tenant partitions) to nodes.
- Route clients or proxy to the node owning the tenant.
- Rebalance/migrate tenant datasets.
- Blast-radius limits: one shard failure does not take down all tenants.

**Out of scope unless new decision**

- Redis Cluster slot API (`MOVED`/`ASK`, 16384 slots).

**Acceptance criteria**

- Adding a node increases aggregate capacity.
- Migration does not corrupt tenant isolation.
- Single-tenant dedicated pods remain a valid ops pattern.

**Depends on:** M12 strongly recommended; M3 tenant model essential.

**Tier progress:** T4 scale-out.

---

## Milestone 14 — Hardening and product polish

**Goal:** Operable for real platform users.

**Deliverables**

- Full command support matrix (implemented / partial / unsupported).
- Compatibility tests against chosen Redis behaviors for supported commands.
- Upgrade/compat policy for snapshot/AOF formats and config.
- Backup/restore per tenant export.
- Chaos tests: network blips, disk full, rapid reconnects, eviction under load, pub/sub reconnects.
- Security review checklist (auth, TLS, multi-tenant isolation).
- Performance regression gates.

**Acceptance criteria**

- New contributors can see what is supported.
- Upgrades between consecutive versions do not brick PVCs.
- Isolation and durability hold under failure injection.

**Depends on:** whatever tier you claim as “1.0.”

---

## Suggested “1.0” definition

| Included | Milestone |
|----------|-----------|
| RESP2 + string/key ops | M1–M2 |
| Multi-tenant isolation, limits, eviction | M3–M4 |
| Pub/Sub (tenant-scoped) | M5 |
| Persistence modes | M6 |
| Auth/TLS basics | M7 |
| Metrics + INFO | M8 |
| GKE Helm/charts, deploy scripts, GitHub Actions CI | M9 |
| Benchmarks + multi-core path | M10 |

**Post-1.0:** M11 (structures) → M12 (HA) → M13 (inter-node) → M14 continuous.  
T3 (MULTI/Lua) only if explicitly scheduled (currently non-goals).

---

## Dependency graph (condensed)

```text
M0 Architecture (done)
 └── M1 RESP
      └── M2 Strings + key TTL + intra-node shards
           └── M3 Tenancy + AUTH
                ├── M4 Memory / eviction / fairness
                ├── M5 Pub/Sub          ◄── before GKE
                ├── M6 Persistence
                ├── M7 Security
                ├── M8 Observability
                └── M9 GKE
                     └── M10 Performance
                          └── M11 Data structures
                               └── M12 HA
                                    └── M13 Inter-node tenant sharding
                                         └── M14 Hardening (ongoing from 1.0)
```

---

## Workstreams you can parallelize later

Once M0–M2 exist:

| Workstream | Milestones | Focus |
|------------|------------|--------|
| Data plane | M2, M4, M10, M11 | Store, types, speed, intra-node shards |
| Tenancy / config | M3, M7 | Isolation, AUTH, config control plane |
| Messaging | M5 | Pub/Sub |
| Durability / HA | M6, M12, M13 | Disk, replicas, inter-node |
| Platform | M8, M9 | Metrics, GKE, runbooks |
| Compatibility QA | all | Client tests, matrix, redis-benchmark |

---

## Explicit non-goals until named later

- Full Redis command parity
- Redis modules API
- JSON / search / vectors / time series (unless forced by a thin vertical)
- Exact RDB/AOF file format compatibility with Redis
- Active-Active multi-region
- Full RESP3 unless a required client demands it
- Lua / MULTI / Streams (see D010)
- Redis Cluster protocol

---

## Immediate next step

**Milestone 1:** create `internal/*` package skeleton, implement RESP2 + `PING`/`ECHO`/`QUIT`, prove `redis-cli` connectivity. See [docs/architecture.md](./docs/architecture.md) §10 for layout.
