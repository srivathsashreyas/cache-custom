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

**Goal:** Mainstream application patterns beyond string cache.

### 11a — Hashes

- `HGET`, `HSET`, `HMGET`, `HDEL`, `HGETALL`, `HINCRBY`, `HEXISTS`, memory accounting.

### 11b — Lists

- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`; blocking pops later if needed.

### 11c — Sets

- `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`; inter/union/diff as needed.

### 11d — Sorted sets

- `ZADD`, `ZRANGE`, `ZRANK`, `ZSCORE`, `ZINCRBY`, `ZREM`; range-by-score as needed.

**Cross-cutting**

- `TYPE` / `WRONGTYPE`; TTL on whole key; eviction + persistence include all types; tenant isolation unchanged.

**Acceptance criteria**

- Each sub-milestone: client tests + persistence round-trip when persistence exists.
- Memory accounting remains plausible under mixed types.

**Depends on:** M2–M6 (persistence should understand encodings before calling T2 “done”).

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
