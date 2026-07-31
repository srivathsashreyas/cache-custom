# Performance baselines (M10)

This document defines how we measure **cache-custom** against **Redis** and where results live.

## Goals

- Same workload matrix for **local** and **GKE same-node** runs.
- **Fair A/B:** Redis and `go_cache` share the same **max memory** and **eviction policy** for each comparison cell.
- **go_cache** is measured under **all three** intra-node sharding strategies (1, 2, 3) for every eviction policy.
- On GKE, Redis and `go_cache` pods are pinned to the **same node**.

## Fair comparison rules

| Knob | Redis | go_cache |
|------|-------|----------|
| Memory limit | `maxmemory` = `BENCH_MAX_MEMORY` (default 256 MiB) | Tenant `MaxMemory` = same |
| Eviction | `maxmemory-policy` = policy under test | Tenant `EvictionPolicy` = same |
| Sharding | n/a (single process map) | `ShardingStrategy` ∈ {1, 2, 3}, `ShardCount` = 4 |
| Auth | none | ACL user `s{strategy}-{policy}` / `benchpass` |

**Redis-native policies** (A/B with Redis):  
`noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`

**go_cache-only** (no Redis cell):  
`allkeys-fifo`, `volatile-fifo` (extensions; not in stock Redis)

Full grid size: **10 policies × 3 strategies** for go_cache, plus **8 Redis** policy cells.

Tenant naming (generated config): `s1-allkeys-lru`, `s2-allkeys-lru`, … Source of truth: `bench/matrix.sh`.

## Workload cells

Each matrix cell runs:

| Workload | Tool | Notes |
|----------|------|--------|
| SET/GET | `redis-benchmark -t set,get --csv` | Default clients/requests |
| Pipelined SET/GET | `-P 16` | Pipeline depth 16 |

Defaults (override with env):

| Env | Default | Smoke |
|-----|---------|--------|
| `BENCH_REQUESTS` | 100000 local / 50000 GKE | 10000 |
| `BENCH_CLIENTS` | 50 | 20 local |
| `BENCH_KEYSPACE` | 10000 | same |
| `BENCH_MAX_MEMORY` | 268435456 (256 MiB) | same |

**Smoke** (`--smoke`): only `allkeys-lru` × strategies 1–3 (+ matching Redis `allkeys-lru`). Proves wiring without the full grid.

## Local

```bash
# Requires: go, redis-server, redis-cli, redis-benchmark
./bench/run-local.sh
./bench/run-local.sh --smoke
```

1. Generates `bench/configs/go-cache-bench.generated.json` (one tenant per strategy×policy).
2. Starts Redis with `maxmemory` set; per cell, `CONFIG SET maxmemory-policy` + `FLUSHALL`.
3. Starts `go_cache` with the generated multi-tenant config.
4. Runs the comparison matrix; writes `bench/results/local-*.txt`.

## GKE same-node

Prerequisites: cluster up, **cache-custom** already deployed and Running.

Benchmark manifests live in the **Helm chart** (optional; off by default):

- `templates/bench-redis.yaml` — Redis peer with **required** podAffinity to chart `selectorLabels`
- `templates/bench-job.yaml` — in-cluster matrix Job

```bash
./bench/run-gke.sh
./bench/run-gke.sh --smoke
```

What it does:

1. Generates the same multi-tenant config as local.
2. `helm upgrade --reuse-values --set benchmark.enabled=true --set-file serverConfig=…` (rolls cache pods onto matrix tenants).
3. Verifies Redis on the **same node**.
4. Job applies Redis `maxmemory` / `maxmemory-policy` per cell and benchmarks every tenant.
5. Saves logs to `bench/results/gke-*.txt`.

**Restore normal deploy config** after bench (App1/App2, etc.):

```bash
./deploy/scripts/06-helm-deploy.sh
# or day2-deploy
```

Disable bench resources only:

```bash
helm upgrade cache-custom deploy/helm/cache-custom -n cache-custom \
  --reuse-values --set benchmark.enabled=false
```

## Hot path / locking (no single global data mutex)

| Component | Locking |
|-----------|---------|
| Per-key data | **Per-shard** `sync.Mutex` in `internal/store` |
| Global eviction order (strategy 1) | Separate `gMu` for tenant-wide lists — **not** held for every key op across all shards |
| Pub/Sub hub | Per-tenant hub mutex |
| Metrics collector | Own mutex (off the RESP data path critical section) |

Acceptance: multi-tenant string ops do **not** serialize on one process-wide data mutex. Isolation tests in `go test ./...` remain the fairness gate.

## Profiling (optional deep dive)

```bash
go tool pprof http://127.0.0.1:6060/debug/pprof/profile
```

pprof HTTP is not enabled by default; use `go test -cpuprofile` / manual instrumentation if chasing a specific ceiling.

## Published baseline numbers

Raw CSV dumps are gitignored under `bench/results/` (`local-*.txt`, `gke-*.txt`). Numbers below are from full matrix runs on **2026-07-31**.

**Shared knobs:** `BENCH_MAX_MEMORY=268435456` (256 MiB), `ShardCount=4`, clients **50**, keyspace **10000**, fair Redis `maxmemory` + `maxmemory-policy` per cell.

### Local (Apple Silicon M1, 2026-07-31)

| Field | Value |
|-------|--------|
| Host | macOS Darwin arm64 (M1 Air) |
| Redis | 8.2.0 local `redis-server` |
| Tool | `redis-benchmark -t set,get --csv` |
| Requests | **100000** / cell |
| Run | `./bench/run-local.sh` → `bench/results/local-20260731T063210Z.txt` |

#### SET/GET (pipeline=1) — Redis vs go_cache (avg strategies 1–3)

| Policy | Redis SET rps | Redis GET rps | go_cache SET rps (avg s1–s3) | go_cache GET rps (avg s1–s3) | go/redis SET |
|--------|---------------|---------------|------------------------------|------------------------------|--------------|
| `noeviction` | 178,253 | 175,131 | 126,450 | 129,217 | **0.71×** |
| `allkeys-lru` | 183,486 | 178,571 | 130,984 | 136,112 | **0.71×** |
| `allkeys-lfu` | 184,162 | 188,324 | 131,618 | 129,016 | **0.71×** |
| `allkeys-random` | 183,824 | 185,529 | 137,370 | 130,752 | **0.75×** |
| `volatile-lru` | 181,488 | 185,185 | 129,454 | 123,433 | **0.71×** |
| `volatile-lfu` | 188,324 | 188,324 | 139,622 | 136,194 | **0.74×** |
| `volatile-random` | 183,486 | 187,970 | 129,950 | 134,101 | **0.71×** |
| `volatile-ttl` | 185,185 | 186,916 | 136,981 | 135,348 | **0.74×** |
| `allkeys-fifo` | — (go only) | — | 134,532 | 135,154 | — |
| `volatile-fifo` | — (go only) | — | 116,366 | 122,856 | — |

#### SET/GET (pipeline=1) — strategies under `allkeys-lru` (detail)

| Target | Strategy | SET rps | GET rps |
|--------|----------|---------|---------|
| Redis | n/a | 183,486 | 178,571 |
| go_cache | 1 | 124,844 | 139,860 |
| go_cache | 2 | 133,156 | 139,276 |
| go_cache | 3 | 134,953 | 129,199 |

#### Pipelined SET/GET (pipeline=16) — averages

| Target | Avg SET rps (all Redis-native cells / all go cells) | Avg GET rps |
|--------|-----------------------------------------------------|-------------|
| Redis | ~1.50M | ~2.11M |
| go_cache | ~513k | ~539k |
| **go/redis SET** | **~0.34×** | **~0.26×** |

### GKE same-node (Autopilot, 2026-07-31)

| Field | Value |
|-------|--------|
| Cluster | GKE Autopilot `cache-custom` / `us-central1` |
| Node | both pods on `gk3-cache-custom-pool-3-b9dceb90-cddm` (required affinity verified) |
| go_cache | Helm release (linux/amd64 image), multi-tenant matrix `serverConfig` |
| Redis | chart `benchmark` peer (`redis:7-alpine`) |
| Requests | **50000** / cell |
| Run | `./bench/run-gke.sh` → `bench/results/gke-20260731T065713Z.txt` |

#### SET/GET (pipeline=1) — Redis vs go_cache (avg strategies 1–3)

| Policy | Redis SET rps | Redis GET rps | go_cache SET rps (avg s1–s3) | go_cache GET rps (avg s1–s3) | go/redis SET |
|--------|---------------|---------------|------------------------------|------------------------------|--------------|
| `noeviction` | 20,194 | 20,342 | 9,814 | 9,992 | **0.49×** |
| `allkeys-lru` | 21,413 | 20,517 | 10,028 | 10,078 | **0.47×** |
| `allkeys-lfu` | 19,654 | 19,794 | 9,857 | 10,081 | **0.50×** |
| `allkeys-random` | 19,048 | 19,928 | 9,982 | 10,348 | **0.52×** |
| `volatile-lru` | 20,080 | 21,468 | 10,210 | 10,018 | **0.51×** |
| `volatile-lfu` | 19,501 | 20,186 | 9,883 | 10,108 | **0.51×** |
| `volatile-random` | 19,508 | 20,467 | 9,541 | 10,182 | **0.49×** |
| `volatile-ttl` | 20,568 | 22,056 | 10,254 | 10,369 | **0.50×** |
| `allkeys-fifo` | — (go only) | — | 9,892 | 10,334 | — |
| `volatile-fifo` | — (go only) | — | 9,837 | 9,969 | — |

#### SET/GET (pipeline=1) — strategies under `allkeys-lru` (detail)

| Target | Strategy | SET rps | GET rps |
|--------|----------|---------|---------|
| Redis | n/a | 21,413 | 20,517 |
| go_cache | 1 | 9,730 | 10,012 |
| go_cache | 2 | 10,091 | 10,163 |
| go_cache | 3 | 10,263 | 10,058 |

#### Pipelined SET/GET (pipeline=16) — averages

| Target | Avg SET rps | Avg GET rps |
|--------|-------------|-------------|
| Redis | ~251k | ~257k |
| go_cache | ~19k | ~20k |
| **go/redis SET** | **~0.08×** | **~0.08×** |

### Comparison takeaways

- **Local single-op throughput:** go_cache lands at roughly **70–75% of Redis** SET/GET rps under matched `maxmemory` + eviction policy (AUTH multi-tenant path still in the hot path).
- **Local pipelining:** Redis pulls further ahead (~**3×** SET rps); go_cache still benefits strongly from `-P 16` (~4× its own P1) but does not match Redis’s pipeline efficiency yet.
- **GKE same-node:** Absolute numbers drop for both (shared node + Autopilot sizing + in-cluster hop). go_cache is about **half of Redis** on plain SET/GET and farther behind on pipeline (~**0.08×**), so cloud pipelining is the clearest gap.
- **Sharding strategies 1–3:** On this matrix (under limit, no forced eviction pressure) throughput is **similar across strategies**; policy choice also does not dominate when the working set fits in `maxmemory`.
- **go_cache-only policies** (`allkeys-fifo`, `volatile-fifo`) track the Redis-native policies’ go_cache numbers locally; no Redis baseline for those cells.
- These are **baseline throughput** numbers, not latency SLOs or multi-tenant fairness under eviction; re-run after hot-path changes and treat ±10% as noise.
