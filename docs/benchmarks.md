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

Re-run scripts and paste CSV lines below (or keep timestamped files under `bench/results/`).

### Local (sample smoke)

`./bench/run-local.sh --smoke` — illustrative only; re-run full matrix for published numbers.

| Target | Cell | Notes |
|--------|------|--------|
| Redis | `allkeys-lru` | maxmemory = 256 MiB |
| go_cache | `s1` / `s2` / `s3` + `allkeys-lru` | same MaxMemory + policy |

Full runs: `bench/results/local-*.txt` (gitignored).

### GKE same-node

After cluster + deploy: `./bench/run-gke.sh` → `bench/results/gke-*.txt`.
