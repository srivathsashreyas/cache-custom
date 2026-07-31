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

Raw dumps (gitignored): `bench/results/local-20260731T081340Z.txt`, `bench/results/gke-20260731T083759Z.txt`.

**CSV columns from `redis-benchmark --csv`:** `rps`, `avg_latency_ms`, `min_latency_ms`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, `max_latency_ms` (reported for **SET** and **GET**).

**Shared knobs:** `maxmemory` / tenant `MaxMemory` = 256 MiB, clients **50**, keyspace **10000**, fair policy match per cell, strategies **1–3**.

### Local (Apple Silicon M1, 2026-07-31)

| Field | Value |
|-------|--------|
| Host | macOS Darwin arm64 (M1 Air) |
| Redis | 8.2.0 local |
| Requests | **100000** / cell |
| Command | `./bench/run-local.sh` |

#### Full matrix (rps + latency)

| Target | Strat | Policy | Op | Pipe | rps | avg_ms | min_ms | p50_ms | p95_ms | p99_ms | max_ms |
|--------|-------|--------|----|------|-----|--------|--------|--------|--------|--------|--------|
| `redis` | — | `noeviction` | SET | P1 | 173,611 | 0.166 | 0.072 | 0.151 | 0.215 | 0.639 | 1.159 |
| `redis` | — | `noeviction` | GET | P1 | 183,486 | 0.154 | 0.056 | 0.151 | 0.199 | 0.295 | 1.119 |
| `redis` | — | `noeviction` | SET | P16 | 1,449,275 | 0.476 | 0.120 | 0.447 | 0.719 | 1.271 | 1.839 |
| `redis` | — | `noeviction` | GET | P16 | 2,083,333 | 0.316 | 0.120 | 0.303 | 0.487 | 0.815 | 1.383 |
| `go_cache` | s1 | `noeviction` | SET | P1 | 127,226 | 0.225 | 0.032 | 0.207 | 0.359 | 0.583 | 2.071 |
| `go_cache` | s1 | `noeviction` | GET | P1 | 128,370 | 0.221 | 0.016 | 0.207 | 0.351 | 0.551 | 2.663 |
| `go_cache` | s1 | `noeviction` | SET | P16 | 510,204 | 0.875 | 0.008 | 0.735 | 1.959 | 3.647 | 13.775 |
| `go_cache` | s1 | `noeviction` | GET | P16 | 546,448 | 0.857 | 0.016 | 0.743 | 1.695 | 3.655 | 9.559 |
| `go_cache` | s2 | `noeviction` | SET | P1 | 120,773 | 0.235 | 0.008 | 0.207 | 0.375 | 0.679 | 19.711 |
| `go_cache` | s2 | `noeviction` | GET | P1 | 126,103 | 0.224 | 0.032 | 0.207 | 0.351 | 0.535 | 2.127 |
| `go_cache` | s2 | `noeviction` | SET | P16 | 534,759 | 0.869 | 0.008 | 0.647 | 2.415 | 4.839 | 9.519 |
| `go_cache` | s2 | `noeviction` | GET | P16 | 571,429 | 0.837 | 0.016 | 0.719 | 1.855 | 4.127 | 8.391 |
| `go_cache` | s3 | `noeviction` | SET | P1 | 122,850 | 0.231 | 0.008 | 0.207 | 0.391 | 0.623 | 3.095 |
| `go_cache` | s3 | `noeviction` | GET | P1 | 119,904 | 0.236 | 0.024 | 0.215 | 0.391 | 0.639 | 1.647 |
| `go_cache` | s3 | `noeviction` | SET | P16 | 512,821 | 0.883 | 0.024 | 0.655 | 2.399 | 3.679 | 11.575 |
| `go_cache` | s3 | `noeviction` | GET | P16 | 523,560 | 0.929 | 0.016 | 0.767 | 2.055 | 3.879 | 9.471 |
| `redis` | — | `allkeys-lru` | SET | P1 | 163,934 | 0.175 | 0.056 | 0.151 | 0.271 | 0.567 | 7.647 |
| `redis` | — | `allkeys-lru` | GET | P1 | 177,305 | 0.159 | 0.072 | 0.151 | 0.215 | 0.367 | 1.247 |
| `redis` | — | `allkeys-lru` | SET | P16 | 1,538,462 | 0.452 | 0.128 | 0.447 | 0.583 | 0.639 | 0.743 |
| `redis` | — | `allkeys-lru` | GET | P16 | 2,173,913 | 0.299 | 0.128 | 0.295 | 0.431 | 0.479 | 0.559 |
| `go_cache` | s1 | `allkeys-lru` | SET | P1 | 124,069 | 0.231 | 0.016 | 0.199 | 0.407 | 0.807 | 4.391 |
| `go_cache` | s1 | `allkeys-lru` | GET | P1 | 107,527 | 0.268 | 0.024 | 0.207 | 0.407 | 0.839 | 34.335 |
| `go_cache` | s1 | `allkeys-lru` | SET | P16 | 520,833 | 0.892 | 0.016 | 0.719 | 2.327 | 3.327 | 8.487 |
| `go_cache` | s1 | `allkeys-lru` | GET | P16 | 523,560 | 0.945 | 0.016 | 0.711 | 2.639 | 4.383 | 9.527 |
| `go_cache` | s2 | `allkeys-lru` | SET | P1 | 131,406 | 0.218 | 0.016 | 0.199 | 0.335 | 0.599 | 2.927 |
| `go_cache` | s2 | `allkeys-lru` | GET | P1 | 127,877 | 0.222 | 0.024 | 0.207 | 0.351 | 0.503 | 1.903 |
| `go_cache` | s2 | `allkeys-lru` | SET | P16 | 507,614 | 0.857 | 0.016 | 0.711 | 2.087 | 3.527 | 15.239 |
| `go_cache` | s2 | `allkeys-lru` | GET | P16 | 546,448 | 1.014 | 0.016 | 0.775 | 2.687 | 3.999 | 6.207 |
| `go_cache` | s3 | `allkeys-lru` | SET | P1 | 124,688 | 0.228 | 0.008 | 0.215 | 0.343 | 0.503 | 4.871 |
| `go_cache` | s3 | `allkeys-lru` | GET | P1 | 132,100 | 0.220 | 0.008 | 0.191 | 0.383 | 0.695 | 4.303 |
| `go_cache` | s3 | `allkeys-lru` | SET | P16 | 526,316 | 0.913 | 0.016 | 0.759 | 2.215 | 3.279 | 5.887 |
| `go_cache` | s3 | `allkeys-lru` | GET | P16 | 543,478 | 0.873 | 0.016 | 0.743 | 1.879 | 3.599 | 14.935 |
| `redis` | — | `allkeys-lfu` | SET | P1 | 186,567 | 0.152 | 0.056 | 0.151 | 0.191 | 0.223 | 0.583 |
| `redis` | — | `allkeys-lfu` | GET | P1 | 187,617 | 0.149 | 0.072 | 0.151 | 0.191 | 0.215 | 0.623 |
| `redis` | — | `allkeys-lfu` | SET | P16 | 1,562,500 | 0.441 | 0.160 | 0.439 | 0.567 | 0.631 | 0.791 |
| `redis` | — | `allkeys-lfu` | GET | P16 | 2,222,222 | 0.296 | 0.112 | 0.295 | 0.431 | 0.487 | 0.671 |
| `go_cache` | s1 | `allkeys-lfu` | SET | P1 | 136,054 | 0.207 | 0.024 | 0.199 | 0.303 | 0.423 | 1.927 |
| `go_cache` | s1 | `allkeys-lfu` | GET | P1 | 134,953 | 0.208 | 0.040 | 0.199 | 0.295 | 0.383 | 1.863 |
| `go_cache` | s1 | `allkeys-lfu` | SET | P16 | 529,101 | 0.809 | 0.024 | 0.695 | 1.887 | 3.631 | 7.167 |
| `go_cache` | s1 | `allkeys-lfu` | GET | P16 | 546,448 | 0.924 | 0.024 | 0.743 | 2.343 | 3.759 | 7.255 |
| `go_cache` | s2 | `allkeys-lfu` | SET | P1 | 137,174 | 0.206 | 0.024 | 0.199 | 0.303 | 0.431 | 1.575 |
| `go_cache` | s2 | `allkeys-lfu` | GET | P1 | 138,122 | 0.204 | 0.048 | 0.191 | 0.303 | 0.391 | 1.575 |
| `go_cache` | s2 | `allkeys-lfu` | SET | P16 | 485,437 | 0.918 | 0.024 | 0.743 | 2.311 | 3.775 | 9.895 |
| `go_cache` | s2 | `allkeys-lfu` | GET | P16 | 564,972 | 0.839 | 0.016 | 0.711 | 1.599 | 3.639 | 14.743 |
| `go_cache` | s3 | `allkeys-lfu` | SET | P1 | 137,931 | 0.205 | 0.008 | 0.199 | 0.303 | 0.431 | 6.135 |
| `go_cache` | s3 | `allkeys-lfu` | GET | P1 | 138,696 | 0.203 | 0.016 | 0.191 | 0.295 | 0.391 | 2.999 |
| `go_cache` | s3 | `allkeys-lfu` | SET | P16 | 537,634 | 0.863 | 0.024 | 0.711 | 2.263 | 3.639 | 9.223 |
| `go_cache` | s3 | `allkeys-lfu` | GET | P16 | 561,798 | 0.892 | 0.032 | 0.727 | 2.143 | 3.711 | 8.119 |
| `redis` | — | `allkeys-random` | SET | P1 | 185,874 | 0.151 | 0.056 | 0.151 | 0.191 | 0.231 | 0.703 |
| `redis` | — | `allkeys-random` | GET | P1 | 178,571 | 0.156 | 0.064 | 0.151 | 0.207 | 0.319 | 1.479 |
| `redis` | — | `allkeys-random` | SET | P16 | 1,612,903 | 0.433 | 0.144 | 0.431 | 0.559 | 0.639 | 0.799 |
| `redis` | — | `allkeys-random` | GET | P16 | 2,222,222 | 0.292 | 0.096 | 0.287 | 0.423 | 0.463 | 0.543 |
| `go_cache` | s1 | `allkeys-random` | SET | P1 | 125,471 | 0.225 | 0.032 | 0.207 | 0.335 | 0.439 | 9.879 |
| `go_cache` | s1 | `allkeys-random` | GET | P1 | 129,870 | 0.216 | 0.016 | 0.207 | 0.319 | 0.383 | 2.175 |
| `go_cache` | s1 | `allkeys-random` | SET | P16 | 515,464 | 0.835 | 0.016 | 0.687 | 2.231 | 3.975 | 9.135 |
| `go_cache` | s1 | `allkeys-random` | GET | P16 | 540,541 | 0.909 | 0.008 | 0.815 | 1.943 | 3.535 | 11.223 |
| `go_cache` | s2 | `allkeys-random` | SET | P1 | 137,363 | 0.207 | 0.016 | 0.199 | 0.303 | 0.399 | 2.383 |
| `go_cache` | s2 | `allkeys-random` | GET | P1 | 138,696 | 0.203 | 0.032 | 0.191 | 0.287 | 0.367 | 1.895 |
| `go_cache` | s2 | `allkeys-random` | SET | P16 | 531,915 | 0.860 | 0.032 | 0.751 | 1.983 | 3.095 | 15.575 |
| `go_cache` | s2 | `allkeys-random` | GET | P16 | 558,659 | 0.901 | 0.024 | 0.767 | 2.271 | 3.711 | 9.279 |
| `go_cache` | s3 | `allkeys-random` | SET | P1 | 141,643 | 0.201 | 0.016 | 0.191 | 0.295 | 0.391 | 2.615 |
| `go_cache` | s3 | `allkeys-random` | GET | P1 | 124,224 | 0.225 | 0.040 | 0.199 | 0.343 | 0.471 | 10.327 |
| `go_cache` | s3 | `allkeys-random` | SET | P16 | 537,634 | 0.885 | 0.008 | 0.687 | 2.391 | 3.791 | 11.975 |
| `go_cache` | s3 | `allkeys-random` | GET | P16 | 549,451 | 0.938 | 0.016 | 0.711 | 2.543 | 3.943 | 7.599 |
| `redis` | — | `volatile-lru` | SET | P1 | 182,482 | 0.154 | 0.080 | 0.151 | 0.199 | 0.239 | 0.711 |
| `redis` | — | `volatile-lru` | GET | P1 | 186,916 | 0.150 | 0.048 | 0.151 | 0.191 | 0.207 | 0.719 |
| `redis` | — | `volatile-lru` | SET | P16 | 1,587,302 | 0.436 | 0.136 | 0.431 | 0.559 | 0.623 | 0.903 |
| `redis` | — | `volatile-lru` | GET | P16 | 2,173,913 | 0.302 | 0.120 | 0.295 | 0.431 | 0.495 | 0.671 |
| `go_cache` | s1 | `volatile-lru` | SET | P1 | 135,685 | 0.207 | 0.024 | 0.199 | 0.303 | 0.383 | 2.191 |
| `go_cache` | s1 | `volatile-lru` | GET | P1 | 137,741 | 0.205 | 0.032 | 0.199 | 0.303 | 0.383 | 1.479 |
| `go_cache` | s1 | `volatile-lru` | SET | P16 | 518,135 | 0.875 | 0.032 | 0.727 | 2.199 | 3.223 | 5.639 |
| `go_cache` | s1 | `volatile-lru` | GET | P16 | 571,429 | 0.851 | 0.008 | 0.759 | 1.799 | 3.343 | 9.591 |
| `go_cache` | s2 | `volatile-lru` | SET | P1 | 135,870 | 0.209 | 0.016 | 0.199 | 0.311 | 0.479 | 1.951 |
| `go_cache` | s2 | `volatile-lru` | GET | P1 | 127,389 | 0.221 | 0.040 | 0.207 | 0.327 | 0.407 | 11.375 |
| `go_cache` | s2 | `volatile-lru` | SET | P16 | 520,833 | 0.827 | 0.016 | 0.639 | 2.207 | 3.839 | 11.671 |
| `go_cache` | s2 | `volatile-lru` | GET | P16 | 507,614 | 1.044 | 0.016 | 0.863 | 2.543 | 4.095 | 9.047 |
| `go_cache` | s3 | `volatile-lru` | SET | P1 | 136,240 | 0.206 | 0.024 | 0.191 | 0.319 | 0.431 | 1.767 |
| `go_cache` | s3 | `volatile-lru` | GET | P1 | 144,718 | 0.193 | 0.032 | 0.183 | 0.271 | 0.367 | 1.239 |
| `go_cache` | s3 | `volatile-lru` | SET | P16 | 549,451 | 0.904 | 0.016 | 0.791 | 2.079 | 2.919 | 4.127 |
| `go_cache` | s3 | `volatile-lru` | GET | P16 | 561,798 | 0.965 | 0.016 | 0.791 | 2.407 | 3.775 | 8.415 |
| `redis` | — | `volatile-lfu` | SET | P1 | 185,529 | 0.152 | 0.072 | 0.151 | 0.191 | 0.215 | 0.983 |
| `redis` | — | `volatile-lfu` | GET | P1 | 187,266 | 0.150 | 0.056 | 0.151 | 0.191 | 0.207 | 0.279 |
| `redis` | — | `volatile-lfu` | SET | P16 | 1,587,302 | 0.434 | 0.136 | 0.439 | 0.543 | 0.599 | 0.679 |
| `redis` | — | `volatile-lfu` | GET | P16 | 2,173,913 | 0.303 | 0.112 | 0.303 | 0.439 | 0.487 | 0.559 |
| `go_cache` | s1 | `volatile-lfu` | SET | P1 | 133,690 | 0.210 | 0.048 | 0.199 | 0.311 | 0.383 | 1.799 |
| `go_cache` | s1 | `volatile-lfu` | GET | P1 | 135,135 | 0.210 | 0.048 | 0.199 | 0.303 | 0.367 | 11.327 |
| `go_cache` | s1 | `volatile-lfu` | SET | P16 | 520,833 | 0.837 | 0.008 | 0.759 | 1.639 | 2.903 | 7.255 |
| `go_cache` | s1 | `volatile-lfu` | GET | P16 | 555,556 | 0.867 | 0.016 | 0.719 | 2.295 | 3.679 | 18.271 |
| `go_cache` | s2 | `volatile-lfu` | SET | P1 | 125,786 | 0.220 | 0.072 | 0.207 | 0.327 | 0.383 | 4.591 |
| `go_cache` | s2 | `volatile-lfu` | GET | P1 | 122,699 | 0.227 | 0.064 | 0.215 | 0.343 | 0.463 | 0.967 |
| `go_cache` | s2 | `volatile-lfu` | SET | P16 | 540,541 | 0.885 | 0.024 | 0.671 | 2.463 | 3.839 | 7.943 |
| `go_cache` | s2 | `volatile-lfu` | GET | P16 | 571,429 | 0.886 | 0.016 | 0.695 | 2.375 | 3.759 | 7.327 |
| `go_cache` | s3 | `volatile-lfu` | SET | P1 | 135,135 | 0.209 | 0.032 | 0.199 | 0.311 | 0.415 | 2.343 |
| `go_cache` | s3 | `volatile-lfu` | GET | P1 | 131,406 | 0.212 | 0.064 | 0.199 | 0.311 | 0.367 | 0.927 |
| `go_cache` | s3 | `volatile-lfu` | SET | P16 | 531,915 | 0.879 | 0.016 | 0.719 | 2.359 | 3.575 | 7.127 |
| `go_cache` | s3 | `volatile-lfu` | GET | P16 | 584,795 | 0.832 | 0.048 | 0.735 | 1.743 | 3.007 | 6.591 |
| `redis` | — | `volatile-random` | SET | P1 | 141,643 | 0.209 | 0.048 | 0.151 | 0.503 | 0.959 | 12.335 |
| `redis` | — | `volatile-random` | GET | P1 | 186,916 | 0.149 | 0.048 | 0.151 | 0.191 | 0.215 | 0.431 |
| `redis` | — | `volatile-random` | SET | P16 | 1,538,462 | 0.452 | 0.120 | 0.447 | 0.599 | 0.647 | 0.839 |
| `redis` | — | `volatile-random` | GET | P16 | 2,272,727 | 0.290 | 0.120 | 0.287 | 0.415 | 0.455 | 0.527 |
| `go_cache` | s1 | `volatile-random` | SET | P1 | 140,056 | 0.200 | 0.032 | 0.191 | 0.295 | 0.383 | 1.895 |
| `go_cache` | s1 | `volatile-random` | GET | P1 | 122,399 | 0.224 | 0.048 | 0.215 | 0.335 | 0.407 | 2.079 |
| `go_cache` | s1 | `volatile-random` | SET | P16 | 507,614 | 0.869 | 0.024 | 0.703 | 2.239 | 3.535 | 7.535 |
| `go_cache` | s1 | `volatile-random` | GET | P16 | 571,429 | 0.862 | 0.024 | 0.719 | 2.167 | 3.399 | 18.319 |
| `go_cache` | s2 | `volatile-random` | SET | P1 | 136,240 | 0.210 | 0.032 | 0.191 | 0.335 | 0.631 | 2.303 |
| `go_cache` | s2 | `volatile-random` | GET | P1 | 125,156 | 0.227 | 0.032 | 0.207 | 0.327 | 0.583 | 23.631 |
| `go_cache` | s2 | `volatile-random` | SET | P16 | 546,448 | 0.815 | 0.016 | 0.703 | 1.999 | 3.367 | 7.871 |
| `go_cache` | s2 | `volatile-random` | GET | P16 | 549,451 | 0.965 | 0.016 | 0.743 | 2.599 | 4.039 | 11.943 |
| `go_cache` | s3 | `volatile-random` | SET | P1 | 123,762 | 0.228 | 0.016 | 0.207 | 0.335 | 0.551 | 11.575 |
| `go_cache` | s3 | `volatile-random` | GET | P1 | 135,501 | 0.206 | 0.016 | 0.191 | 0.303 | 0.431 | 2.863 |
| `go_cache` | s3 | `volatile-random` | SET | P16 | 531,915 | 0.870 | 0.024 | 0.631 | 2.583 | 4.407 | 6.831 |
| `go_cache` | s3 | `volatile-random` | GET | P16 | 571,429 | 0.879 | 0.016 | 0.695 | 2.303 | 4.151 | 6.543 |
| `redis` | — | `volatile-ttl` | SET | P1 | 179,211 | 0.153 | 0.064 | 0.151 | 0.183 | 0.199 | 1.007 |
| `redis` | — | `volatile-ttl` | GET | P1 | 181,488 | 0.150 | 0.080 | 0.151 | 0.183 | 0.191 | 0.327 |
| `redis` | — | `volatile-ttl` | SET | P16 | 1,562,500 | 0.444 | 0.144 | 0.439 | 0.575 | 0.631 | 0.695 |
| `redis` | — | `volatile-ttl` | GET | P16 | 2,083,333 | 0.319 | 0.120 | 0.311 | 0.463 | 0.503 | 0.607 |
| `go_cache` | s1 | `volatile-ttl` | SET | P1 | 138,122 | 0.203 | 0.040 | 0.191 | 0.295 | 0.391 | 1.935 |
| `go_cache` | s1 | `volatile-ttl` | GET | P1 | 135,685 | 0.208 | 0.040 | 0.199 | 0.303 | 0.407 | 1.743 |
| `go_cache` | s1 | `volatile-ttl` | SET | P16 | 512,821 | 0.848 | 0.016 | 0.711 | 2.159 | 3.247 | 5.983 |
| `go_cache` | s1 | `volatile-ttl` | GET | P16 | 558,659 | 0.903 | 0.024 | 0.695 | 2.583 | 4.247 | 9.215 |
| `go_cache` | s2 | `volatile-ttl` | SET | P1 | 117,096 | 0.238 | 0.056 | 0.215 | 0.359 | 0.535 | 13.399 |
| `go_cache` | s2 | `volatile-ttl` | GET | P1 | 135,870 | 0.206 | 0.056 | 0.199 | 0.295 | 0.359 | 1.183 |
| `go_cache` | s2 | `volatile-ttl` | SET | P16 | 520,833 | 0.910 | 0.024 | 0.727 | 2.487 | 3.943 | 5.935 |
| `go_cache` | s2 | `volatile-ttl` | GET | P16 | 571,429 | 0.933 | 0.024 | 0.703 | 2.687 | 4.327 | 14.615 |
| `go_cache` | s3 | `volatile-ttl` | SET | P1 | 125,945 | 0.220 | 0.040 | 0.207 | 0.319 | 0.399 | 4.255 |
| `go_cache` | s3 | `volatile-ttl` | GET | P1 | 139,860 | 0.199 | 0.040 | 0.191 | 0.271 | 0.335 | 0.967 |
| `go_cache` | s3 | `volatile-ttl` | SET | P16 | 529,101 | 0.875 | 0.016 | 0.695 | 2.279 | 3.335 | 6.679 |
| `go_cache` | s3 | `volatile-ttl` | GET | P16 | 561,798 | 0.933 | 0.032 | 0.719 | 2.415 | 4.263 | 18.847 |
| `go_cache` | s1 | `allkeys-fifo` | SET | P1 | 129,032 | 0.216 | 0.016 | 0.207 | 0.311 | 0.447 | 1.911 |
| `go_cache` | s1 | `allkeys-fifo` | GET | P1 | 130,548 | 0.213 | 0.072 | 0.207 | 0.303 | 0.351 | 0.791 |
| `go_cache` | s1 | `allkeys-fifo` | SET | P16 | 471,698 | 0.887 | 0.016 | 0.687 | 2.343 | 4.159 | 19.007 |
| `go_cache` | s1 | `allkeys-fifo` | GET | P16 | 520,833 | 0.928 | 0.016 | 0.775 | 2.015 | 3.791 | 12.855 |
| `go_cache` | s2 | `allkeys-fifo` | SET | P1 | 133,869 | 0.210 | 0.016 | 0.199 | 0.303 | 0.423 | 4.719 |
| `go_cache` | s2 | `allkeys-fifo` | GET | P1 | 138,122 | 0.202 | 0.080 | 0.199 | 0.271 | 0.311 | 0.927 |
| `go_cache` | s2 | `allkeys-fifo` | SET | P16 | 515,464 | 0.903 | 0.024 | 0.679 | 2.599 | 3.703 | 6.943 |
| `go_cache` | s2 | `allkeys-fifo` | GET | P16 | 571,429 | 0.871 | 0.040 | 0.719 | 2.103 | 3.807 | 16.071 |
| `go_cache` | s3 | `allkeys-fifo` | SET | P1 | 131,926 | 0.210 | 0.072 | 0.199 | 0.303 | 0.391 | 1.031 |
| `go_cache` | s3 | `allkeys-fifo` | GET | P1 | 131,062 | 0.213 | 0.024 | 0.207 | 0.295 | 0.439 | 1.783 |
| `go_cache` | s3 | `allkeys-fifo` | SET | P16 | 543,478 | 0.851 | 0.024 | 0.687 | 2.335 | 3.687 | 6.159 |
| `go_cache` | s3 | `allkeys-fifo` | GET | P16 | 515,464 | 0.896 | 0.024 | 0.743 | 2.103 | 4.351 | 16.047 |
| `go_cache` | s1 | `volatile-fifo` | SET | P1 | 135,501 | 0.205 | 0.064 | 0.199 | 0.279 | 0.327 | 0.759 |
| `go_cache` | s1 | `volatile-fifo` | GET | P1 | 114,155 | 0.241 | 0.016 | 0.231 | 0.335 | 0.519 | 12.143 |
| `go_cache` | s1 | `volatile-fifo` | SET | P16 | 549,451 | 0.841 | 0.016 | 0.711 | 2.207 | 3.175 | 6.751 |
| `go_cache` | s1 | `volatile-fifo` | GET | P16 | 500,000 | 0.890 | 0.016 | 0.751 | 2.007 | 3.799 | 11.439 |
| `go_cache` | s2 | `volatile-fifo` | SET | P1 | 135,135 | 0.207 | 0.088 | 0.199 | 0.279 | 0.343 | 0.951 |
| `go_cache` | s2 | `volatile-fifo` | GET | P1 | 137,552 | 0.201 | 0.104 | 0.199 | 0.263 | 0.311 | 0.647 |
| `go_cache` | s2 | `volatile-fifo` | SET | P16 | 485,437 | 0.933 | 0.024 | 0.751 | 2.231 | 4.535 | 9.127 |
| `go_cache` | s2 | `volatile-fifo` | GET | P16 | 564,972 | 0.852 | 0.016 | 0.735 | 1.839 | 3.527 | 7.983 |
| `go_cache` | s3 | `volatile-fifo` | SET | P1 | 135,501 | 0.207 | 0.032 | 0.199 | 0.279 | 0.519 | 1.543 |
| `go_cache` | s3 | `volatile-fifo` | GET | P1 | 137,931 | 0.199 | 0.064 | 0.199 | 0.263 | 0.311 | 0.471 |
| `go_cache` | s3 | `volatile-fifo` | SET | P16 | 502,513 | 0.943 | 0.024 | 0.719 | 2.543 | 3.855 | 7.655 |
| `go_cache` | s3 | `volatile-fifo` | GET | P16 | 555,556 | 0.956 | 0.024 | 0.759 | 2.495 | 3.919 | 6.223 |

### GKE same-node (Autopilot, 2026-07-31)

| Field | Value |
|-------|--------|
| Cluster | GKE Autopilot `cache-custom` / `us-central1` |
| Node | `gk3-cache-custom-pool-3-eb447980-6w89` (required affinity verified for Redis + go_cache) |
| go_cache | Deployment + emptyDir for this run (PVC zone was memory-full; see notes) |
| Redis | chart `benchmark` peer |
| Requests | **50000** / cell |
| Command | `./bench/run-gke.sh` |

#### Full matrix (rps + latency)

| Target | Strat | Policy | Op | Pipe | rps | avg_ms | min_ms | p50_ms | p95_ms | p99_ms | max_ms |
|--------|-------|--------|----|------|-----|--------|--------|--------|--------|--------|--------|
| `redis` | — | `noeviction` | SET | P1 | 10,663 | 3.751 | 0.368 | 2.031 | 4.567 | 47.423 | 60.511 |
| `redis` | — | `noeviction` | GET | P1 | 11,631 | 3.507 | 0.360 | 1.871 | 3.703 | 47.967 | 55.135 |
| `redis` | — | `noeviction` | SET | P16 | 132,275 | 5.684 | 0.432 | 2.687 | 45.695 | 48.159 | 51.903 |
| `redis` | — | `noeviction` | GET | P16 | 149,701 | 4.782 | 0.720 | 2.511 | 7.479 | 50.879 | 54.975 |
| `go_cache` | s1 | `noeviction` | SET | P1 | 9,540 | 4.358 | 0.128 | 1.823 | 24.447 | 50.751 | 62.879 |
| `go_cache` | s1 | `noeviction` | GET | P1 | 8,317 | 5.139 | 0.136 | 1.991 | 33.023 | 56.287 | 87.103 |
| `go_cache` | s1 | `noeviction` | SET | P16 | 18,423 | 33.959 | 0.232 | 37.535 | 70.399 | 103.359 | 147.327 |
| `go_cache` | s1 | `noeviction` | GET | P16 | 18,403 | 34.088 | 0.192 | 39.391 | 68.223 | 95.359 | 146.687 |
| `go_cache` | s2 | `noeviction` | SET | P1 | 9,599 | 4.001 | 0.120 | 1.743 | 18.719 | 48.831 | 58.751 |
| `go_cache` | s2 | `noeviction` | GET | P1 | 9,280 | 4.322 | 0.128 | 1.831 | 24.287 | 49.151 | 59.103 |
| `go_cache` | s2 | `noeviction` | SET | P16 | 17,668 | 35.102 | 0.120 | 39.231 | 77.183 | 102.335 | 164.223 |
| `go_cache` | s2 | `noeviction` | GET | P16 | 18,498 | 34.710 | 0.272 | 35.263 | 83.327 | 99.391 | 159.999 |
| `go_cache` | s3 | `noeviction` | SET | P1 | 9,037 | 4.543 | 0.136 | 1.871 | 27.311 | 51.295 | 67.647 |
| `go_cache` | s3 | `noeviction` | GET | P1 | 8,527 | 4.619 | 0.120 | 1.943 | 27.903 | 51.103 | 65.855 |
| `go_cache` | s3 | `noeviction` | SET | P16 | 17,870 | 35.670 | 0.208 | 37.503 | 85.055 | 101.759 | 152.063 |
| `go_cache` | s3 | `noeviction` | GET | P16 | 18,553 | 34.848 | 0.248 | 37.791 | 84.671 | 113.343 | 197.119 |
| `redis` | — | `allkeys-lru` | SET | P1 | 10,844 | 3.651 | 0.376 | 2.151 | 4.087 | 45.791 | 52.607 |
| `redis` | — | `allkeys-lru` | GET | P1 | 11,361 | 3.823 | 0.216 | 2.039 | 4.167 | 47.999 | 57.727 |
| `redis` | — | `allkeys-lru` | SET | P16 | 124,069 | 5.426 | 0.616 | 2.359 | 45.503 | 58.207 | 60.959 |
| `redis` | — | `allkeys-lru` | GET | P16 | 128,205 | 4.877 | 0.776 | 1.823 | 46.751 | 50.687 | 51.967 |
| `go_cache` | s1 | `allkeys-lru` | SET | P1 | 9,022 | 4.411 | 0.136 | 1.839 | 22.959 | 49.663 | 81.983 |
| `go_cache` | s1 | `allkeys-lru` | GET | P1 | 9,285 | 4.280 | 0.136 | 1.815 | 24.815 | 49.439 | 59.775 |
| `go_cache` | s1 | `allkeys-lru` | SET | P16 | 18,302 | 33.304 | 0.240 | 27.087 | 83.967 | 106.559 | 203.519 |
| `go_cache` | s1 | `allkeys-lru` | GET | P16 | 18,437 | 34.377 | 0.184 | 21.231 | 88.767 | 102.399 | 186.367 |
| `go_cache` | s2 | `allkeys-lru` | SET | P1 | 9,639 | 4.205 | 0.136 | 1.703 | 21.823 | 50.463 | 82.239 |
| `go_cache` | s2 | `allkeys-lru` | GET | P1 | 9,198 | 4.471 | 0.128 | 1.879 | 27.167 | 49.951 | 72.255 |
| `go_cache` | s2 | `allkeys-lru` | SET | P16 | 17,655 | 34.136 | 0.216 | 38.719 | 82.879 | 100.671 | 153.599 |
| `go_cache` | s2 | `allkeys-lru` | GET | P16 | 17,947 | 34.498 | 0.232 | 36.575 | 88.639 | 103.999 | 149.375 |
| `go_cache` | s3 | `allkeys-lru` | SET | P1 | 9,249 | 4.398 | 0.136 | 1.815 | 23.727 | 51.903 | 79.679 |
| `go_cache` | s3 | `allkeys-lru` | GET | P1 | 9,452 | 4.293 | 0.128 | 1.767 | 19.631 | 51.359 | 66.367 |
| `go_cache` | s3 | `allkeys-lru` | SET | P16 | 17,458 | 34.252 | 0.184 | 30.191 | 82.431 | 104.063 | 344.575 |
| `go_cache` | s3 | `allkeys-lru` | GET | P16 | 19,209 | 34.033 | 0.208 | 36.319 | 78.463 | 98.111 | 147.455 |
| `redis` | — | `allkeys-lfu` | SET | P1 | 10,700 | 3.810 | 0.408 | 2.127 | 4.351 | 47.359 | 59.071 |
| `redis` | — | `allkeys-lfu` | GET | P1 | 11,101 | 3.754 | 0.392 | 2.103 | 4.183 | 47.295 | 52.351 |
| `redis` | — | `allkeys-lfu` | SET | P16 | 142,045 | 4.843 | 0.848 | 2.447 | 9.391 | 52.543 | 54.399 |
| `redis` | — | `allkeys-lfu` | GET | P16 | 133,690 | 5.228 | 0.880 | 2.527 | 44.703 | 49.695 | 55.135 |
| `go_cache` | s1 | `allkeys-lfu` | SET | P1 | 8,913 | 4.554 | 0.136 | 1.895 | 25.375 | 50.239 | 66.879 |
| `go_cache` | s1 | `allkeys-lfu` | GET | P1 | 9,390 | 4.306 | 0.152 | 1.831 | 22.351 | 50.527 | 60.927 |
| `go_cache` | s1 | `allkeys-lfu` | SET | P16 | 17,800 | 33.696 | 0.288 | 35.679 | 92.159 | 145.151 | 209.151 |
| `go_cache` | s1 | `allkeys-lfu` | GET | P16 | 17,730 | 35.798 | 0.200 | 38.079 | 87.295 | 114.367 | 298.751 |
| `go_cache` | s2 | `allkeys-lfu` | SET | P1 | 9,228 | 4.315 | 0.128 | 1.879 | 22.751 | 49.247 | 55.871 |
| `go_cache` | s2 | `allkeys-lfu` | GET | P1 | 8,922 | 4.340 | 0.144 | 1.847 | 24.783 | 49.567 | 148.479 |
| `go_cache` | s2 | `allkeys-lfu` | SET | P16 | 17,655 | 32.172 | 0.336 | 35.103 | 79.295 | 102.975 | 197.375 |
| `go_cache` | s2 | `allkeys-lfu` | GET | P16 | 17,844 | 34.399 | 0.200 | 29.327 | 86.015 | 101.055 | 158.207 |
| `go_cache` | s3 | `allkeys-lfu` | SET | P1 | 9,230 | 4.199 | 0.144 | 1.815 | 23.167 | 50.175 | 60.639 |
| `go_cache` | s3 | `allkeys-lfu` | GET | P1 | 9,306 | 4.214 | 0.136 | 1.751 | 24.367 | 50.111 | 57.471 |
| `go_cache` | s3 | `allkeys-lfu` | SET | P16 | 17,668 | 37.194 | 0.264 | 37.407 | 88.127 | 103.871 | 288.255 |
| `go_cache` | s3 | `allkeys-lfu` | GET | P16 | 18,471 | 32.579 | 0.272 | 38.463 | 63.935 | 88.447 | 146.047 |
| `redis` | — | `allkeys-random` | SET | P1 | 11,109 | 3.759 | 0.400 | 2.055 | 4.247 | 46.911 | 52.159 |
| `redis` | — | `allkeys-random` | GET | P1 | 11,596 | 3.750 | 0.224 | 2.015 | 4.007 | 47.999 | 51.263 |
| `redis` | — | `allkeys-random` | SET | P16 | 130,890 | 4.779 | 0.648 | 2.543 | 43.231 | 49.119 | 52.159 |
| `redis` | — | `allkeys-random` | GET | P16 | 147,493 | 4.058 | 0.440 | 2.167 | 5.991 | 49.247 | 52.511 |
| `go_cache` | s1 | `allkeys-random` | SET | P1 | 8,687 | 4.464 | 0.136 | 1.879 | 24.799 | 50.911 | 72.767 |
| `go_cache` | s1 | `allkeys-random` | GET | P1 | 8,843 | 4.323 | 0.152 | 1.871 | 27.263 | 49.343 | 61.823 |
| `go_cache` | s1 | `allkeys-random` | SET | P16 | 16,513 | 36.866 | 0.168 | 37.599 | 90.495 | 149.503 | 293.631 |
| `go_cache` | s1 | `allkeys-random` | GET | P16 | 18,546 | 34.152 | 0.224 | 37.087 | 80.895 | 147.327 | 265.983 |
| `go_cache` | s2 | `allkeys-random` | SET | P1 | 8,690 | 4.642 | 0.136 | 1.943 | 24.911 | 50.271 | 67.519 |
| `go_cache` | s2 | `allkeys-random` | GET | P1 | 9,086 | 4.453 | 0.136 | 1.855 | 26.415 | 50.655 | 56.575 |
| `go_cache` | s2 | `allkeys-random` | SET | P16 | 16,578 | 36.467 | 0.176 | 37.023 | 85.567 | 134.655 | 205.951 |
| `go_cache` | s2 | `allkeys-random` | GET | P16 | 18,342 | 32.552 | 0.200 | 37.663 | 68.799 | 94.911 | 207.231 |
| `go_cache` | s3 | `allkeys-random` | SET | P1 | 8,672 | 4.535 | 0.120 | 1.903 | 24.159 | 51.967 | 88.959 |
| `go_cache` | s3 | `allkeys-random` | GET | P1 | 9,294 | 4.182 | 0.144 | 1.751 | 9.799 | 50.847 | 62.431 |
| `go_cache` | s3 | `allkeys-random` | SET | P16 | 16,667 | 35.281 | 0.304 | 37.919 | 90.303 | 141.695 | 204.671 |
| `go_cache` | s3 | `allkeys-random` | GET | P16 | 18,109 | 33.823 | 0.152 | 31.407 | 82.751 | 95.999 | 125.695 |
| `redis` | — | `volatile-lru` | SET | P1 | 10,858 | 3.886 | 0.384 | 2.087 | 4.479 | 47.519 | 57.759 |
| `redis` | — | `volatile-lru` | GET | P1 | 11,450 | 3.803 | 0.312 | 1.999 | 4.303 | 48.863 | 57.471 |
| `redis` | — | `volatile-lru` | SET | P16 | 129,534 | 4.414 | 0.424 | 2.199 | 8.599 | 49.887 | 51.455 |
| `redis` | — | `volatile-lru` | GET | P16 | 129,870 | 4.340 | 0.688 | 1.799 | 45.119 | 49.823 | 53.439 |
| `go_cache` | s1 | `volatile-lru` | SET | P1 | 8,972 | 4.337 | 0.128 | 1.815 | 23.343 | 49.247 | 68.799 |
| `go_cache` | s1 | `volatile-lru` | GET | P1 | 9,294 | 4.172 | 0.136 | 1.767 | 24.303 | 49.535 | 62.207 |
| `go_cache` | s1 | `volatile-lru` | SET | P16 | 17,001 | 36.253 | 0.248 | 38.943 | 92.479 | 141.183 | 202.239 |
| `go_cache` | s1 | `volatile-lru` | GET | P16 | 18,491 | 34.062 | 0.224 | 38.111 | 81.471 | 105.343 | 160.895 |
| `go_cache` | s2 | `volatile-lru` | SET | P1 | 8,970 | 4.403 | 0.144 | 1.815 | 25.471 | 49.343 | 58.527 |
| `go_cache` | s2 | `volatile-lru` | GET | P1 | 8,886 | 4.528 | 0.136 | 1.839 | 27.103 | 52.383 | 72.511 |
| `go_cache` | s2 | `volatile-lru` | SET | P16 | 17,844 | 29.428 | 0.288 | 21.839 | 78.847 | 92.607 | 148.735 |
| `go_cache` | s2 | `volatile-lru` | GET | P16 | 16,480 | 37.047 | 0.240 | 33.535 | 90.687 | 103.615 | 228.735 |
| `go_cache` | s3 | `volatile-lru` | SET | P1 | 8,820 | 4.550 | 0.144 | 1.919 | 25.919 | 49.279 | 55.327 |
| `go_cache` | s3 | `volatile-lru` | GET | P1 | 9,217 | 4.171 | 0.136 | 1.791 | 23.471 | 49.151 | 59.199 |
| `go_cache` | s3 | `volatile-lru` | SET | P16 | 16,181 | 36.252 | 0.176 | 38.527 | 93.119 | 116.543 | 201.215 |
| `go_cache` | s3 | `volatile-lru` | GET | P16 | 18,519 | 32.075 | 0.144 | 34.431 | 83.647 | 107.839 | 203.007 |
| `redis` | — | `volatile-lfu` | SET | P1 | 11,201 | 3.882 | 0.368 | 2.079 | 4.271 | 47.903 | 55.167 |
| `redis` | — | `volatile-lfu` | GET | P1 | 11,302 | 3.667 | 0.328 | 2.055 | 3.975 | 47.103 | 52.863 |
| `redis` | — | `volatile-lfu` | SET | P16 | 124,378 | 5.377 | 0.864 | 2.159 | 48.031 | 51.391 | 51.999 |
| `redis` | — | `volatile-lfu` | GET | P16 | 156,740 | 4.863 | 1.104 | 2.599 | 7.895 | 48.607 | 51.487 |
| `go_cache` | s1 | `volatile-lfu` | SET | P1 | 8,743 | 4.453 | 0.128 | 1.879 | 26.991 | 49.279 | 83.199 |
| `go_cache` | s1 | `volatile-lfu` | GET | P1 | 8,940 | 4.297 | 0.144 | 1.871 | 22.719 | 48.543 | 60.575 |
| `go_cache` | s1 | `volatile-lfu` | SET | P16 | 18,228 | 32.487 | 0.152 | 19.599 | 86.527 | 109.823 | 154.239 |
| `go_cache` | s1 | `volatile-lfu` | GET | P16 | 16,082 | 37.557 | 0.312 | 38.015 | 92.223 | 159.487 | 204.031 |
| `go_cache` | s2 | `volatile-lfu` | SET | P1 | 8,519 | 4.650 | 0.144 | 1.863 | 27.967 | 51.807 | 66.239 |
| `go_cache` | s2 | `volatile-lfu` | GET | P1 | 8,818 | 4.408 | 0.136 | 1.903 | 24.479 | 49.823 | 58.879 |
| `go_cache` | s2 | `volatile-lfu` | SET | P16 | 15,768 | 37.139 | 0.128 | 35.647 | 93.311 | 154.239 | 251.391 |
| `go_cache` | s2 | `volatile-lfu` | GET | P16 | 17,966 | 32.372 | 0.288 | 29.471 | 79.551 | 91.135 | 104.895 |
| `go_cache` | s3 | `volatile-lfu` | SET | P1 | 9,084 | 4.315 | 0.128 | 1.831 | 24.815 | 49.983 | 60.351 |
| `go_cache` | s3 | `volatile-lfu` | GET | P1 | 8,749 | 4.408 | 0.144 | 1.847 | 26.879 | 51.135 | 65.535 |
| `go_cache` | s3 | `volatile-lfu` | SET | P16 | 18,423 | 31.034 | 0.240 | 37.663 | 57.631 | 92.159 | 150.143 |
| `go_cache` | s3 | `volatile-lfu` | GET | P16 | 18,491 | 31.289 | 0.184 | 33.023 | 80.703 | 99.583 | 154.367 |
| `redis` | — | `volatile-random` | SET | P1 | 11,201 | 3.814 | 0.344 | 2.023 | 4.159 | 48.447 | 52.991 |
| `redis` | — | `volatile-random` | GET | P1 | 11,374 | 3.858 | 0.384 | 2.111 | 4.135 | 47.999 | 55.839 |
| `redis` | — | `volatile-random` | SET | P16 | 128,205 | 4.800 | 0.480 | 1.975 | 45.759 | 51.199 | 53.087 |
| `redis` | — | `volatile-random` | GET | P16 | 154,321 | 4.419 | 0.328 | 2.263 | 7.671 | 48.831 | 49.983 |
| `go_cache` | s1 | `volatile-random` | SET | P1 | 8,964 | 4.521 | 0.128 | 1.799 | 25.679 | 50.111 | 82.687 |
| `go_cache` | s1 | `volatile-random` | GET | P1 | 9,065 | 4.219 | 0.136 | 1.815 | 23.311 | 50.015 | 58.143 |
| `go_cache` | s1 | `volatile-random` | SET | P16 | 17,838 | 32.400 | 0.224 | 37.055 | 66.111 | 93.759 | 151.807 |
| `go_cache` | s1 | `volatile-random` | GET | P16 | 17,908 | 35.836 | 0.240 | 37.503 | 80.959 | 98.687 | 147.967 |
| `go_cache` | s2 | `volatile-random` | SET | P1 | 8,817 | 4.545 | 0.128 | 1.815 | 27.647 | 50.719 | 77.823 |
| `go_cache` | s2 | `volatile-random` | GET | P1 | 8,932 | 4.518 | 0.144 | 1.911 | 25.663 | 51.199 | 62.399 |
| `go_cache` | s2 | `volatile-random` | SET | P16 | 17,624 | 34.563 | 0.256 | 39.487 | 70.719 | 95.167 | 137.983 |
| `go_cache` | s2 | `volatile-random` | GET | P16 | 15,995 | 36.712 | 0.160 | 30.799 | 90.879 | 137.215 | 241.791 |
| `go_cache` | s3 | `volatile-random` | SET | P1 | 8,758 | 4.600 | 0.136 | 1.943 | 26.879 | 50.783 | 68.415 |
| `go_cache` | s3 | `volatile-random` | GET | P1 | 9,443 | 4.294 | 0.096 | 1.775 | 23.039 | 50.047 | 57.503 |
| `go_cache` | s3 | `volatile-random` | SET | P16 | 18,477 | 31.159 | 0.256 | 32.319 | 74.879 | 108.543 | 150.911 |
| `go_cache` | s3 | `volatile-random` | GET | P16 | 15,427 | 37.526 | 0.248 | 39.743 | 92.095 | 143.103 | 250.495 |
| `redis` | — | `volatile-ttl` | SET | P1 | 11,307 | 3.843 | 0.432 | 2.167 | 4.159 | 47.263 | 53.183 |
| `redis` | — | `volatile-ttl` | GET | P1 | 11,650 | 3.748 | 0.392 | 1.983 | 3.975 | 48.415 | 52.415 |
| `redis` | — | `volatile-ttl` | SET | P16 | 127,877 | 5.044 | 0.360 | 2.615 | 41.599 | 49.183 | 50.943 |
| `redis` | — | `volatile-ttl` | GET | P16 | 156,250 | 4.918 | 1.528 | 2.727 | 8.255 | 46.239 | 48.255 |
| `go_cache` | s1 | `volatile-ttl` | SET | P1 | 9,632 | 4.119 | 0.128 | 1.711 | 20.927 | 50.527 | 57.247 |
| `go_cache` | s1 | `volatile-ttl` | GET | P1 | 9,213 | 4.349 | 0.136 | 1.783 | 24.335 | 51.231 | 113.407 |
| `go_cache` | s1 | `volatile-ttl` | SET | P16 | 17,883 | 31.783 | 0.272 | 34.687 | 76.735 | 97.279 | 144.895 |
| `go_cache` | s1 | `volatile-ttl` | GET | P16 | 14,607 | 41.098 | 0.152 | 30.479 | 96.575 | 139.391 | 205.567 |
| `go_cache` | s2 | `volatile-ttl` | SET | P1 | 9,203 | 4.306 | 0.120 | 1.807 | 19.855 | 51.135 | 56.735 |
| `go_cache` | s2 | `volatile-ttl` | GET | P1 | 9,370 | 4.262 | 0.120 | 1.767 | 25.007 | 50.463 | 62.527 |
| `go_cache` | s2 | `volatile-ttl` | SET | P16 | 18,328 | 33.275 | 0.200 | 38.687 | 67.071 | 90.303 | 117.567 |
| `go_cache` | s2 | `volatile-ttl` | GET | P16 | 15,674 | 37.654 | 0.200 | 40.255 | 96.831 | 141.055 | 258.047 |
| `go_cache` | s3 | `volatile-ttl` | SET | P1 | 9,060 | 4.177 | 0.128 | 1.799 | 9.943 | 50.303 | 61.759 |
| `go_cache` | s3 | `volatile-ttl` | GET | P1 | 9,256 | 4.291 | 0.128 | 1.791 | 23.407 | 50.335 | 64.127 |
| `go_cache` | s3 | `volatile-ttl` | SET | P16 | 18,342 | 33.544 | 0.224 | 33.023 | 81.279 | 104.255 | 142.975 |
| `go_cache` | s3 | `volatile-ttl` | GET | P16 | 15,333 | 39.968 | 0.240 | 37.503 | 96.767 | 161.151 | 285.951 |
| `go_cache` | s1 | `allkeys-fifo` | SET | P1 | 9,264 | 4.049 | 0.120 | 1.759 | 9.935 | 49.983 | 56.991 |
| `go_cache` | s1 | `allkeys-fifo` | GET | P1 | 9,290 | 4.305 | 0.128 | 1.799 | 25.087 | 50.399 | 57.535 |
| `go_cache` | s1 | `allkeys-fifo` | SET | P16 | 18,103 | 31.855 | 0.152 | 31.583 | 78.207 | 95.039 | 190.975 |
| `go_cache` | s1 | `allkeys-fifo` | GET | P16 | 18,288 | 32.552 | 0.184 | 31.343 | 81.279 | 96.895 | 128.831 |
| `go_cache` | s2 | `allkeys-fifo` | SET | P1 | 8,661 | 4.614 | 0.136 | 1.855 | 26.223 | 50.271 | 86.463 |
| `go_cache` | s2 | `allkeys-fifo` | GET | P1 | 9,391 | 4.421 | 0.136 | 1.799 | 23.311 | 50.655 | 58.815 |
| `go_cache` | s2 | `allkeys-fifo` | SET | P16 | 17,838 | 32.674 | 0.216 | 31.583 | 82.751 | 105.023 | 161.535 |
| `go_cache` | s2 | `allkeys-fifo` | GET | P16 | 18,685 | 31.877 | 0.248 | 37.791 | 62.335 | 98.303 | 109.695 |
| `go_cache` | s3 | `allkeys-fifo` | SET | P1 | 8,814 | 4.524 | 0.128 | 1.807 | 26.383 | 50.943 | 87.743 |
| `go_cache` | s3 | `allkeys-fifo` | GET | P1 | 8,661 | 4.507 | 0.136 | 1.927 | 24.367 | 51.071 | 65.663 |
| `go_cache` | s3 | `allkeys-fifo` | SET | P16 | 18,335 | 33.138 | 0.232 | 38.207 | 66.559 | 95.167 | 153.727 |
| `go_cache` | s3 | `allkeys-fifo` | GET | P16 | 19,011 | 33.844 | 0.240 | 30.511 | 83.071 | 101.311 | 223.359 |
| `go_cache` | s1 | `volatile-fifo` | SET | P1 | 8,482 | 4.666 | 0.144 | 1.847 | 31.087 | 51.455 | 87.487 |
| `go_cache` | s1 | `volatile-fifo` | GET | P1 | 8,622 | 4.725 | 0.120 | 1.903 | 43.039 | 51.807 | 73.663 |
| `go_cache` | s1 | `volatile-fifo` | SET | P16 | 16,835 | 34.721 | 0.264 | 39.487 | 71.743 | 93.375 | 143.487 |
| `go_cache` | s1 | `volatile-fifo` | GET | P16 | 17,343 | 33.727 | 0.176 | 36.095 | 88.703 | 146.815 | 257.023 |
| `go_cache` | s2 | `volatile-fifo` | SET | P1 | 5,941 | 6.381 | 0.128 | 1.895 | 45.311 | 82.303 | 478.719 |
| `go_cache` | s2 | `volatile-fifo` | GET | P1 | 6,407 | 6.077 | 0.144 | 1.895 | 43.487 | 64.415 | 887.807 |
| `go_cache` | s2 | `volatile-fifo` | SET | P16 | 10,307 | 54.046 | 0.168 | 31.647 | 267.519 | 421.375 | 904.191 |
| `go_cache` | s2 | `volatile-fifo` | GET | P16 | 15,451 | 39.933 | 0.144 | 31.887 | 101.823 | 362.495 | 428.543 |
| `go_cache` | s3 | `volatile-fifo` | SET | P1 | 7,149 | 5.465 | 0.144 | 1.887 | 28.655 | 55.967 | 470.271 |
| `go_cache` | s3 | `volatile-fifo` | GET | P1 | 6,510 | 5.938 | 0.136 | 1.815 | 37.375 | 64.095 | 536.575 |
| `go_cache` | s3 | `volatile-fifo` | SET | P16 | 16,869 | 35.300 | 0.168 | 37.183 | 90.367 | 136.447 | 161.791 |
| `go_cache` | s3 | `volatile-fifo` | GET | P16 | 14,413 | 41.849 | 0.200 | 39.839 | 96.831 | 280.319 | 311.551 |

### Comparison takeaways

- **Local throughput:** go_cache is roughly **0.7–0.8× Redis SET rps** on pipeline=1 across matched policies; Redis still leads.
- **Local latency:** go_cache **avg/p50** are modestly higher than Redis on P1 (~0.20 ms vs ~0.15 ms p50 SET). At **pipeline=16**, go_cache p95/p99 rise more (often multi-ms) while Redis stays sub-ms → pipeline efficiency is a clear gap.
- **GKE same-node throughput:** Absolute rps is lower for both (shared Autopilot node). On this run go_cache was closer on plain SET/GET (~**0.8×** Redis rps) but only ~**0.14×** on pipeline=16.
- **GKE latency:** Redis P1 p50 ~2 ms with long tail (p99 often ~45–50 ms under node noise). go_cache P1 p50 is similar (~1.8 ms) but **pipeline p50 ~35 ms** and p99 often **>100 ms**, so cloud pipelining latency is the main penalty.
- **Strategies 1–3 / policies:** Under this working set (fits in 256 MiB), strategy and policy choice do not dominate; throughput and latency stay in a similar band. FIFO-only policies track other go_cache cells (no Redis baseline).
- Treat ±10% as noise; re-run after hot-path changes. Full rows above are the source of truth for this publish date.
