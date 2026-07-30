# Performance baselines (M10)

This document defines how we measure **cache-custom** against **Redis** and where results live.

## Goals

- Same workload matrix for **local** and **GKE same-node** runs.
- Fair A/B: Redis and `go_cache` under the same client tool (`redis-benchmark`) and, on GKE, **the same Kubernetes node**.
- Smoke path for regressions without a full GKE spin-up.

## Workload matrix

| Workload | Tool | Notes |
|----------|------|--------|
| SET/GET | `redis-benchmark -t set,get` | Default clients/requests in scripts |
| Pipelined SET/GET | `-P 16` | Pipeline depth 16 |
| Multi-tenant (local only) | Sequential runs as `App1` then `App2` | Redis has no multi-tenant AUTH model like ours |

Defaults (override with env):

| Env | Default | Smoke |
|-----|---------|--------|
| `BENCH_REQUESTS` | 100000 local / 50000 GKE job | 10000 / 5000 |
| `BENCH_CLIENTS` | 50 | 10–20 |
| `BENCH_KEYSPACE` | 10000 | same |

**Auth:** `go_cache` uses `--user App1 --pass secret1`. Redis local/GKE bench images run **without** AUTH for a clean baseline.

## Local

```bash
# Requires: go, redis-server, redis-benchmark
./bench/run-local.sh
# or smoke:
./bench/ci-smoke.sh
```

Starts Redis on `:16379` and `go_cache` on `:19001` with `bench/configs/go-cache-bench.json`, runs the matrix, writes `bench/results/local-*.txt`.

## GKE same-node

Prerequisites: cluster up, **cache-custom** already deployed and Running (e.g. `./deploy/scripts/from-scratch.sh` or day-2).

```bash
# Pins redis-bench to the node running cache-custom via required podAffinity
./bench/run-gke.sh
# smoke:
./bench/run-gke.sh --smoke
```

What it does:

1. Confirms a Running `cache-custom` pod and records its `nodeName`.
2. Applies `bench/k8s/redis-same-node.yaml` (Redis + **required** affinity on `kubernetes.io/hostname` to `app.kubernetes.io/name=cache-custom`).
3. Verifies Redis scheduled on the **same node**.
4. Runs `bench/k8s/bench-job.yaml` (`redis-benchmark` in-cluster against both Services).
5. Saves logs to `bench/results/gke-*.txt`.

Cleanup:

```bash
kubectl delete -f bench/k8s/redis-same-node.yaml
kubectl -n cache-bench delete job redis-benchmark-job --ignore-not-found
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
# Example: CPU profile while local bench runs (attach as needed)
go tool pprof http://127.0.0.1:6060/debug/pprof/profile
```

pprof HTTP is not enabled by default; use `go test -cpuprofile` / manual instrumentation if chasing a specific ceiling. Harden sharding only if profiles show a clear bottleneck.

## Published baseline numbers

Re-run scripts and paste CSV/summary lines below (or keep timestamped files under `bench/results/` as source of truth).

### Local (sample smoke on Apple Silicon M1, 2026-07-28)

`./bench/ci-smoke.sh` — 5k requests, 10 clients (illustrative only; re-run full matrix for published numbers).

| Target | Workload | SET rps (approx) | GET rps (approx) |
|--------|----------|------------------|------------------|
| Redis | set/get | ~98k | ~152k |
| Redis | pipeline 16 | ~1.25M | ~2.5M |
| go_cache (AUTH) | set/get | ~128k | ~122k |
| go_cache (AUTH) | pipeline 16 | ~455k | ~500k |

Full runs: `bench/results/local-*.txt` (gitignored; generate locally).

### GKE same-node

After cluster + `cache-custom` deploy: `./bench/run-gke.sh` → `bench/results/gke-*.txt` (same-node Redis affinity verified in script).

## CI smoke

```bash
./bench/ci-smoke.sh
```

Optional: `workflow_dispatch` only (not every PR) — see `.github/workflows/bench-smoke.yml`.
