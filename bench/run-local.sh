#!/usr/bin/env bash
# Local performance baseline: go_cache vs Redis on the same machine.
# Purpose: fair A/B — same maxmemory + eviction policy; go_cache × all sharding strategies.
#
# Prerequisites: redis-server, redis-benchmark, redis-cli, go toolchain.
# Usage:
#   ./bench/run-local.sh              # full matrix (all policies × strategies 1–3)
#   ./bench/run-local.sh --smoke      # allkeys-lru × strategies 1–3 only
#   BENCH_REQUESTS=10000 ./bench/run-local.sh

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=workloads.sh
source "${ROOT}/bench/workloads.sh"
# shellcheck source=matrix.sh
source "${ROOT}/bench/matrix.sh"

SMOKE=false
if [[ "${1:-}" == "--smoke" ]]; then
  SMOKE=true
  BENCH_REQUESTS="${BENCH_REQUESTS:-10000}"
  BENCH_CLIENTS="${BENCH_CLIENTS:-20}"
fi

require_redis_benchmark
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: need $1" >&2; exit 1; }
}
require_cmd go
require_cmd redis-server
require_cmd redis-cli

OUT_DIR="${ROOT}/bench/results"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/local-${STAMP}.txt"
if [[ "${SMOKE}" == "true" ]]; then
  OUT_FILE="${OUT_DIR}/local-smoke-${STAMP}.txt"
fi

GO_PORT=19001
REDIS_PORT=16379
GO_PID=""
REDIS_PID=""
BIN="${ROOT}/bench/.bin/cache-custom"
CFG="${ROOT}/bench/configs/go-cache-bench.generated.json"
mkdir -p "${ROOT}/bench/.bin" "${ROOT}/bench/configs"

cleanup() {
  [[ -n "${GO_PID}" ]] && kill "${GO_PID}" 2>/dev/null || true
  [[ -n "${REDIS_PID}" ]] && kill "${REDIS_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

matrix_select "${SMOKE}"
gen_go_cache_config "${CFG}"
echo "Wrote go_cache config: ${CFG}"
echo "  tenants: $((${#STRATEGIES_TO_RUN[@]} * ${#POLICIES_TO_RUN[@]})) (strategies × policies)"

echo "Building go_cache..."
go build -o "${BIN}" "${ROOT}/cmd/cache-custom"

echo "Starting Redis on :${REDIS_PORT} (maxmemory=${BENCH_MAX_MEMORY})..."
# Initial policy is overwritten per matrix cell via CONFIG SET.
redis-server \
  --port "${REDIS_PORT}" \
  --save "" \
  --appendonly no \
  --maxmemory "${BENCH_MAX_MEMORY}" \
  --maxmemory-policy allkeys-lru \
  --daemonize no &
REDIS_PID=$!
sleep 0.5

echo "Starting go_cache on :${GO_PORT}..."
"${BIN}" -addr ":${GO_PORT}" -config "${CFG}" &
GO_PID=$!
for _ in $(seq 1 50); do
  if redis-cli -h 127.0.0.1 -p "${GO_PORT}" --user "$(tenant_name "${STRATEGIES_TO_RUN[0]}" "${POLICIES_TO_RUN[0]}")" -a "${BENCH_PASS}" PING 2>/dev/null | grep -q PONG; then
    break
  fi
  # Also accept unauthenticated PING failure; wait for process accept.
  if redis-cli -h 127.0.0.1 -p "${GO_PORT}" PING 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.1
done

{
  echo "cache-custom local benchmark"
  echo "timestamp_utc=${STAMP}"
  echo "host=$(hostname)"
  echo "uname=$(uname -a)"
  echo "requests=${BENCH_REQUESTS} clients=${BENCH_CLIENTS} keyspace=${BENCH_KEYSPACE}"
  echo "fair_baseline=same MaxMemory + EvictionPolicy for Redis and go_cache tenants"
  echo "go_sharding_strategies=${STRATEGIES_TO_RUN[*]}"
  echo "config=${CFG}"
  echo

  run_comparison_matrix 127.0.0.1 "${REDIS_PORT}" 127.0.0.1 "${GO_PORT}"
} | tee "${OUT_FILE}"

echo
echo "Results written to ${OUT_FILE}"
