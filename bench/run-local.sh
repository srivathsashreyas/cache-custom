#!/usr/bin/env bash
# Local performance baseline: go_cache vs Redis on the same machine.
# Purpose: M10 apples-to-apples numbers without GKE noise.
#
# Prerequisites: redis-server, redis-benchmark, go toolchain.
# Usage:
#   ./bench/run-local.sh              # full matrix
#   BENCH_REQUESTS=10000 ./bench/run-local.sh   # smoke
#   ./bench/run-local.sh --smoke

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=workloads.sh
source "${ROOT}/bench/workloads.sh"

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
mkdir -p "${ROOT}/bench/.bin"

cleanup() {
  [[ -n "${GO_PID}" ]] && kill "${GO_PID}" 2>/dev/null || true
  [[ -n "${REDIS_PID}" ]] && kill "${REDIS_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "Building go_cache..."
go build -o "${BIN}" "${ROOT}/cmd/cache-custom"

echo "Starting Redis on :${REDIS_PORT}..."
redis-server --port "${REDIS_PORT}" --save "" --appendonly no --daemonize no &
REDIS_PID=$!
sleep 0.5

echo "Starting go_cache on :${GO_PORT}..."
"${BIN}" -addr ":${GO_PORT}" -config "${ROOT}/bench/configs/go-cache-bench.json" &
GO_PID=$!
# Wait for PING
for i in $(seq 1 50); do
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
  echo

  echo "===== Redis (local :${REDIS_PORT}, no auth) ====="
  run_matrix 127.0.0.1 "${REDIS_PORT}" "" "" "redis"

  echo
  echo "===== go_cache (local :${GO_PORT}, AUTH App1/secret1) ====="
  run_matrix 127.0.0.1 "${GO_PORT}" "App1" "secret1" "go_cache"

  if [[ "${SMOKE}" != "true" ]]; then
    echo
    echo "===== go_cache multi-tenant (App1 then App2, sequential SET/GET) ====="
    BENCH_PIPELINE=1
    run_workload "go_cache/tenant-App1" 127.0.0.1 "${GO_PORT}" "App1" "secret1"
    run_workload "go_cache/tenant-App2" 127.0.0.1 "${GO_PORT}" "App2" "secret2"
  fi
} | tee "${OUT_FILE}"

echo
echo "Results written to ${OUT_FILE}"
