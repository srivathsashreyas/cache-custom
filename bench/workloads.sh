#!/usr/bin/env bash
# Shared redis-benchmark workloads for local and GKE same-node runs.
# Purpose: one matrix definition so local vs GKE numbers stay comparable.
#
# Env (optional):
#   BENCH_REQUESTS  default 100000 (use 10000 for smoke)
#   BENCH_CLIENTS   default 50
#   BENCH_PIPELINE  default 1 (set 16 for pipeline workload)
#   BENCH_KEYSPACE  default 10000

set -euo pipefail

BENCH_REQUESTS="${BENCH_REQUESTS:-100000}"
BENCH_CLIENTS="${BENCH_CLIENTS:-50}"
BENCH_PIPELINE="${BENCH_PIPELINE:-1}"
BENCH_KEYSPACE="${BENCH_KEYSPACE:-10000}"

require_redis_benchmark() {
  if ! command -v redis-benchmark >/dev/null 2>&1; then
    echo "error: redis-benchmark not found (install redis tools)" >&2
    exit 1
  fi
}

# Run one named workload against host:port with optional ACL user/pass.
# Args: name host port [user] [pass]
run_workload() {
  local name="$1" host="$2" port="$3"
  local user="${4:-}" pass="${5:-}"
  local auth_args=()
  # redis-benchmark: ACL style is --user NAME -a PASS (not --pass).
  if [[ -n "${user}" && -n "${pass}" ]]; then
    auth_args=(--user "${user}" -a "${pass}")
  elif [[ -n "${pass}" ]]; then
    auth_args=(-a "${pass}")
  fi

  echo
  echo "======== workload=${name} target=${host}:${port} requests=${BENCH_REQUESTS} clients=${BENCH_CLIENTS} pipeline=${BENCH_PIPELINE} ========"

  # -t selects tests; -q quieter summary; --csv machine-readable; -n/-c/-P/-r matrix knobs.
  # ${auth_args[@]+...} avoids unbound-variable under `set -u` when no AUTH.
  redis-benchmark -h "${host}" -p "${port}" \
    ${auth_args[@]+"${auth_args[@]}"} \
    -n "${BENCH_REQUESTS}" \
    -c "${BENCH_CLIENTS}" \
    -P "${BENCH_PIPELINE}" \
    -r "${BENCH_KEYSPACE}" \
    -t set,get \
    -q \
    --csv
}

# Full matrix for a target (host port [user] [pass])
run_matrix() {
  local host="$1" port="$2"
  local user="${3:-}" pass="${4:-}"
  local label="${5:-target}"

  BENCH_PIPELINE=1 run_workload "${label}/set-get" "${host}" "${port}" "${user}" "${pass}"
  BENCH_PIPELINE=16 run_workload "${label}/set-get-pipeline16" "${host}" "${port}" "${user}" "${pass}"
}
