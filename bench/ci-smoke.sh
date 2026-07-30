#!/usr/bin/env bash
# Lightweight local regression smoke for CI / pre-merge checks.
# Purpose: prove redis-benchmark can talk to go_cache (AUTH + SET/GET) without full matrix.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export BENCH_REQUESTS="${BENCH_REQUESTS:-5000}"
export BENCH_CLIENTS="${BENCH_CLIENTS:-10}"
exec "${ROOT}/bench/run-local.sh" --smoke
