#!/usr/bin/env bash
# GKE same-node baseline: deploy Redis co-located with cache-custom, run redis-benchmark Job.
# Purpose: M10 cloud comparison with both servers on one node (podAffinity).
#
# Prerequisites:
#   - GKE cluster + cache-custom release Running (day2-deploy / from-scratch)
#   - kubectl context set; helm already deployed cache-custom in namespace cache-custom
#   - deploy/.env loaded via common.sh if present
#
# Usage:
#   ./bench/run-gke.sh
#   BENCH_REQUESTS=10000 ./bench/run-gke.sh --smoke

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT}/deploy/scripts/common.sh" ]]; then
  # shellcheck source=../deploy/scripts/common.sh
  source "${ROOT}/deploy/scripts/common.sh"
fi

SMOKE=false
if [[ "${1:-}" == "--smoke" ]]; then
  SMOKE=true
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: need $1" >&2; exit 1; }
}
require_cmd kubectl

export USE_GKE_GCLOUD_AUTH_PLUGIN="${USE_GKE_GCLOUD_AUTH_PLUGIN:-True}"
NAMESPACE_CACHE="${NAMESPACE:-cache-custom}"
LABEL="app.kubernetes.io/name=cache-custom"

log() { echo "[bench-gke] $*"; }

# Ensure cache-custom is up so affinity can bind Redis to its node.
if ! kubectl get pods -A -l "${LABEL}" --field-selector=status.phase=Running 2>/dev/null | grep -q .; then
  echo "error: no Running cache-custom pod found. Deploy go_cache first (deploy/scripts/day2-deploy.sh)." >&2
  exit 1
fi

CACHE_NODE="$(kubectl get pods -A -l "${LABEL}" -o jsonpath='{.items[0].spec.nodeName}')"
log "cache-custom is on node: ${CACHE_NODE}"

log "Applying Redis same-node Deployment..."
kubectl apply -f "${ROOT}/bench/k8s/redis-same-node.yaml"

log "Waiting for redis-bench Ready..."
kubectl -n cache-bench rollout status deploy/redis-bench --timeout=300s
REDIS_NODE="$(kubectl -n cache-bench get pods -l app=redis-bench -o jsonpath='{.items[0].spec.nodeName}')"
log "redis-bench is on node: ${REDIS_NODE}"
if [[ "${CACHE_NODE}" != "${REDIS_NODE}" ]]; then
  echo "error: Redis not on same node as cache-custom (${CACHE_NODE} vs ${REDIS_NODE})" >&2
  exit 1
fi
log "Same-node co-location confirmed."

# Patch job request count for smoke
JOB_FILE="${ROOT}/bench/k8s/bench-job.yaml"
kubectl -n cache-bench delete job redis-benchmark-job --ignore-not-found >/dev/null 2>&1 || true
if [[ "${SMOKE}" == "true" ]]; then
  log "Applying benchmark Job (smoke requests=10000)..."
  sed 's/value: "50000"/value: "10000"/' "${JOB_FILE}" | kubectl apply -f -
else
  log "Applying benchmark Job..."
  kubectl apply -f "${JOB_FILE}"
fi

log "Waiting for Job completion..."
kubectl -n cache-bench wait --for=condition=complete job/redis-benchmark-job --timeout=600s

OUT_DIR="${ROOT}/bench/results"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/gke-${STAMP}.txt"
{
  echo "cache-custom GKE same-node benchmark"
  echo "timestamp_utc=${STAMP}"
  echo "node=${CACHE_NODE}"
  echo "cache_namespace=${NAMESPACE_CACHE}"
  kubectl -n cache-bench logs job/redis-benchmark-job
} | tee "${OUT_FILE}"

log "Results written to ${OUT_FILE}"
log "Cleanup: kubectl delete -f bench/k8s/redis-same-node.yaml; kubectl -n cache-bench delete job redis-benchmark-job"
