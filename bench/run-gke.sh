#!/usr/bin/env bash
# GKE same-node baseline: enable chart benchmark templates, run redis-benchmark Job.
# Purpose: M10 cloud comparison with Redis co-located via chart podAffinity (selectorLabels).
#
# Prerequisites:
#   - GKE cluster + cache-custom release Running (day2-deploy / from-scratch)
#   - kubectl context set; helm already deployed cache-custom
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
require_cmd kubectl helm

export USE_GKE_GCLOUD_AUTH_PLUGIN="${USE_GKE_GCLOUD_AUTH_PLUGIN:-True}"
NAMESPACE_CACHE="${NAMESPACE:-cache-custom}"
HELM_REL="${HELM_RELEASE:-cache-custom}"
CHART_PATH="${ROOT}/${HELM_CHART:-deploy/helm/cache-custom}"
INSTANCE_LABEL="app.kubernetes.io/instance=${HELM_REL}"

log() { echo "[bench-gke] $*"; }

if [[ ! -d "${CHART_PATH}" ]]; then
  echo "error: chart not found at ${CHART_PATH}" >&2
  exit 1
fi

# Main cache pods: release instance, not the redis-bench / job components.
CACHE_POD="$(kubectl get pods -n "${NAMESPACE_CACHE}" -l "${INSTANCE_LABEL}" \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.app\.kubernetes\.io/component}{"\t"}{.spec.nodeName}{"\n"}{end}' \
  2>/dev/null | awk -F'\t' '$2 != "redis-bench" && $2 != "redis-benchmark" {print; exit}')"

if [[ -z "${CACHE_POD}" ]]; then
  echo "error: no Running cache-custom pod in ${NAMESPACE_CACHE}. Deploy go_cache first (deploy/scripts/day2-deploy.sh)." >&2
  exit 1
fi
CACHE_NODE="$(echo "${CACHE_POD}" | awk -F'\t' '{print $3}')"
log "cache-custom is on node: ${CACHE_NODE}"

BENCH_REQUESTS_VAL=50000
if [[ "${SMOKE}" == "true" ]]; then
  BENCH_REQUESTS_VAL=10000
fi
BENCH_REQUESTS_VAL="${BENCH_REQUESTS:-${BENCH_REQUESTS_VAL}}"

# Jobs are immutable: delete previous run before helm recreates it.
log "Removing previous benchmark Job (if any)..."
kubectl -n "${NAMESPACE_CACHE}" delete job -l "app.kubernetes.io/component=redis-benchmark,${INSTANCE_LABEL}" \
  --ignore-not-found >/dev/null 2>&1 || true

log "Helm upgrade: benchmark.enabled=true (requests=${BENCH_REQUESTS_VAL})..."
# No --wait: Job completion is handled below after same-node checks.
helm upgrade "${HELM_REL}" "${CHART_PATH}" \
  --namespace "${NAMESPACE_CACHE}" \
  --reuse-values \
  --set benchmark.enabled=true \
  --set "benchmark.job.requests=${BENCH_REQUESTS_VAL}" \
  --timeout 10m

REDIS_DEPLOY="$(kubectl -n "${NAMESPACE_CACHE}" get deploy \
  -l "app.kubernetes.io/component=redis-bench,${INSTANCE_LABEL}" \
  -o jsonpath='{.items[0].metadata.name}')"
JOB_NAME="$(kubectl -n "${NAMESPACE_CACHE}" get job \
  -l "app.kubernetes.io/component=redis-benchmark,${INSTANCE_LABEL}" \
  -o jsonpath='{.items[0].metadata.name}')"

log "Waiting for redis-bench Ready (${REDIS_DEPLOY})..."
kubectl -n "${NAMESPACE_CACHE}" rollout status "deploy/${REDIS_DEPLOY}" --timeout=300s
REDIS_NODE="$(kubectl -n "${NAMESPACE_CACHE}" get pods \
  -l "app.kubernetes.io/component=redis-bench,${INSTANCE_LABEL}" \
  -o jsonpath='{.items[0].spec.nodeName}')"
log "redis-bench is on node: ${REDIS_NODE}"
if [[ "${CACHE_NODE}" != "${REDIS_NODE}" ]]; then
  echo "error: Redis not on same node as cache-custom (${CACHE_NODE} vs ${REDIS_NODE})" >&2
  exit 1
fi
log "Same-node co-location confirmed."

log "Waiting for Job completion (${JOB_NAME})..."
kubectl -n "${NAMESPACE_CACHE}" wait --for=condition=complete "job/${JOB_NAME}" --timeout=600s

OUT_DIR="${ROOT}/bench/results"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/gke-${STAMP}.txt"
{
  echo "cache-custom GKE same-node benchmark"
  echo "timestamp_utc=${STAMP}"
  echo "node=${CACHE_NODE}"
  echo "cache_namespace=${NAMESPACE_CACHE}"
  kubectl -n "${NAMESPACE_CACHE}" logs "job/${JOB_NAME}"
} | tee "${OUT_FILE}"

log "Results written to ${OUT_FILE}"
log "Disable bench resources: helm upgrade ${HELM_REL} ${CHART_PATH} -n ${NAMESPACE_CACHE} --reuse-values --set benchmark.enabled=false"
