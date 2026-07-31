#!/usr/bin/env bash
# GKE same-node baseline: multi-tenant fair matrix (policy × strategy) vs co-located Redis.
# Purpose: M10 cloud comparison; Redis maxmemory/policy match each go_cache tenant.
#
# Prerequisites:
#   - GKE cluster + cache-custom release Running (day2-deploy / from-scratch)
#   - kubectl context set; helm already deployed cache-custom
#   - deploy/.env loaded via common.sh if present
#
# Note: helm upgrade injects a generated multi-tenant serverConfig for the matrix.
# Re-run day2-deploy / 06-helm-deploy afterward to restore normal App1/App2 config.
#
# Usage:
#   ./bench/run-gke.sh
#   ./bench/run-gke.sh --smoke

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT}/deploy/scripts/common.sh" ]]; then
  # shellcheck source=../deploy/scripts/common.sh
  source "${ROOT}/deploy/scripts/common.sh"
fi
# shellcheck source=matrix.sh
source "${ROOT}/bench/matrix.sh"

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
CFG="${ROOT}/bench/configs/go-cache-bench.generated.json"

log() { echo "[bench-gke] $*"; }

if [[ ! -d "${CHART_PATH}" ]]; then
  echo "error: chart not found at ${CHART_PATH}" >&2
  exit 1
fi

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

matrix_select "${SMOKE}"
mkdir -p "${ROOT}/bench/configs"
gen_go_cache_config "${CFG}"
log "Generated matrix config (${#STRATEGIES_TO_RUN[@]} strategies × ${#POLICIES_TO_RUN[@]} policies) → ${CFG}"

log "Removing previous benchmark Job (if any)..."
kubectl -n "${NAMESPACE_CACHE}" delete job -l "app.kubernetes.io/component=redis-benchmark,${INSTANCE_LABEL}" \
  --ignore-not-found >/dev/null 2>&1 || true

# serverConfig is a string field; --set-file injects the JSON body.
log "Helm upgrade: benchmark.enabled=true + matrix serverConfig (requests=${BENCH_REQUESTS_VAL})..."
helm upgrade "${HELM_REL}" "${CHART_PATH}" \
  --namespace "${NAMESPACE_CACHE}" \
  --reuse-values \
  --set benchmark.enabled=true \
  --set "benchmark.job.requests=${BENCH_REQUESTS_VAL}" \
  --set "benchmark.job.smoke=${SMOKE}" \
  --set "benchmark.matrix.maxMemory=${BENCH_MAX_MEMORY}" \
  --set "benchmark.matrix.password=${BENCH_PASS}" \
  --set-file "serverConfig=${CFG}" \
  --timeout 10m

log "Waiting for cache-custom rollout after config change..."
# Deployment or StatefulSet depending on persistence.
if kubectl -n "${NAMESPACE_CACHE}" get sts -l "${INSTANCE_LABEL}" -o name 2>/dev/null | grep -q .; then
  STS_NAME="$(kubectl -n "${NAMESPACE_CACHE}" get sts -l "${INSTANCE_LABEL}" -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "${NAMESPACE_CACHE}" rollout status "sts/${STS_NAME}" --timeout=300s
else
  DEP_NAME="$(kubectl -n "${NAMESPACE_CACHE}" get deploy -l "${INSTANCE_LABEL}" \
    -o jsonpath='{range .items[?(@.metadata.labels.app\.kubernetes\.io/component!="redis-bench")]}{.metadata.name}{"\n"}{end}' | head -1)"
  # Prefer main app deploy (not redis-bench).
  if [[ -z "${DEP_NAME}" ]]; then
    DEP_NAME="$(kubectl -n "${NAMESPACE_CACHE}" get deploy -l "${INSTANCE_LABEL}" -o jsonpath='{.items[0].metadata.name}')"
  fi
  kubectl -n "${NAMESPACE_CACHE}" rollout status "deploy/${DEP_NAME}" --timeout=300s || true
fi

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
  # Node may change after rollout; refresh cache node.
  CACHE_NODE="$(kubectl get pods -n "${NAMESPACE_CACHE}" -l "${INSTANCE_LABEL}" \
    -o jsonpath='{range .items[*]}{.metadata.labels.app\.kubernetes\.io/component}{"\t"}{.spec.nodeName}{"\n"}{end}' \
    | awk -F'\t' '$1 != "redis-bench" && $1 != "redis-benchmark" {print $2; exit}')"
  log "cache-custom node after rollout: ${CACHE_NODE}"
fi
if [[ "${CACHE_NODE}" != "${REDIS_NODE}" ]]; then
  echo "error: Redis not on same node as cache-custom (${CACHE_NODE} vs ${REDIS_NODE})" >&2
  exit 1
fi
log "Same-node co-location confirmed."

log "Waiting for Job completion (${JOB_NAME})..."
kubectl -n "${NAMESPACE_CACHE}" wait --for=condition=complete "job/${JOB_NAME}" --timeout=1800s

OUT_DIR="${ROOT}/bench/results"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="${OUT_DIR}/gke-${STAMP}.txt"
{
  echo "cache-custom GKE same-node benchmark"
  echo "timestamp_utc=${STAMP}"
  echo "node=${CACHE_NODE}"
  echo "cache_namespace=${NAMESPACE_CACHE}"
  echo "fair_baseline=same MaxMemory + EvictionPolicy for Redis and go_cache tenants"
  echo "strategies=${STRATEGIES_TO_RUN[*]} policies=${POLICIES_TO_RUN[*]}"
  kubectl -n "${NAMESPACE_CACHE}" logs "job/${JOB_NAME}"
} | tee "${OUT_FILE}"

log "Results written to ${OUT_FILE}"
log "Disable bench + restore normal config: re-run deploy/scripts/06-helm-deploy.sh (or day2-deploy)"
log "Or: helm upgrade ${HELM_REL} ${CHART_PATH} -n ${NAMESPACE_CACHE} --reuse-values --set benchmark.enabled=false"
