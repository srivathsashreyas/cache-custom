#!/usr/bin/env bash
# End-to-end smoke test of a deployed release on GKE.
# Purpose: verify pods ready, HTTP health, and basic RESP (PING / AUTH / SET / GET).
# Requires: kubectl context, release installed, redis-cli OR a temporary redis image pod.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd kubectl
require_vars NAMESPACE HELM_RELEASE

# Resolve Service name from Helm instance labels (stable regardless of release name).
SVC="$(kubectl -n "${NAMESPACE}" get svc -l "app.kubernetes.io/instance=${HELM_RELEASE}" \
  -o jsonpath='{.items[0].metadata.name}')"
if [[ -z "${SVC}" ]]; then
  echo "error: no Service found for instance ${HELM_RELEASE} in ${NAMESPACE}" >&2
  exit 1
fi
log "Using Service ${SVC}"

METRICS_PORT=9090
RESP_PORT=9001
LABEL="app.kubernetes.io/instance=${HELM_RELEASE}"

log "Checking rollout / pods in ${NAMESPACE}..."
# Works for Deployment or StatefulSet named like the chart fullname.
APP="$(kubectl -n "${NAMESPACE}" get deploy,sts -l "${LABEL}" -o jsonpath='{.items[0].kind}/{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -n "${APP}" ]]; then
  kubectl -n "${NAMESPACE}" rollout status "${APP}" --timeout=180s
else
  kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "${LABEL}" --timeout=180s
fi

log "Pods:"
kubectl -n "${NAMESPACE}" get pods -l "${LABEL}" -o wide

log "HTTP health via in-cluster curl (metrics port)..."
# Prefer in-cluster curl over local port-forward (more reliable in CI/non-interactive shells).
kubectl -n "${NAMESPACE}" delete pod cache-custom-health --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" run cache-custom-health --restart=Never --image=curlimages/curl:8.5.0 --command -- \
  sh -c "curl -sf http://${SVC}:${METRICS_PORT}/healthz | grep -q ok && curl -sf http://${SVC}:${METRICS_PORT}/readyz | grep -q ok && echo HEALTH_OK"
kubectl -n "${NAMESPACE}" wait --for=condition=Succeeded pod/cache-custom-health --timeout=120s
kubectl -n "${NAMESPACE}" logs cache-custom-health
kubectl -n "${NAMESPACE}" delete pod cache-custom-health --ignore-not-found >/dev/null 2>&1 || true
log "healthz/readyz OK"

log "RESP smoke test via ephemeral redis-cli pod..."
# One redis-cli process so AUTH sticks for SET/GET (each CLI invocation is a new TCP conn).
kubectl -n "${NAMESPACE}" delete pod cache-custom-smoke --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" run cache-custom-smoke --restart=Never --image=redis:7-alpine --command -- \
  sh -c "
    set -e
    redis-cli -h ${SVC} -p ${RESP_PORT} PING | grep -q PONG
    redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 SET m9smoke hello | grep -q OK
    redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 GET m9smoke | grep -q hello
    echo RESP_SMOKE_OK
  "
kubectl -n "${NAMESPACE}" wait --for=condition=Succeeded pod/cache-custom-smoke --timeout=180s
kubectl -n "${NAMESPACE}" logs cache-custom-smoke
kubectl -n "${NAMESPACE}" delete pod cache-custom-smoke --ignore-not-found >/dev/null 2>&1 || true

log "All GKE smoke checks passed."
