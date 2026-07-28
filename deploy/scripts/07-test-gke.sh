#!/usr/bin/env bash
# End-to-end smoke test of a deployed release on GKE.
# Purpose: pods ready, HTTP health, RESP, and (when persistence is on) data survives pod kill.
# Requires: kubectl context, release installed.

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
  kubectl -n "${NAMESPACE}" rollout status "${APP}" --timeout=300s
else
  kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "${LABEL}" --timeout=300s
fi

log "Pods:"
kubectl -n "${NAMESPACE}" get pods -l "${LABEL}" -o wide

# Autopilot can take several minutes to schedule one-shot smoke pods (image pull + scale-up).
wait_pod_succeeded() {
  local pod="$1"
  local timeout_s="${2:-300}"
  local elapsed=0
  local phase=Missing
  while (( elapsed < timeout_s )); do
    phase="$(kubectl -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.status.phase}' 2>/dev/null || echo Missing)"
    case "${phase}" in
      Succeeded) return 0 ;;
      Failed)
        kubectl -n "${NAMESPACE}" logs "${pod}" || true
        echo "error: pod ${pod} failed" >&2
        return 1
        ;;
    esac
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "error: timed out waiting for pod ${pod} (last phase=${phase:-unknown})" >&2
  kubectl -n "${NAMESPACE}" describe pod "${pod}" | tail -30 || true
  return 1
}

# Run redis-cli in a one-shot pod; args after -- are passed to sh -c script body via env-style.
# $1 = unique pod name suffix, remaining = shell script for redis-cli
run_redis_script() {
  local name="$1"
  shift
  local script="$*"
  kubectl -n "${NAMESPACE}" delete pod "${name}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "${NAMESPACE}" run "${name}" --restart=Never --image=redis:7-alpine --command -- \
    sh -c "${script}"
  wait_pod_succeeded "${name}" 300
  kubectl -n "${NAMESPACE}" logs "${name}"
  kubectl -n "${NAMESPACE}" delete pod "${name}" --ignore-not-found >/dev/null 2>&1 || true
}

log "HTTP health via in-cluster curl (metrics port)..."
# Prefer in-cluster curl over local port-forward (more reliable in CI/non-interactive shells).
kubectl -n "${NAMESPACE}" delete pod cache-custom-health --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "${NAMESPACE}" run cache-custom-health --restart=Never --image=curlimages/curl:8.5.0 --command -- \
  sh -c "curl -sf http://${SVC}:${METRICS_PORT}/healthz | grep -q ok && curl -sf http://${SVC}:${METRICS_PORT}/readyz | grep -q ok && echo HEALTH_OK"
wait_pod_succeeded cache-custom-health 300
kubectl -n "${NAMESPACE}" logs cache-custom-health
kubectl -n "${NAMESPACE}" delete pod cache-custom-health --ignore-not-found >/dev/null 2>&1 || true
log "healthz/readyz OK"

log "RESP smoke test via ephemeral redis-cli pod..."
run_redis_script cache-custom-smoke "
  set -e
  redis-cli -h ${SVC} -p ${RESP_PORT} PING | grep -q PONG
  redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 SET m9smoke hello | grep -q OK
  redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 GET m9smoke | grep -q hello
  echo RESP_SMOKE_OK
"
log "RESP smoke OK"

# ---------------------------------------------------------------------------
# Persistence: only when release is a StatefulSet (PVC-backed) or env says so.
# Kill the cache pod, wait for replacement, confirm key still present on disk path.
# ---------------------------------------------------------------------------
HAS_STS=false
if kubectl -n "${NAMESPACE}" get sts -l "${LABEL}" --no-headers 2>/dev/null | grep -q .; then
  HAS_STS=true
fi

if [[ "${PERSISTENCE_ENABLED:-false}" == "true" || "${HAS_STS}" == "true" ]]; then
  log "Persistence smoke: write key, delete pod, verify restore from PVC..."

  # Distinct key so this step is self-contained.
  run_redis_script cache-custom-persist-write "
    set -e
    redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 SET m9persist survived-pod-kill | grep -q OK
    # Brief pause so AOF always fsync can settle (Mode=aof, AOFFsync=always in values-persistence).
    sleep 1
    redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 GET m9persist | grep -q survived-pod-kill
    echo PERSIST_WRITE_OK
  "

  POD="$(kubectl -n "${NAMESPACE}" get pod -l "${LABEL}" -o jsonpath='{.items[0].metadata.name}')"
  if [[ -z "${POD}" ]]; then
    echo "error: no cache pod to delete" >&2
    exit 1
  fi
  log "Deleting cache pod ${POD} (PVC should retain data)..."
  kubectl -n "${NAMESPACE}" delete pod "${POD}" --wait=true --timeout=120s

  log "Waiting for replacement pod Ready..."
  kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l "${LABEL}" --timeout=300s
  kubectl -n "${NAMESPACE}" get pods -l "${LABEL}" -o wide

  run_redis_script cache-custom-persist-read "
    set -e
    # After restart, data must come from AOF/PVC — not the killed process memory.
    redis-cli -h ${SVC} -p ${RESP_PORT} --user App1 --pass secret1 GET m9persist | grep -q survived-pod-kill
    echo PERSIST_RESTORE_OK
  "
  log "Persistence smoke OK (data survived pod deletion)."
else
  log "Skipping persistence smoke (no StatefulSet / PERSISTENCE_ENABLED!=true)."
fi

log "All GKE smoke checks passed."
