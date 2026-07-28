#!/usr/bin/env bash
# Install or upgrade the cache-custom Helm release.
# Purpose: day-2 and from-scratch final step — apply chart to the current kube context.
# Requires: kubectl context already set (04-get-credentials.sh), image already pushed.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd helm kubectl
require_vars PROJECT_ID REGION AR_REPOSITORY IMAGE_NAME IMAGE_TAG NAMESPACE HELM_RELEASE

REPO="$(image_repository)"
CHART_PATH="${ROOT_DIR}/${HELM_CHART}"

if [[ ! -d "${CHART_PATH}" ]]; then
  echo "error: chart not found at ${CHART_PATH}" >&2
  exit 1
fi

log "Ensuring namespace ${NAMESPACE} exists..."
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"

# Base --set flags: image always from env (full AR path).
SET_ARGS=(
  --set "image.repository=${REPO}"
  --set "image.tag=${IMAGE_TAG}"
  --set "image.pullPolicy=IfNotPresent"
)

# Extra -f values files (e.g. persistence overlay).
VALUE_FILES=()

if [[ "${PERSISTENCE_ENABLED}" == "true" ]]; then
  # values-persistence.yaml: StatefulSet+PVC + serverConfig Mode=aof Dir=/data
  PERSIST_FILE="${CHART_PATH}/values-persistence.yaml"
  if [[ ! -f "${PERSIST_FILE}" ]]; then
    echo "error: persistence overlay missing: ${PERSIST_FILE}" >&2
    exit 1
  fi
  VALUE_FILES+=(-f "${PERSIST_FILE}")
  log "Persistence enabled (StatefulSet + PVC, AOF → /data)."
fi

# Optional extra sets from env (space-separated key=value pairs).
# shellcheck disable=SC2206
EXTRA=( ${HELM_EXTRA_SET:-} )
for kv in "${EXTRA[@]:-}"; do
  [[ -z "${kv}" ]] && continue
  SET_ARGS+=(--set "${kv}")
done

log "helm upgrade --install ${HELM_RELEASE} (namespace ${NAMESPACE})..."
helm upgrade --install "${HELM_RELEASE}" "${CHART_PATH}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  "${VALUE_FILES[@]}" \
  "${SET_ARGS[@]}" \
  --wait \
  --timeout 10m

log "Deploy complete."
kubectl -n "${NAMESPACE}" get pods,svc -l "app.kubernetes.io/instance=${HELM_RELEASE}"
