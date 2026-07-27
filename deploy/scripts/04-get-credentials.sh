#!/usr/bin/env bash
# Fetch GKE credentials into the local kubeconfig.
# Purpose: point kubectl/helm at the cluster (re-run after laptop switch or expired config).
# Requires: gcloud auth with permission to get cluster credentials.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID CLUSTER_NAME CLUSTER_TYPE

if [[ "${CLUSTER_TYPE}" == "standard" ]]; then
  require_vars ZONE
  log "Fetching credentials for zonal cluster ${CLUSTER_NAME} (${ZONE})..."
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}"
else
  require_vars REGION
  log "Fetching credentials for regional cluster ${CLUSTER_NAME} (${REGION})..."
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}"
fi

log "kubeconfig updated. Current context:"
kubectl config current-context 2>/dev/null || true
