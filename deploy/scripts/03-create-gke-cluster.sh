#!/usr/bin/env bash
# Create a GKE cluster for running cache-custom.
# Purpose: infra one-time setup. Prefer Autopilot for less node ops; Standard for a cheap zonal dev cluster.
# Re-runnable: exits successfully if the cluster already exists.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID REGION CLUSTER_NAME CLUSTER_TYPE

exists=false
if [[ "${CLUSTER_TYPE}" == "standard" ]]; then
  if gcloud container clusters describe "${CLUSTER_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1; then
    exists=true
  fi
else
  if gcloud container clusters describe "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1; then
    exists=true
  fi
fi

if [[ "${exists}" == "true" ]]; then
  log "Cluster already exists: ${CLUSTER_NAME}"
  exit 0
fi

case "${CLUSTER_TYPE}" in
  autopilot)
    # Autopilot: Google manages nodes; you pay per pod requests.
    log "Creating Autopilot cluster ${CLUSTER_NAME} in ${REGION}..."
    gcloud container clusters create-auto "${CLUSTER_NAME}" \
      --region="${REGION}" \
      --project="${PROJECT_ID}" \
      --release-channel=regular
    ;;
  standard)
    # Small zonal cluster for lower fixed cost in dev (you manage nodes).
    log "Creating Standard zonal cluster ${CLUSTER_NAME} in ${ZONE}..."
    gcloud container clusters create "${CLUSTER_NAME}" \
      --zone="${ZONE}" \
      --project="${PROJECT_ID}" \
      --num-nodes=1 \
      --machine-type=e2-small \
      --disk-size=30 \
      --release-channel=regular
    ;;
  *)
    echo "error: CLUSTER_TYPE must be autopilot or standard (got ${CLUSTER_TYPE})" >&2
    exit 1
    ;;
esac

log "Cluster create finished: ${CLUSTER_NAME}"
