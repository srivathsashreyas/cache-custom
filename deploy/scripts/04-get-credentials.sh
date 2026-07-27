#!/usr/bin/env bash
# Fetch GKE credentials into the local kubeconfig.
# Purpose: point kubectl/helm at the cluster (re-run after laptop switch or expired config).
# Requires: gcloud auth with permission to get cluster credentials.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID CLUSTER_NAME CLUSTER_TYPE

# kubectl talks to GKE via an exec plugin that exchanges your gcloud identity for a
# short-lived token. Without the plugin, helm/kubectl fail even when gcloud works.
ensure_gke_auth_plugin() {
  export USE_GKE_GCLOUD_AUTH_PLUGIN=True
  if command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
    return 0
  fi
  log "gke-gcloud-auth-plugin not found; attempting install..."
  # GitHub-hosted runners / Debian: apt package (gcloud components often disabled).
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -qq >/dev/null 2>&1 || true
    if sudo apt-get install -y -qq google-cloud-cli-gke-gcloud-auth-plugin 2>/dev/null ||
      sudo apt-get install -y -qq google-cloud-sdk-gke-gcloud-auth-plugin 2>/dev/null; then
      log "Installed gke-gcloud-auth-plugin via apt."
      return 0
    fi
  fi
  # Local SDK installs may still use components manager.
  if gcloud components install gke-gcloud-auth-plugin --quiet 2>/dev/null; then
    log "Installed gke-gcloud-auth-plugin via gcloud components."
    return 0
  fi
  echo "error: install gke-gcloud-auth-plugin (see https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin)" >&2
  exit 1
}

ensure_gke_auth_plugin
require_cmd kubectl

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
