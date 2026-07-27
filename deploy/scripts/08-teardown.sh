#!/usr/bin/env bash
# Tear down cost-incurring cache-custom GCP/GKE resources created by the deploy scripts.
# Purpose: stop billing for Autopilot/Standard cluster + Artifact Registry storage.
#
# Deletes (when present):
#   1. Helm release + Kubernetes namespace (workloads, Services, Secrets, PVCs)
#   2. GKE cluster
#   3. Artifact Registry Docker repository (all images in it)
#
# Does NOT delete: the GCP project, enabled APIs, or unrelated resources.
# Re-runnable: missing resources are skipped.
#
# Usage:
#   ./deploy/scripts/08-teardown.sh           # interactive confirm
#   CONFIRM_TEARDOWN=yes ./deploy/scripts/08-teardown.sh   # non-interactive

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID REGION CLUSTER_NAME AR_REPOSITORY

log "Teardown target:"
log "  project=${PROJECT_ID}"
log "  cluster=${CLUSTER_NAME} (${CLUSTER_TYPE}, region/zone via env)"
log "  artifact_registry=${AR_REPOSITORY} @ ${REGION}"
log "  helm_release=${HELM_RELEASE} namespace=${NAMESPACE}"

if [[ "${CONFIRM_TEARDOWN:-}" != "yes" ]]; then
  read -r -p "Type 'yes' to permanently delete these resources: " ans
  if [[ "${ans}" != "yes" ]]; then
    log "Aborted."
    exit 1
  fi
fi

# --- 1) In-cluster resources (needs kubectl + credentials) -----------------
if command -v kubectl >/dev/null 2>&1 && command -v helm >/dev/null 2>&1; then
  # Best-effort credentials; if cluster already gone, skip K8s cleanup.
  set +e
  if [[ "${CLUSTER_TYPE}" == "standard" ]]; then
    gcloud container clusters get-credentials "${CLUSTER_NAME}" \
      --zone="${ZONE}" --project="${PROJECT_ID}" 2>/dev/null
  else
    gcloud container clusters get-credentials "${CLUSTER_NAME}" \
      --region="${REGION}" --project="${PROJECT_ID}" 2>/dev/null
  fi
  creds_ok=$?
  set -e

  if [[ "${creds_ok}" -eq 0 ]]; then
    if helm -n "${NAMESPACE}" status "${HELM_RELEASE}" >/dev/null 2>&1; then
      log "Helm uninstall ${HELM_RELEASE} (namespace ${NAMESPACE})..."
      helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}" --wait --timeout 5m || true
    else
      log "Helm release ${HELM_RELEASE} not found; skipping uninstall."
    fi

    if kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      log "Deleting namespace ${NAMESPACE}..."
      kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=5m || true
    else
      log "Namespace ${NAMESPACE} not found; skipping."
    fi
  else
    log "Could not get cluster credentials (cluster may already be gone); skipping K8s cleanup."
  fi
else
  log "kubectl/helm not available; skipping in-cluster cleanup."
fi

# --- 2) GKE cluster --------------------------------------------------------
log "Deleting GKE cluster ${CLUSTER_NAME} (if it exists)..."
set +e
if [[ "${CLUSTER_TYPE}" == "standard" ]]; then
  gcloud container clusters describe "${CLUSTER_NAME}" \
    --zone="${ZONE}" --project="${PROJECT_ID}" >/dev/null 2>&1
  exists=$?
  if [[ "${exists}" -eq 0 ]]; then
    gcloud container clusters delete "${CLUSTER_NAME}" \
      --zone="${ZONE}" --project="${PROJECT_ID}" --quiet
  else
    log "Cluster ${CLUSTER_NAME} not found in zone ${ZONE}."
  fi
else
  gcloud container clusters describe "${CLUSTER_NAME}" \
    --region="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1
  exists=$?
  if [[ "${exists}" -eq 0 ]]; then
    gcloud container clusters delete "${CLUSTER_NAME}" \
      --region="${REGION}" --project="${PROJECT_ID}" --quiet
  else
    log "Cluster ${CLUSTER_NAME} not found in region ${REGION}."
  fi
fi
set -e

# --- 3) Artifact Registry repository (images + storage) --------------------
log "Deleting Artifact Registry repository ${AR_REPOSITORY} (if it exists)..."
set +e
gcloud artifacts repositories describe "${AR_REPOSITORY}" \
  --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1
ar_exists=$?
if [[ "${ar_exists}" -eq 0 ]]; then
  gcloud artifacts repositories delete "${AR_REPOSITORY}" \
    --location="${REGION}" --project="${PROJECT_ID}" --quiet
  log "Deleted Artifact Registry repo ${AR_REPOSITORY}."
else
  log "Artifact Registry repo ${AR_REPOSITORY} not found; skipping."
fi
set -e

log "Teardown finished. Cost drivers removed: GKE cluster, AR images/repo, Helm workloads."
log "Project ${PROJECT_ID} and enabled APIs remain (no ongoing cluster charge)."
