#!/usr/bin/env bash
# Tear down cost-incurring cache-custom GCP/GKE resources created by the deploy scripts.
# Purpose: stop billing for Autopilot/Standard cluster + Artifact Registry storage.
#
# Deletes (when present):
#   1. Helm release
#   2. PVCs in the namespace (and waits for them) — Helm often leaves StatefulSet PVCs
#   3. Kubernetes namespace
#   4. Any leftover PVs that referenced this namespace (Released/Available)
#   5. GKE cluster
#   6. Artifact Registry Docker repository (all images in it)
#   7. Best-effort: orphaned GCE PDs matching this cluster name (cost if left behind)
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

    # StatefulSet PVCs often survive helm uninstall; delete them explicitly so GCE PDs
    # are released (default StorageClass reclaimPolicy is usually Delete).
    if kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      if kubectl -n "${NAMESPACE}" get pvc --no-headers 2>/dev/null | grep -q .; then
        log "Deleting PVCs in namespace ${NAMESPACE} (releases underlying disks)..."
        kubectl -n "${NAMESPACE}" delete pvc --all --wait=true --timeout=5m || true
      else
        log "No PVCs in ${NAMESPACE}."
      fi

      log "Deleting namespace ${NAMESPACE}..."
      kubectl delete namespace "${NAMESPACE}" --wait=true --timeout=5m || true
    else
      log "Namespace ${NAMESPACE} not found; skipping."
    fi

    # PVs may linger in Released state if reclaim failed; drop ones that claim-bound here.
    log "Cleaning leftover PVs for namespace ${NAMESPACE} (if any)..."
    while read -r pv; do
      [[ -z "${pv}" ]] && continue
      claim_ns="$(kubectl get pv "${pv}" -o jsonpath='{.spec.claimRef.namespace}' 2>/dev/null || true)"
      if [[ "${claim_ns}" == "${NAMESPACE}" ]]; then
        log "  deleting PV ${pv}"
        kubectl delete pv "${pv}" --wait=true --timeout=2m || true
      fi
    done < <(kubectl get pv -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
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

# Best-effort: disks whose name still references this GKE cluster after cluster delete.
# Dynamic provisioner names often include the cluster name; unused disks still bill.
log "Scanning for leftover GCE disks matching cluster ${CLUSTER_NAME}..."
# Unattached disks only (still bill). Names often include the GKE cluster name.
set +e
while read -r dname dzone; do
  [[ -z "${dname}" || -z "${dzone}" ]] && continue
  log "  deleting unattached disk ${dname} (zone ${dzone})..."
  gcloud compute disks delete "${dname}" --zone="${dzone}" --project="${PROJECT_ID}" --quiet || true
done < <(gcloud compute disks list --project="${PROJECT_ID}" \
  --filter="name~${CLUSTER_NAME} AND NOT users:*" \
  --format="value(name,zone)" 2>/dev/null)
set -e

log "Teardown finished. Cost drivers removed: GKE cluster, PVCs/PVs/disks (best-effort), AR images/repo."
log "Project ${PROJECT_ID} and enabled APIs remain (no ongoing cluster charge)."
