#!/usr/bin/env bash
# Enable GCP APIs required for GKE + Artifact Registry.
# Purpose: one-time (re-runnable) project setup before creating cluster or registry.
# Requires: gcloud authenticated to PROJECT_ID with permission to enable services.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID

log "Enabling APIs on project ${PROJECT_ID}..."
# container.googleapis.com     — GKE
# artifactregistry.googleapis.com — Docker image repository
# compute.googleapis.com       — VMs / networking used by GKE
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  --project="${PROJECT_ID}"

log "APIs enabled."
