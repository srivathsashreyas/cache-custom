#!/usr/bin/env bash
# Create a Docker Artifact Registry repository for cache-custom images.
# Purpose: place to push images that GKE nodes can pull (same project/region).
# Re-runnable: skips create if the repository already exists.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID REGION AR_REPOSITORY

if gcloud artifacts repositories describe "${AR_REPOSITORY}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1; then
  log "Artifact Registry repository already exists: ${AR_REPOSITORY} (${REGION})"
  exit 0
fi

log "Creating Artifact Registry Docker repo ${AR_REPOSITORY} in ${REGION}..."
gcloud artifacts repositories create "${AR_REPOSITORY}" \
  --repository-format=docker \
  --location="${REGION}" \
  --description="cache-custom container images" \
  --project="${PROJECT_ID}"

log "Created. Image path prefix: ${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/"
