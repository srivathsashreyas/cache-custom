#!/usr/bin/env bash
# Build the Docker image and push it to Artifact Registry.
# Purpose: produce the image reference used by Helm (image.repository / image.tag).
# Requires: docker (or compatible), gcloud auth configure-docker for the region.

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud docker
require_vars PROJECT_ID REGION AR_REPOSITORY IMAGE_NAME IMAGE_TAG

REPO="$(image_repository)"
REF="$(image_ref)"

log "Configuring docker auth for ${REGION}-docker.pkg.dev..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

log "Building ${REF} from repo root ${ROOT_DIR}..."
# linux/amd64: GKE nodes are x86_64; local Mac ARM builds otherwise fail with
# "no match for platform in manifest" on ImagePull.
docker build --platform=linux/amd64 -t "${REF}" -f "${ROOT_DIR}/Dockerfile" "${ROOT_DIR}"

log "Pushing ${REF}..."
docker push "${REF}"

log "Pushed. Use with Helm: --set image.repository=${REPO} --set image.tag=${IMAGE_TAG}"
