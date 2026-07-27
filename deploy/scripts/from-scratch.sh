#!/usr/bin/env bash
# Run the full from-scratch GKE path in order.
# Purpose: one entrypoint for a new project/cluster (APIs → registry → cluster → creds → image → helm → test).
# Requires: deploy/.env filled in; gcloud authenticated; docker available.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"
require_cmd gcloud docker helm kubectl

log "=== 01 enable APIs ==="
"${SCRIPT_DIR}/01-enable-apis.sh"

log "=== 02 Artifact Registry ==="
"${SCRIPT_DIR}/02-create-artifact-registry.sh"

log "=== 03 GKE cluster ==="
"${SCRIPT_DIR}/03-create-gke-cluster.sh"

log "=== 04 kubeconfig ==="
"${SCRIPT_DIR}/04-get-credentials.sh"

log "=== 05 build & push image ==="
"${SCRIPT_DIR}/05-build-push-image.sh"

log "=== 06 helm deploy ==="
"${SCRIPT_DIR}/06-helm-deploy.sh"

log "=== 07 test ==="
"${SCRIPT_DIR}/07-test-gke.sh"

log "From-scratch deploy finished successfully."
