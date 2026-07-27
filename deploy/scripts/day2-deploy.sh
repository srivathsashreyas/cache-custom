#!/usr/bin/env bash
# Day-2 deploy: existing cluster — refresh creds, build/push image, helm upgrade, smoke test.
# Purpose: routine release without recreating registry or cluster.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"
require_cmd gcloud docker helm kubectl

log "=== 04 kubeconfig ==="
"${SCRIPT_DIR}/04-get-credentials.sh"

log "=== 05 build & push image ==="
"${SCRIPT_DIR}/05-build-push-image.sh"

log "=== 06 helm deploy ==="
"${SCRIPT_DIR}/06-helm-deploy.sh"

log "=== 07 test ==="
"${SCRIPT_DIR}/07-test-gke.sh"

log "Day-2 deploy finished successfully."
