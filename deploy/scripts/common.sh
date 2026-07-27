#!/usr/bin/env bash
# Shared helpers for deploy scripts.
# Purpose: load deploy/.env, derive image URLs, and fail fast on missing tools/vars.
# Sourced by 01–07 scripts — not meant to be run directly.

set -euo pipefail

# Repo root = parent of deploy/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEPLOY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load local env if present (copy from .env.example).
ENV_FILE="${DEPLOY_DIR}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  # Export all assignments in .env for child processes.
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

# Defaults (match .env.example) so scripts work with partial env.
: "${PROJECT_ID:=}"
: "${REGION:=us-central1}"
: "${CLUSTER_NAME:=cache-custom}"
: "${CLUSTER_TYPE:=autopilot}"
: "${ZONE:=us-central1-a}"
: "${AR_REPOSITORY:=cache-custom}"
: "${IMAGE_NAME:=cache-custom}"
: "${IMAGE_TAG:=latest}"
: "${NAMESPACE:=cache-custom}"
: "${HELM_RELEASE:=cache-custom}"
: "${HELM_CHART:=deploy/helm/cache-custom}"
: "${PERSISTENCE_ENABLED:=false}"

# Full Artifact Registry image reference (no tag).
image_repository() {
  echo "${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/${IMAGE_NAME}"
}

image_ref() {
  echo "$(image_repository):${IMAGE_TAG}"
}

require_cmd() {
  local c
  for c in "$@"; do
    if ! command -v "${c}" >/dev/null 2>&1; then
      echo "error: required command not found: ${c}" >&2
      exit 1
    fi
  done
}

require_vars() {
  local v
  for v in "$@"; do
    if [[ -z "${!v:-}" ]]; then
      echo "error: required env var ${v} is empty — set it in deploy/.env (see deploy/.env.example)" >&2
      exit 1
    fi
  done
}

log() {
  echo "[deploy] $*"
}
