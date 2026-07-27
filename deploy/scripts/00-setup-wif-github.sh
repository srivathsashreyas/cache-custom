#!/usr/bin/env bash
# One-time: configure Workload Identity Federation so GitHub Actions can use GCP
# without a long-lived service account JSON key.
#
# Purpose:
#   GitHub OIDC token → trusted by a Workload Identity Provider → impersonates
#   a GCP deploy SA → short-lived credentials for gcloud/helm in CI.
#
# Re-runnable: creates missing pieces; skips existing pool/provider/SA; re-applies IAM.
#
# Prerequisites:
#   - gcloud auth login with permission to manage IAM + WIF on PROJECT_ID
#   - deploy/.env with PROJECT_ID set
#   - GITHUB_REPO=owner/name (defaults to srivathsashreyas/cache-custom if unset)
#
# After success, set these GitHub Actions *repository variables*:
#   GCP_WORKLOAD_IDENTITY_PROVIDER  (printed below — full provider resource name)
#   GCP_SERVICE_ACCOUNT             (printed below — SA email)
#   (+ existing deploy vars: GCP_PROJECT_ID, GKE_CLUSTER, etc.)
#
# Usage:
#   ./deploy/scripts/00-setup-wif-github.sh
#   GITHUB_REPO=myorg/myrepo ./deploy/scripts/00-setup-wif-github.sh

set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_cmd gcloud
require_vars PROJECT_ID

# Repo that is allowed to federate (must match assertion.repository in the OIDC token).
: "${GITHUB_REPO:=srivathsashreyas/cache-custom}"
# Stable IDs for pool/provider/SA (override via env if needed).
: "${WIF_POOL_ID:=github-pool}"
: "${WIF_PROVIDER_ID:=github-provider}"
: "${DEPLOY_SA_ID:=github-deploy}"

PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
SA_EMAIL="${DEPLOY_SA_ID}@${PROJECT_ID}.iam.gserviceaccount.com"
PROVIDER_RESOURCE="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WIF_POOL_ID}/providers/${WIF_PROVIDER_ID}"
# principalSet restricted to this GitHub repository attribute.
MEMBER="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WIF_POOL_ID}/attribute.repository/${GITHUB_REPO}"

log "WIF setup for project=${PROJECT_ID} number=${PROJECT_NUMBER}"
log "  github_repo=${GITHUB_REPO}"
log "  deploy_sa=${SA_EMAIL}"
log "  pool=${WIF_POOL_ID} provider=${WIF_PROVIDER_ID}"

# ---------------------------------------------------------------------------
# 1) APIs needed for WIF token exchange and IAM
#    Why: without iamcredentials / IAM APIs, federation and impersonation fail.
# ---------------------------------------------------------------------------
log "Enabling IAM / STS-related APIs..."
gcloud services enable \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  cloudresourcemanager.googleapis.com \
  sts.googleapis.com \
  --project="${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 2) Deploy service account (reuse if already present)
#    Why: CI should act as a dedicated identity with least-privilege roles, not
#    your user account. We re-use github-deploy when it already exists.
# ---------------------------------------------------------------------------
if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  log "Service account already exists: ${SA_EMAIL} (reusing)"
else
  log "Creating service account ${DEPLOY_SA_ID}..."
  gcloud iam service-accounts create "${DEPLOY_SA_ID}" \
    --project="${PROJECT_ID}" \
    --display-name="GitHub Actions deploy"
fi

# ---------------------------------------------------------------------------
# 3) Roles on the project for day-2 deploy (image push + GKE + helm)
#    Why: matches what deploy/scripts need (AR write, cluster access).
#    Re-applying bindings is idempotent.
# ---------------------------------------------------------------------------
log "Granting deploy roles to ${SA_EMAIL}..."
for role in \
  roles/container.developer \
  roles/container.clusterViewer \
  roles/artifactregistry.writer \
  roles/artifactregistry.reader
do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet >/dev/null
  log "  + ${role}"
done

# ---------------------------------------------------------------------------
# 4) Workload Identity Pool
#    Why: container for external identity providers (GitHub is one provider).
# ---------------------------------------------------------------------------
if gcloud iam workload-identity-pools describe "${WIF_POOL_ID}" \
  --project="${PROJECT_ID}" --location="global" >/dev/null 2>&1; then
  log "Workload Identity Pool already exists: ${WIF_POOL_ID}"
else
  log "Creating Workload Identity Pool ${WIF_POOL_ID}..."
  gcloud iam workload-identity-pools create "${WIF_POOL_ID}" \
    --project="${PROJECT_ID}" \
    --location="global" \
    --display-name="GitHub Actions"
fi

# ---------------------------------------------------------------------------
# 5) OIDC provider for GitHub Actions
#    Why: tells GCP to trust tokens issued by token.actions.githubusercontent.com
#    and map claims (repo, ref, subject) for IAM conditions.
#    attribute-condition: only this repository can federate (not every GitHub repo).
# ---------------------------------------------------------------------------
if gcloud iam workload-identity-pools providers describe "${WIF_PROVIDER_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${WIF_POOL_ID}" >/dev/null 2>&1; then
  log "OIDC provider already exists: ${WIF_PROVIDER_ID} (update attribute condition if repo changed)"
  # Keep mapping/condition in sync on re-run.
  gcloud iam workload-identity-pools providers update-oidc "${WIF_PROVIDER_ID}" \
    --project="${PROJECT_ID}" \
    --location="global" \
    --workload-identity-pool="${WIF_POOL_ID}" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor,attribute.ref=assertion.ref" \
    --attribute-condition="assertion.repository=='${GITHUB_REPO}'" \
    --quiet
else
  log "Creating OIDC provider ${WIF_PROVIDER_ID} for GitHub..."
  gcloud iam workload-identity-pools providers create-oidc "${WIF_PROVIDER_ID}" \
    --project="${PROJECT_ID}" \
    --location="global" \
    --workload-identity-pool="${WIF_POOL_ID}" \
    --display-name="GitHub" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor,attribute.ref=assertion.ref" \
    --attribute-condition="assertion.repository=='${GITHUB_REPO}'"
fi

# ---------------------------------------------------------------------------
# 6) Allow GitHub identities from this repo to impersonate the deploy SA
#    Why: without workloadIdentityUser, federation succeeds but cannot mint
#    credentials as the SA — CI would auth-fail on gcloud.
# ---------------------------------------------------------------------------
log "Binding roles/iam.workloadIdentityUser for repository ${GITHUB_REPO}..."
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="${MEMBER}" \
  --quiet >/dev/null

log "WIF configuration complete."
echo
echo "============================================================================"
echo " Set these GitHub repository VARIABLES (Settings → Secrets and variables"
echo " → Actions → Variables). Do NOT put a SA JSON key in secrets if using WIF."
echo "============================================================================"
echo
echo "  GCP_WORKLOAD_IDENTITY_PROVIDER = ${PROVIDER_RESOURCE}"
echo "  GCP_SERVICE_ACCOUNT            = ${SA_EMAIL}"
echo "  GCP_PROJECT_ID                 = ${PROJECT_ID}"
echo
echo "Also set deploy knobs as needed: GCP_REGION, GKE_CLUSTER, AR_REPOSITORY,"
echo "IMAGE_NAME, K8S_NAMESPACE, HELM_RELEASE, RUN_DEPLOY (true only when you"
echo "want auto-deploy on merge to master)."
echo
echo "How CI uses this:"
echo "  1. Job has permissions.id-token: write → GitHub mints an OIDC token."
echo "  2. google-github-actions/auth exchanges it via the provider above."
echo "  3. GCP issues short-lived creds as GCP_SERVICE_ACCOUNT."
echo "  4. Later steps (gcloud, docker push, helm) use those env credentials."
echo "  Setting the two vars is required; gcloud does not invent them automatically."
echo "============================================================================"
