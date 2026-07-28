# Deploying cache-custom to GKE

This document covers **Milestone 9**: container image, Helm chart, operator scripts, GitHub Actions deploy gates, and how to test.

Local development **without** containers remains:

```bash
go build -o go_cache ./cmd/cache-custom
./go_cache -addr :9001 -config config.json
```

---

## What gets deployed

| Piece | Path | Role |
|--------|------|------|
| Image | `Dockerfile` | Multi-stage Go build → distroless non-root runtime |
| Chart | `deploy/helm/cache-custom/` | Deployment or StatefulSet, Service, Secret config, PDB |
| Env template | `deploy/.env.example` | Non-secret knobs for scripts (copy to `deploy/.env`) |
| Scripts | `deploy/scripts/` | APIs → registry → cluster → credentials → push → helm → test |
| Actions | `.github/workflows/deploy.yml` | Deploy only on `master`, gated by `RUN_DEPLOY`; reuses `day2-deploy.sh` |

**Auth model:** Helm never logs into GCP by itself. You (or CI) authenticate to Google, load cluster credentials into kubeconfig, then `helm` talks to the Kubernetes API.

---

## Prerequisites (operator machine)

- `gcloud` CLI, logged in: `gcloud auth login` and `gcloud config set project PROJECT_ID`
- Sufficient IAM (e.g. ability to enable APIs, create GKE / Artifact Registry, get credentials)
- `docker`, `kubectl`, `helm` v3, `curl`
- A GCP project with billing enabled

---

## Configuration: `deploy/.env`

```bash
cp deploy/.env.example deploy/.env
# edit PROJECT_ID, REGION, CLUSTER_NAME, etc.
```

| Variable | Purpose |
|----------|---------|
| `PROJECT_ID` | GCP project |
| `REGION` | Region for Autopilot + Artifact Registry |
| `CLUSTER_NAME` | GKE cluster name |
| `CLUSTER_TYPE` | `autopilot` (default) or `standard` |
| `ZONE` | Used only for `standard` zonal clusters |
| `AR_REPOSITORY` | Artifact Registry repository id |
| `IMAGE_NAME` / `IMAGE_TAG` | Image name and tag |
| `NAMESPACE` / `HELM_RELEASE` | Kubernetes namespace and Helm release name |
| `PERSISTENCE_ENABLED` | `true` → chart uses StatefulSet + PVC |

**Do not** put service account JSON keys or tenant passwords in `.env`. Tenant passwords live in Helm `serverConfig` (Secret) or a sealed secret process you choose later.

Full image path:

```text
${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG}
```

---

## Scripts

All scripts source `deploy/scripts/common.sh` (loads `.env`, helpers).

### One-time (or rare) setup

Run these when bootstrapping a **new** GCP project / cluster / CI trust. Safe to re-run (idempotent where possible).

| Script | When | Purpose |
|--------|------|---------|
| `00-setup-wif-github.sh` | Once per project (for CI) | Workload Identity Federation: pool, GitHub OIDC provider, deploy SA, IAM. Prints GitHub vars to set. |
| `01-enable-apis.sh` | Once per project | Enable GKE / Artifact Registry / compute APIs |
| `02-create-artifact-registry.sh` | Once per project/region | Create Docker Artifact Registry repo |
| `03-create-gke-cluster.sh` | Once per environment | Create Autopilot or Standard cluster |
| `08-teardown.sh` | When stopping cost | Delete Helm release, namespace, GKE cluster, AR repo |

`00-setup-wif-github.sh` is **not** part of `from-scratch.sh` (local deploy does not need WIF). Run it before relying on GitHub Actions deploy.

```bash
# CI trust (once) — reuses SA github-deploy if it already exists
GITHUB_REPO=srivathsashreyas/cache-custom ./deploy/scripts/00-setup-wif-github.sh
# Then paste the printed GCP_WORKLOAD_IDENTITY_PROVIDER + GCP_SERVICE_ACCOUNT into GitHub Variables
```

### Every deploy (local day-2 / CI)

| Script | Purpose |
|--------|---------|
| `04-get-credentials.sh` | `get-credentials` → kubeconfig |
| `05-build-push-image.sh` | `docker build --platform=linux/amd64` + push to Artifact Registry |
| `06-helm-deploy.sh` | `helm upgrade --install` |
| `07-test-gke.sh` | Smoke: health + RESP |
| `day2-deploy.sh` | Runs 04→07 (what CI calls) |
| `from-scratch.sh` | Runs 01→07 (new cluster path; still no WIF) |

```bash
chmod +x deploy/scripts/*.sh

# New project / cluster (infra + app)
./deploy/scripts/from-scratch.sh

# Routine release (cluster already exists)
./deploy/scripts/day2-deploy.sh
```

### Existing cluster only

Skip one-time create scripts; ensure `.env` matches the existing cluster and registry:

```bash
./deploy/scripts/04-get-credentials.sh
./deploy/scripts/05-build-push-image.sh
./deploy/scripts/06-helm-deploy.sh
./deploy/scripts/07-test-gke.sh
```

### Persistence

1. Set `PERSISTENCE_ENABLED=true` in `.env` (or GitHub variable `PERSISTENCE_ENABLED=true` for CI).
2. `06-helm-deploy.sh` applies `deploy/helm/cache-custom/values-persistence.yaml`:
   - `persistence.enabled=true` → StatefulSet + PVC
   - server `Persistence.Mode=aof`, `Dir=/data`, `AOFFsync=always`
3. `07-test-gke.sh` writes a key, **deletes the cache pod**, waits for restart, confirms the key is still present (PVC restore smoke).

```bash
PERSISTENCE_ENABLED=true ./deploy/scripts/day2-deploy.sh
```

### Single-tenant dedicated pod

Use a values file with a single entry under `serverConfig.Tenants` and a dedicated Helm release/namespace per tenant.

---

## Helm chart notes

- **Deployment** when `persistence.enabled=false` (default).
- **StatefulSet + PVC** when `persistence.enabled=true`.
- Config (including passwords) is a **Secret** mounted at `/config/config.json`.
- Probes: HTTP `metrics` port `/healthz` (liveness) and `/readyz` (readiness).
- Service: ClusterIP ports `9001` (RESP) and `9090` (metrics).
- Pods use the namespace **default** ServiceAccount (no custom SA until RBAC is needed).

In-cluster RESP address:

```text
<release-fullname>.<namespace>.svc.cluster.local:9001
```

---

## GitHub Actions deploy

Workflow: [`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml)

There is **no** separate PR test workflow in this milestone; deploy is the controlled CI path.

### When it runs

| Trigger | Deploys? |
|---------|----------|
| Feature branch push / PR | **No** |
| Push / merge to `master` | Only if repository variable **`RUN_DEPLOY=true`** |
| `workflow_dispatch` (manual) | Yes (for controlled deploys when auto is off) |

If `RUN_DEPLOY` is unset or `false`, merges to `master` **do not** deploy.

### Repository variables / secrets to configure

**Variables** (Settings → Secrets and variables → Actions → Variables):

| Name | Example | Purpose |
|------|---------|---------|
| `RUN_DEPLOY` | `false` / `true` | Auto-deploy gate on `master` |
| `GCP_PROJECT_ID` | `my-project` | Project id |
| `GCP_REGION` | `us-central1` | Region |
| `GKE_CLUSTER` | `cache-custom` | Cluster name |
| `GKE_CLUSTER_TYPE` | `autopilot` or `standard` | Selects region vs zone credentials |
| `GKE_ZONE` | `us-central1-a` | Required if standard |
| `AR_REPOSITORY` | `cache-custom` | Artifact Registry repo id |
| `IMAGE_NAME` | `cache-custom` | Image name |
| `K8S_NAMESPACE` | `cache-custom` | Deploy namespace |
| `HELM_RELEASE` | `cache-custom` | Helm release name |
| `PERSISTENCE_ENABLED` | `false` | Passed through to scripts / Helm |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Full provider resource name (from setup script) | WIF OIDC provider |
| `GCP_SERVICE_ACCOUNT` | `github-deploy@PROJECT.iam.gserviceaccount.com` | SA CI impersonates |

**How to get WIF values (one-time):**

```bash
GITHUB_REPO=srivathsashreyas/cache-custom ./deploy/scripts/00-setup-wif-github.sh
```

The script reuses SA `github-deploy` if present, creates pool/provider/IAM, and prints:

```text
GCP_WORKLOAD_IDENTITY_PROVIDER = projects/NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider
GCP_SERVICE_ACCOUNT            = github-deploy@PROJECT_ID.iam.gserviceaccount.com
```

**How WIF works in CI (both vars required):**

1. Job has `permissions: id-token: write` → GitHub mints an OIDC token.  
2. `google-github-actions/auth` is given **provider + service account email**.  
3. Token is exchanged at Google; short-lived ADC env vars are exported.  
4. Later `gcloud` / docker / helm steps use those automatically.

Provider alone is **not** enough — set **both** `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_SERVICE_ACCOUNT`. gcloud does not invent them.

**Secrets:**

| Name | Purpose |
|------|---------|
| `GCP_SA_KEY` | Optional JSON key **only if** WIF vars are unset. Prefer WIF. |

### What the workflow does

1. Check out repo  
2. Authenticate to GCP (WIF if `GCP_WORKLOAD_IDENTITY_PROVIDER` is set, else `GCP_SA_KEY`)  
3. Write `deploy/.env` from repository variables (`IMAGE_TAG=github.sha`)  
4. Run **`./deploy/scripts/day2-deploy.sh`** (same as local day-2: credentials → build/push → helm → smoke test)

---


## How to test (summary)

Automated:

```bash
./deploy/scripts/07-test-gke.sh
```

Manual:

```bash
# Pods ready
kubectl -n cache-custom get pods

# Health
kubectl -n cache-custom port-forward svc/cache-custom 9090:9090
curl -s localhost:9090/healthz
curl -s localhost:9090/readyz

# RESP
kubectl -n cache-custom run redis-cli --rm -it --restart=Never \
  --image=redis:7-alpine -- redis-cli -h cache-custom PING
```

With default values tenants: `AUTH App1 secret1` then `SET` / `GET`.

---

## Suggested from-scratch order

1. Fill `deploy/.env`  
2. Install `gke-gcloud-auth-plugin` if kubectl reports it missing  
3. `./deploy/scripts/from-scratch.sh`  
4. Confirm `07-test-gke.sh` passes  
5. Leave `RUN_DEPLOY=false` until you trust CI auth  
6. Configure GitHub variables + WIF  
7. Set `RUN_DEPLOY=true` only when you want auto-deploy on `master`  
8. Or use **Actions → Deploy → Run workflow** for manual deploys  

### Tear down (stop cost)

```bash
CONFIRM_TEARDOWN=yes ./deploy/scripts/08-teardown.sh
```

Removes: Helm release + namespace, GKE cluster, Artifact Registry repo (images). Keeps the GCP project and enabled APIs.

### Where to view the setup in Google Cloud Console

Replace `PROJECT_ID` and `REGION` (defaults from `.env`: project + `us-central1`).

| What | Console path |
|------|----------------|
| **GKE cluster** | [Kubernetes Engine → Clusters](https://console.cloud.google.com/kubernetes/list/overview) |
| **Workloads (Deployment/pods)** | [Kubernetes Engine → Workloads](https://console.cloud.google.com/kubernetes/workload) — filter namespace `cache-custom` |
| **Services** | [Kubernetes Engine → Services & Ingress](https://console.cloud.google.com/kubernetes/discovery) |
| **Artifact Registry images** | [Artifact Registry](https://console.cloud.google.com/artifacts) → region → repo `cache-custom` |
| **Direct workload deep-link** (after cluster exists) | `https://console.cloud.google.com/kubernetes/workload_/gcloud/REGION/CLUSTER_NAME?project=PROJECT_ID` |

After a successful deploy you should see Deployment `cache-custom`, Service `cache-custom` (ports 9001/9090), and pods `Running` / Ready.  

---

## Cost / scale (short)

- Prefer **Autopilot** or a **small Standard** node for a single replica.  
- Keep `replicaCount: 1` when persistence is on.  
- Horizontal autoscaling is **not** configured by default (single-node durability / shared state). Scale vertically or run dedicated single-tenant releases if needed.
