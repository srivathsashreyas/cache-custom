# Multi-stage build: compile a static-ish Go binary, then run as non-root in a minimal image.
# Purpose: produce a production container for GKE (M9) without a full Go toolchain at runtime.

# --- build stage -----------------------------------------------------------
FROM golang:1.22-bookworm AS build

WORKDIR /src

# Cache module downloads separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the server binary only (cmd/cache-custom).
# CGO_ENABLED=0 → static binary that runs on distroless/static base images.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/cache-custom ./cmd/cache-custom

# --- runtime stage ---------------------------------------------------------
# distroless/static: no shell, non-root by default (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Binary
COPY --from=build /out/cache-custom /cache-custom

# Default paths: config and optional persistence dir are mounted at runtime by Kubernetes.
# Ports: 9001 = RESP, 9090 = metrics/health (see Observability.MetricsAddr).
EXPOSE 9001 9090

# Distroless nonroot user (65532).
USER nonroot:nonroot

# Entrypoint is the server; flags come from the Helm chart (args) or docker run.
ENTRYPOINT ["/cache-custom"]
# Sensible defaults if no args are provided (override in K8s).
CMD ["-addr", ":9001", "-config", "/config/config.json"]
