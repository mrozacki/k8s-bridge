# Multi-stage build for the k8s-bridge controller.
#
# Build context is this directory (.); the chart's
# image.repository/tag values point at whatever this produces once published
# (image publishing itself is backlog A4 — this Dockerfile is not built or
# pushed by any automation yet).
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0: static binary, required for the distroless/static base below
# (it has no libc). GOFLAGS=-trimpath keeps build-machine paths out of the
# binary. No -ldflags version stamping yet — no release process exists
# (CNCF gap item 7, tags/releases).
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -o /out/k8s-bridge ./cmd/k8s-bridge

# distroless static + the :nonroot tag: no shell, no package manager, runs as a
# non-root UID by default — matches the Deployment's runAsNonRoot securityContext
# and minimizes the image's attack surface (no tools for an attacker to abuse
# post-compromise). "nonroot" is a TAG on the static-debian12 repository, not a
# repository of its own — the earlier gcr.io/distroless/static-nonroot reference
# did not exist and failed to resolve. Digest-pinning this base is tracked as a
# security follow-up (backlog SEC2, image digests).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/k8s-bridge /k8s-bridge

ENTRYPOINT ["/k8s-bridge"]
