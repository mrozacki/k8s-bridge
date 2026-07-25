# Multi-stage build for the k8s-bridge controller.
#
# Build context is this directory (.); the chart's
# image.repository/tag values point at whatever this produces once published
# (image publishing itself is backlog A4 — this Dockerfile is not built or
# pushed by any automation yet).
# L4 (base images pinned by digest, closes backlog SEC2): the tag stays for
# human readability, the @sha256 digest is what actually gets pulled. `golang:1.26`
# is a moving tag — it advances on every patch release — so without the digest
# two builds of the same commit are not the same build, and a compromised or
# simply changed upstream tag would flow straight into a release image with no
# diff anywhere in this repo. Dependabot understands this form and bumps tag
# and digest together.
FROM golang:1.26@sha256:3aff6657219a4d9c14e27fb1d8976c49c29fddb70ba835014f477e1c70636647 AS build
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
# did not exist and failed to resolve. Digest-pinning this base is DONE (L4 /
# backlog SEC2, image digests) — `nonroot` is itself a moving tag, so the digest
# below is what pins the actual bytes; the tag is kept only so a human reading
# this file can still see which variant we depend on.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35
COPY --from=build /out/k8s-bridge /k8s-bridge

ENTRYPOINT ["/k8s-bridge"]
