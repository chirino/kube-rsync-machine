# Build static frontend assets on the native builder platform; they are reused
# unchanged by every target image architecture.
FROM --platform=$BUILDPLATFORM node:25-alpine AS frontend-builder

WORKDIR /workspace/frontend
RUN npm install -g pnpm@10.10.0
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm build

# Run the Go toolchain on the native builder platform and cross-compile for the
# target image architecture. This avoids slow QEMU-emulated Go builds.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-$(go env GOARCH)} go build -trimpath -ldflags="-s -w" -o /out/kube-rsync-machine ./cmd/kube-rsync-machine

FROM alpine:3.22

ENV KRM_FRONTEND_DIR=/usr/share/kube-rsync-machine/frontend

RUN apk add --no-cache ca-certificates rsync

COPY --from=builder /out/kube-rsync-machine /usr/local/bin/kube-rsync-machine
COPY --from=frontend-builder /workspace/frontend/dist/ /usr/share/kube-rsync-machine/frontend/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kube-rsync-machine"]
