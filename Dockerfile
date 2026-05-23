FROM node:25-bookworm-slim AS frontend-builder

WORKDIR /workspace/frontend
RUN npm install -g pnpm@10.10.0
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm build

FROM golang:1.26-bookworm AS builder

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/kube-rsync-machine ./cmd/kube-rsync-machine

FROM debian:bookworm-slim

ENV KRM_FRONTEND_DIR=/usr/share/kube-rsync-machine/frontend

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates rsync \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/kube-rsync-machine /usr/local/bin/kube-rsync-machine
COPY --from=frontend-builder /workspace/frontend/dist/ /usr/share/kube-rsync-machine/frontend/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kube-rsync-machine"]
