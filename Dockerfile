FROM node:25-alpine AS frontend-builder

WORKDIR /workspace/frontend
RUN npm install -g pnpm@10.10.0
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm build

FROM golang:1.26-alpine AS builder

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kube-rsync-machine ./cmd/kube-rsync-machine

FROM alpine:3.22

ENV KRM_FRONTEND_DIR=/usr/share/kube-rsync-machine/frontend

RUN apk add --no-cache ca-certificates rsync

COPY --from=builder /out/kube-rsync-machine /usr/local/bin/kube-rsync-machine
COPY --from=frontend-builder /workspace/frontend/dist/ /usr/share/kube-rsync-machine/frontend/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/kube-rsync-machine"]
