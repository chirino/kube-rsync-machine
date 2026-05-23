#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
	cat <<'EOF'
Run the kube-rsync-machine kind integration suite.

Runtime requirements:
  - go
  - docker or another Docker-compatible builder used by `docker build`
  - kind
  - kubectl with Kustomize support (`kubectl apply -k`)

Common knobs:
  IMG=kube-rsync-machine:kind              Image tag to build, load, deploy, and pass to generated Jobs.
  KIND_CLUSTER=kube-rsync-machine          kind cluster name.
  KIND_USE_EXISTING=1                     Require and use an existing cluster instead of creating one.
  PRESERVE_CLUSTER=1                      Keep a cluster created by this script after tests finish.
  SKIP_BUILD=1                            Skip docker build.
  SKIP_LOAD=1                             Skip kind image load.
  SKIP_DEPLOY=1                           Skip config/default deploy and manager rollout wait.
  KRM_INTEGRATION_PRESERVE=1              Keep test namespaces and resources.
  KRM_INTEGRATION_SCENARIOS=backup,restore
                                           Comma-separated scenario names or regular expressions.
                                           Empty runs the smoke set only. Deep scenarios are
                                           selected on demand: restart, finalizer-retry,
                                           restore-points, watch-reconcile, source-failure,
                                           out-of-space, manager-namespace, csi.
  TEST_RUN=TestKindBackup                 Go test -run selector.
  TEST_TIMEOUT=20m                        Go test timeout.

Examples:
  make test-integration
  make test-integration-deep
  make test-integration PRESERVE_CLUSTER=1 KRM_INTEGRATION_PRESERVE=1 TEST_RUN=TestKindBackup
  make test-integration KRM_INTEGRATION_SCENARIOS=restart,watch-reconcile
  KIND_USE_EXISTING=1 SKIP_BUILD=1 make test-integration IMG=kube-rsync-machine:debug
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

require_tool() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "missing required tool: $1" >&2
		exit 2
	fi
}

IMG="${IMG:-kube-rsync-machine:kind}"
KIND_CLUSTER="${KIND_CLUSTER:-kube-rsync-machine}"
PRESERVE_CLUSTER="${PRESERVE_CLUSTER:-0}"
KIND_USE_EXISTING="${KIND_USE_EXISTING:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_LOAD="${SKIP_LOAD:-0}"
SKIP_DEPLOY="${SKIP_DEPLOY:-0}"
TEST_RUN="${TEST_RUN:-TestKind}"
TEST_TIMEOUT="${TEST_TIMEOUT:-20m}"
KRM_INTEGRATION_NAMESPACE="${KRM_INTEGRATION_NAMESPACE:-krm-it}"
KRM_INTEGRATION_SCENARIOS="${KRM_INTEGRATION_SCENARIOS:-}"
KRM_INTEGRATION_PRESERVE="${KRM_INTEGRATION_PRESERVE:-0}"
KRM_INTEGRATION_CONTEXT="kind-$KIND_CLUSTER"

require_tool go
require_tool kind
require_tool kubectl
if [[ "$SKIP_BUILD" != "1" ]]; then
	require_tool docker
fi

cluster_created=0
tmp_kubeconfig=""
cleanup() {
	local status=$?
	if [[ "$cluster_created" == "1" && "$PRESERVE_CLUSTER" != "1" ]]; then
		kind delete cluster --name "$KIND_CLUSTER" >/dev/null
	fi
	if [[ -n "$tmp_kubeconfig" ]]; then
		rm -f "$tmp_kubeconfig"
	fi
	exit "$status"
}
trap cleanup EXIT

if kind get clusters | grep -qx "$KIND_CLUSTER"; then
	echo "Using existing kind cluster: $KIND_CLUSTER"
else
	if [[ "$KIND_USE_EXISTING" == "1" ]]; then
		echo "kind cluster $KIND_CLUSTER does not exist and KIND_USE_EXISTING=1" >&2
		exit 1
	fi
	echo "Creating kind cluster: $KIND_CLUSTER"
	kind create cluster --name "$KIND_CLUSTER"
	cluster_created=1
fi

tmp_kubeconfig="$(mktemp)"
kind get kubeconfig --name "$KIND_CLUSTER" >"$tmp_kubeconfig"
export KUBECONFIG="$tmp_kubeconfig"
export KRM_INTEGRATION_CONTEXT

if [[ "$SKIP_BUILD" != "1" ]]; then
	echo "Building image: $IMG"
	docker build -f "$ROOT/hack/Dockerfile.integration" -t "$IMG" "$ROOT"
fi

if [[ "$SKIP_LOAD" != "1" ]]; then
	echo "Loading image into kind: $IMG"
	kind load docker-image --name "$KIND_CLUSTER" "$IMG"
fi

if [[ "$SKIP_DEPLOY" != "1" ]]; then
	echo "Deploying config/default with image: $IMG"
	kubectl --context "$KRM_INTEGRATION_CONTEXT" apply -k "$ROOT/config/default"
	kubectl --context "$KRM_INTEGRATION_CONTEXT" -n kube-rsync-machine-operator patch deployment kube-rsync-machine-controller-manager --type=strategic -p "$(cat <<EOF
{"spec":{"template":{"spec":{"containers":[{"name":"manager","image":"$IMG","args":["manager","--leader-elect","--metrics-bind-address=:8080","--health-probe-bind-address=:8081","--image=$IMG"]}]}}}}
EOF
)"
	kubectl --context "$KRM_INTEGRATION_CONTEXT" -n kube-rsync-machine-operator rollout status deployment/kube-rsync-machine-controller-manager --timeout=180s
	kubectl --context "$KRM_INTEGRATION_CONTEXT" wait --for=condition=Established crd/backupjobs.krm.chirino.github.io --timeout=60s
fi

echo "Running integration tests: go test ./internal/integration -run $TEST_RUN"
KRM_INTEGRATION=1 \
KRM_INTEGRATION_NAMESPACE="$KRM_INTEGRATION_NAMESPACE" \
KRM_INTEGRATION_SCENARIOS="$KRM_INTEGRATION_SCENARIOS" \
KRM_INTEGRATION_PRESERVE="$KRM_INTEGRATION_PRESERVE" \
KRM_IMAGE="$IMG" \
KIND_CLUSTER="$KIND_CLUSTER" \
go test ./internal/integration -run "$TEST_RUN" -count=1 -timeout "$TEST_TIMEOUT" -v
