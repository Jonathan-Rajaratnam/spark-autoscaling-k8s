#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${SCRIPT_DIR}/efs-fix-perms-pod.yaml"
POD_NAME="efs-fix-perms"
NAMESPACE="default"

usage() {
  cat <<'EOF'
Usage:
  ./aws/efs-helper-pod.sh up       Create the EFS helper pod and wait until ready
  ./aws/efs-helper-pod.sh fix      Create output dirs and make /data writable
  ./aws/efs-helper-pod.sh shell    Open a shell in the helper pod
  ./aws/efs-helper-pod.sh status   Show helper pod status
  ./aws/efs-helper-pod.sh down     Delete the helper pod

Common permission fix:
  ./aws/efs-helper-pod.sh fix
EOF
}

case "${1:-}" in
  up)
    kubectl apply -f "${MANIFEST}"
    kubectl wait --for=condition=ready "pod/${POD_NAME}" \
      -n "${NAMESPACE}" --timeout=120s
    kubectl get "pod/${POD_NAME}" -n "${NAMESPACE}"
    ;;
  fix)
    kubectl exec "pod/${POD_NAME}" -n "${NAMESPACE}" -- sh -c \
      'mkdir -p /data/games /data/input /data/output && chmod -R 777 /data'
    ;;
  shell)
    kubectl exec -it "pod/${POD_NAME}" -n "${NAMESPACE}" -- sh
    ;;
  status)
    kubectl get "pod/${POD_NAME}" -n "${NAMESPACE}"
    ;;
  down)
    kubectl delete -f "${MANIFEST}" --ignore-not-found --wait=true
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    echo "Unknown command: $1" >&2
    usage >&2
    exit 1
    ;;
esac
