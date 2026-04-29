#!/usr/bin/env bash
#
# setup-aws.sh — Provision an EKS cluster with EFS storage, Helm dependencies,
# and dataset migration for the Spark autoscaling benchmark.
#
# Hardened version: quoted variables, proper waiters, idempotent operations,
# pinned Helm charts, error handling, and explicit region configuration.
#
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
CLUSTER_NAME="spark-benchmark-cluster"
AWS_REGION="us-east-1"
EFS_NAME="spark-benchmark-efs"
SG_NAME="efs-sg-spark"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helm chart versions — pin to prevent breaking upgrades
AWS_EFS_CSI_DRIVER_VERSION="4.0.1"
SPARK_OPERATOR_VERSION="2.5.0"
KUBE_PROMETHEUS_VERSION="83.7.0"
KEDA_VERSION="2.19.0"
PROMETHEUS_ADAPTER_VERSION="5.3.0"
METRICS_SERVER_VERSION="3.13.0"
METRICS_SERVER_IMAGE_TAG="v0.8.1"

export AWS_DEFAULT_REGION="${AWS_REGION}"

# ── Colour helpers ───────────────────────────────────────────────────────────
info()  { printf '\n\033[1;34m▸ %s\033[0m\n' "$*"; }
ok()    { printf '\033[1;32m  ✔ %s\033[0m\n' "$*"; }
warn()  { printf '\033[1;33m  ⚠ %s\033[0m\n' "$*"; }
fail()  { printf '\033[1;31m  ✖ %s\033[0m\n' "$*"; exit 1; }

# ── Cleanup trap ─────────────────────────────────────────────────────────────
cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        echo ""
        warn "Script failed at step: ${CURRENT_STEP:-unknown} (exit code ${exit_code})"
        warn "The environment may be partially provisioned. Review the step above and re-run."
    fi
}
trap cleanup EXIT
CURRENT_STEP="prerequisites"

# ── Prerequisites ────────────────────────────────────────────────────────────
echo "=================================================="
echo "    AWS EKS & EFS Automation Setup"
echo "=================================================="

for cmd in eksctl aws kubectl helm perl; do
    command -v "$cmd" &>/dev/null || fail "${cmd} is required but not found. Please install it."
done

# ── 1. EKS Cluster (idempotent) ─────────────────────────────────────────────
CURRENT_STEP="1-eks-cluster"
info "1. Creating EKS cluster (skips if already exists)..."

if aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" &>/dev/null; then
    ok "Cluster '${CLUSTER_NAME}' already exists — skipping creation."
else
    eksctl create cluster -f "${SCRIPT_DIR}/cluster.yaml"
    ok "Cluster created."
fi

# ── 2. VPC discovery ────────────────────────────────────────────────────────
CURRENT_STEP="2-vpc-discovery"
info "2. Discovering VPC info..."

VPC_ID=$(aws eks describe-cluster \
    --name "${CLUSTER_NAME}" \
    --query "cluster.resourcesVpcConfig.vpcId" \
    --output text)

if [[ -z "${VPC_ID}" || "${VPC_ID}" == "None" ]]; then
    fail "Could not determine VPC ID for cluster '${CLUSTER_NAME}'."
fi

CIDR=$(aws ec2 describe-vpcs \
    --vpc-ids "${VPC_ID}" \
    --query "Vpcs[0].CidrBlock" \
    --output text)

if [[ -z "${CIDR}" || "${CIDR}" == "None" ]]; then
    fail "Could not determine CIDR for VPC '${VPC_ID}'."
fi

ok "VPC: ${VPC_ID}  CIDR: ${CIDR}"

# ── 3. Security Group ───────────────────────────────────────────────────────
CURRENT_STEP="3-security-group"
info "3. Ensuring EFS security group exists..."

SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${VPC_ID}" "Name=group-name,Values=${SG_NAME}" \
    --query "SecurityGroups[0].GroupId" \
    --output text 2>/dev/null || echo "None")

if [[ "${SG_ID}" == "None" || -z "${SG_ID}" ]]; then
    SG_ID=$(aws ec2 create-security-group \
        --group-name "${SG_NAME}" \
        --description "Allow NFS traffic for EKS" \
        --vpc-id "${VPC_ID}" \
        --query "GroupId" \
        --output text)

    aws ec2 authorize-security-group-ingress \
        --group-id "${SG_ID}" \
        --protocol tcp \
        --port 2049 \
        --cidr "${CIDR}"

    ok "Created security group: ${SG_ID}"
else
    ok "Security group already exists: ${SG_ID}"
fi

# ── 4. EFS File System ──────────────────────────────────────────────────────
CURRENT_STEP="4-efs-filesystem"
info "4. Ensuring EFS file system exists..."

EFS_ID=$(aws efs describe-file-systems \
    --query "FileSystems[?Name=='${EFS_NAME}'].FileSystemId | [0]" \
    --output text 2>/dev/null || echo "None")

if [[ -n "${EFS_ID}" && "${EFS_ID}" != "None" ]]; then
    ok "EFS already exists: ${EFS_ID}"
else
    EFS_ID=$(aws efs create-file-system \
        --creation-token "${EFS_NAME}" \
        --performance-mode generalPurpose \
        --tags "Key=Name,Value=${EFS_NAME}" \
        --query "FileSystemId" \
        --output text)

    # Wait for EFS to become available (proper waiter, not sleep)
    echo "  Waiting for EFS to become available..."
    local_retries=0
    while true; do
        efs_state=$(aws efs describe-file-systems \
            --file-system-id "${EFS_ID}" \
            --query "FileSystems[0].LifeCycleState" \
            --output text)

        if [[ "${efs_state}" == "available" ]]; then
            break
        fi

        local_retries=$((local_retries + 1))
        if [[ ${local_retries} -ge 30 ]]; then
            fail "EFS ${EFS_ID} did not become available within 5 minutes."
        fi
        sleep 10
    done

    ok "Created EFS: ${EFS_ID}"
fi

# ── 5. Mount Targets ────────────────────────────────────────────────────────
CURRENT_STEP="5-mount-targets"
info "5. Creating mount targets (idempotent)..."

SUBNETS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${VPC_ID}" \
              "Name=tag:kubernetes.io/role/internal-elb,Values=1" \
    --query "Subnets[*].SubnetId" \
    --output text)

if [[ -z "${SUBNETS}" ]]; then
    warn "No subnets found with tag kubernetes.io/role/internal-elb=1."
    warn "Falling back to all private subnets in the VPC."
    SUBNETS=$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=${VPC_ID}" \
        --query "Subnets[*].SubnetId" \
        --output text)
fi

for SUBNET in ${SUBNETS}; do
    aws efs create-mount-target \
        --file-system-id "${EFS_ID}" \
        --subnet-id "${SUBNET}" \
        --security-groups "${SG_ID}" &>/dev/null \
        || ok "Mount target for ${SUBNET} already exists."
done

# Wait for mount targets to be available
echo "  Waiting for mount targets to become available..."
mt_retries=0
while true; do
    mt_states=$(aws efs describe-mount-targets \
        --file-system-id "${EFS_ID}" \
        --query "MountTargets[*].LifeCycleState" \
        --output text)

    all_available=true
    for state in ${mt_states}; do
        if [[ "${state}" != "available" ]]; then
            all_available=false
            break
        fi
    done

    if ${all_available}; then
        break
    fi

    mt_retries=$((mt_retries + 1))
    if [[ ${mt_retries} -ge 30 ]]; then
        fail "Mount targets did not become available within 5 minutes."
    fi
    sleep 10
done

ok "All mount targets available."

# ── 6. Kubernetes Storage ────────────────────────────────────────────────────
CURRENT_STEP="6-k8s-storage"
info "6. Applying EFS StorageClass and PVC..."

echo "Cleaning out broken dynamic PVC configs..."
kubectl delete storageclass efs-sc --ignore-not-found 2>/dev/null || true
kubectl delete pvc spark-data-pvc --ignore-not-found 2>/dev/null || true
kubectl delete persistentvolume efs-pv --ignore-not-found 2>/dev/null || true

perl -pe "s/FILE_SYSTEM_ID/${EFS_ID}/g" "${SCRIPT_DIR}/efs-pv.yaml" \
    | kubectl apply -f -
kubectl apply -f "${SCRIPT_DIR}/efs-pvc.yaml"

ok "StorageClass and PVC applied."

# ── 7. Helm Dependencies ────────────────────────────────────────────────────
CURRENT_STEP="7-helm-dependencies"
info "7. Installing Helm dependencies (pinned versions)..."

helm repo add aws-efs-csi-driver https://kubernetes-sigs.github.io/aws-efs-csi-driver/ 2>/dev/null || true
helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ 2>/dev/null || true
helm repo update

helm upgrade --install aws-efs-csi-driver aws-efs-csi-driver/aws-efs-csi-driver \
    --namespace kube-system \
    --version "${AWS_EFS_CSI_DRIVER_VERSION}" \
    --wait --timeout 5m

helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator --create-namespace \
    --version "${SPARK_OPERATOR_VERSION}" \
    --set webhook.enable=true \
    --wait --timeout 5m

helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
    --namespace monitoring --create-namespace \
    --version "${KUBE_PROMETHEUS_VERSION}" \
    -f "${SCRIPT_DIR}/../k8s/monitoring/values-cloud.yaml" \
    --wait --timeout 5m

if helm status metrics-server --namespace kube-system &>/dev/null; then
    helm upgrade metrics-server metrics-server/metrics-server \
        --namespace kube-system \
        --version "${METRICS_SERVER_VERSION}" \
        --set "image.tag=${METRICS_SERVER_IMAGE_TAG}" \
        --wait --timeout 5m
elif kubectl get deployment metrics-server --namespace kube-system &>/dev/null; then
    warn "Metrics Server already exists but is not managed by Helm — leaving current deployment in place."
else
    helm install metrics-server metrics-server/metrics-server \
        --namespace kube-system \
        --version "${METRICS_SERVER_VERSION}" \
        --set "image.tag=${METRICS_SERVER_IMAGE_TAG}" \
        --wait --timeout 5m
fi

helm upgrade --install keda kedacore/keda \
    --namespace keda --create-namespace \
    --version "${KEDA_VERSION}" \
    --wait --timeout 5m

helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
    --namespace monitoring \
    --version "${PROMETHEUS_ADAPTER_VERSION}" \
    -f "${SCRIPT_DIR}/../k8s/strategies/3-hpa/prometheus-adapter-values.yaml" \
    --wait --timeout 5m

ok "All Helm releases installed."

# ── 8. Wait for PVC bind ────────────────────────────────────────────────────
CURRENT_STEP="8-pvc-bind"
info "8. Waiting for PVC to bind..."

PVC_STATUS=$(kubectl get pvc spark-data-pvc -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
if [[ "${PVC_STATUS}" == "Bound" ]]; then
    ok "PVC already bound — skipping wait."
else
    if ! kubectl wait --for=condition=bound pvc/spark-data-pvc --timeout=120s; then
        fail "PVC spark-data-pvc did not bind within 120 seconds."
    fi
fi

ok "PVC bound."

# ── 9. Dataset migration ────────────────────────────────────────────────────
CURRENT_STEP="9-dataset-migration"
info "9. Migrating datasets to EFS..."

# Clean up any leftover upload pod
kubectl delete pod efs-data-upload --ignore-not-found --wait=true

cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: efs-data-upload
  namespace: default
spec:
  restartPolicy: Never
  containers:
  - name: uploader
    image: busybox:1.36
    command: ["sleep", "3600"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: spark-data-pvc
EOF

kubectl wait --for=condition=ready pod/efs-data-upload --timeout=120s

echo "  Creating directory structure..."
kubectl exec efs-data-upload -- mkdir -p /data/games /data/input /data/output

echo "  Copying game data..."
kubectl cp "${SCRIPT_DIR}/../data/games/" efs-data-upload:/data/games/

echo "  Copying land registry data (5.4 GB — this may take several minutes)..."
kubectl cp "${SCRIPT_DIR}/../data/input/land-data.csv" efs-data-upload:/data/input/land-data.csv

echo "  Verifying upload..."
kubectl exec efs-data-upload -- sh -c 'echo "=== /data contents ===" && du -sh /data/*'

echo "  Cleaning up upload pod..."
kubectl delete pod efs-data-upload --wait=true

ok "Datasets migrated to EFS."

# ── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo "=================================================="
echo "    AWS EKS Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Push your image:    docker push jonathan/spark-app:1.5"
echo "  3. Run benchmark:      ${SCRIPT_DIR}/../experiment/run_experiment.sh steam_heavy"
echo ""
