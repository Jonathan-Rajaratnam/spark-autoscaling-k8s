# Spark on Kubernetes Setup Guide

This guide covers the two supported environments in this repo:

- Local Kubernetes through Docker Desktop, OrbStack, Minikube, or Kind.
- AWS EKS through the automation in `aws/setup-aws.sh`.

The benchmark compares three Spark autoscaling strategies:

1. Dynamic Allocation with the Spark Operator.
2. KEDA scaling a Spark Standalone worker Deployment.
3. Kubernetes HPA scaling a Spark Standalone worker Deployment with Prometheus Adapter metrics.

## Prerequisites

Install these before starting:

- Docker
- Kubernetes with `kubectl` configured
- Helm 3
- Python 3
- `curl`

For local CPU and memory collection, install Metrics Server in your cluster. For EKS, the AWS setup script installs Metrics Server, Prometheus, KEDA, the Spark Operator, and the Prometheus Adapter.

For local tests, `run_tests.sh` expects Java 17 from Homebrew at `/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home` and a project virtual environment at `.venv/`.

Shared defaults live in `scripts/config.sh`, and optional local overrides can go in `.env` using `.env.example` as a starting point. Source the config before running the local Helm commands below:

```bash
source scripts/config.sh
```

## Data Layout

The manifests expect datasets under the repository `data/` directory:

```text
data/
├── games/
│   ├── recommendations.csv
│   ├── games.csv
│   └── users.csv
├── input/
│   └── land-data.csv
└── output/
```

Local manifests mount `/Users/jonathan/Docs/spark-app/data` into containers as `/data` using `hostPath`. EKS manifests mount the `spark-data-pvc` EFS volume as `/data`.

Check the local data layout before running jobs:

```bash
./scripts/check-data.sh

# Also require the large Land Registry file
./scripts/check-data.sh --require-property
```

## Local Setup

### 1. Install the Spark Operator

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --version "$SPARK_OPERATOR_VERSION" \
  --set webhook.enable=true \
  --wait
```

The repo uses `SparkApplication` manifests with API version `sparkoperator.k8s.io/v1beta2`.

### 2. Build the Docker Image

The current strategy manifests use the image configured as `SPARK_APP_IMAGE`.

```bash
docker build -t "$SPARK_APP_IMAGE" .
```

If you use Minikube, build inside Minikube's Docker daemon:

```bash
eval "$(minikube docker-env)"
docker build -t "$SPARK_APP_IMAGE" .
```

If you use Kind, load the image after building:

```bash
kind load docker-image "$SPARK_APP_IMAGE"
```

Docker Desktop and OrbStack normally make locally built images available to the local Kubernetes cluster.

### 3. Apply Spark RBAC

```bash
kubectl apply -f k8s/rbac/
```

This creates the `spark` service account used by the Spark driver.

### 4. Install Monitoring

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --version "$KUBE_PROMETHEUS_VERSION" \
  -f k8s/monitoring/values.yaml \
  --wait

kubectl apply -f k8s/monitoring/spark-servicemonitor.yaml
kubectl apply -f k8s/monitoring/spark-metrics-service.yaml
```

Install Metrics Server locally if your Kubernetes distribution does not already provide it:

```bash
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update

helm upgrade --install metrics-server metrics-server/metrics-server \
  --namespace kube-system \
  --version "$METRICS_SERVER_VERSION" \
  --set "image.tag=$METRICS_SERVER_IMAGE_TAG" \
  --wait
```

Grafana is configured by `k8s/monitoring/values.yaml`:

- URL through port-forward: `http://localhost:3000`
- URL through NodePort: `http://localhost:30000`
- User: `admin`
- Password: `admin`

Port-forward Grafana:

```bash
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring
```

Port-forward Prometheus:

```bash
kubectl port-forward svc/monitoring-kube-prometheus-prometheus 9091:9090 -n monitoring
```

### 5. Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

helm upgrade --install keda kedacore/keda \
  --namespace keda \
  --create-namespace \
  --version "$KEDA_VERSION" \
  --wait
```

### 6. Install Prometheus Adapter

```bash
helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring \
  --version "$PROMETHEUS_ADAPTER_VERSION" \
  -f k8s/strategies/3-hpa/prometheus-adapter-values.yaml \
  --wait
```

### 7. Run a Strategy Manually

Dynamic Allocation:

```bash
kubectl apply -f k8s/strategies/1-dynamic-allocation/spark-app.yaml
kubectl get sparkapplication spark-app-dynamic -w
```

KEDA:

```bash
kubectl apply -f k8s/strategies/2-keda/spark-master.yaml
kubectl apply -f k8s/strategies/2-keda/spark-worker-deployment.yaml
kubectl apply -f k8s/strategies/2-keda/keda-scaledobject.yaml
kubectl apply -f k8s/strategies/2-keda/spark-job.yaml
```

HPA:

```bash
kubectl apply -f k8s/strategies/3-hpa/spark-master.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-worker-deployment.yaml
kubectl apply -f k8s/strategies/3-hpa/hpa.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-job.yaml
```

### 8. Run the Full Local Benchmark

```bash
./experiment/run_experiment.sh
```

The default mode is `steam_heavy` and the default environment is `local`.

You can pass mode and environment in either order:

```bash
./experiment/run_experiment.sh property_prices
./experiment/run_experiment.sh local property_prices
./experiment/run_experiment.sh property_prices local
```

Results are written to `experiment/results/<timestamp>-<mode>.csv` and `experiment/results/<timestamp>-<mode>-summary.txt`.

## AWS EKS Setup

The AWS path is automated by `aws/setup-aws.sh`.

### AWS Prerequisites

Install and configure:

- AWS CLI with credentials for `us-east-1`
- `eksctl`
- `kubectl`
- Helm
- Docker
- `perl`

You can override setup values by exporting variables or creating a local `.env` file. The AWS script reads `.env` first and then applies defaults from `scripts/config.sh`.

### 1. Build and Push the Image

The EKS manifests pull `SPARK_APP_IMAGE`, so the image must exist in the registry before jobs start.

```bash
source scripts/config.sh
docker build -t "$SPARK_APP_IMAGE" .
docker push "$SPARK_APP_IMAGE"
```

### 2. Provision EKS, EFS, Monitoring, and Autoscalers

```bash
./aws/setup-aws.sh
```

The script provisions:

- EKS cluster `spark-benchmark-cluster` in `us-east-1`.
- EFS-backed `spark-data-pvc`.
- Spark Operator chart version `2.5.0`.
- kube-prometheus-stack chart version `83.7.0`.
- KEDA chart version `2.19.0`.
- Prometheus Adapter chart version `5.3.0`.
- Metrics Server chart version `3.13.0` with image tag `v0.8.1`.
- Dataset upload into EFS at `/data`.

The cluster currently stays on Kubernetes `1.33` by design. The setup script warns that this version is expected to enter extended support in July 2026, but it does not change the cluster version.

Dataset upload behavior:

```bash
# Provision infrastructure without copying datasets
SKIP_DATA_UPLOAD=1 ./aws/setup-aws.sh

# Require the large Land Registry file for property_prices mode
REQUIRE_PROPERTY_DATA=1 ./aws/setup-aws.sh
```

By default, the script requires the three Steam CSVs in `data/games/`, warns if `data/input/land-data.csv` is missing, and still finishes so Steam modes can run.

### 3. Run the EKS Benchmark

```bash
./experiment/run_experiment.sh eks
./experiment/run_experiment.sh eks steam_heavy
./experiment/run_experiment.sh property_prices eks
```

The script automatically switches to the EKS-specific manifests:

- `k8s/strategies/1-dynamic-allocation/spark-app-eks.yaml`
- `k8s/strategies/2-keda/spark-worker-deployment-eks.yaml`
- `k8s/strategies/2-keda/spark-job-eks.yaml`
- `k8s/strategies/3-hpa/spark-worker-deployment-eks.yaml`
- `k8s/strategies/3-hpa/spark-job-eks.yaml`
- `k8s/strategies/3-hpa/hpa-eks.yaml`

### 4. Tear Down AWS Resources

This deletes the EKS cluster, EFS resources, and uploaded data:

```bash
./aws/teardown-aws.sh
```

## Analysis Modes

Valid modes are:

- `top_games`
- `sentiment`
- `user_activity`
- `steam_heavy`
- `property_prices`

`steam_heavy` is the default benchmark mode. `property_prices` is the heaviest mode and expects `data/input/land-data.csv`.

## Local Test Commands

Create the virtual environment expected by `run_tests.sh`:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run the automated tests:

```bash
.venv/bin/pytest tests/ -v
```

Or use the project test helper:

```bash
./run_tests.sh
```
