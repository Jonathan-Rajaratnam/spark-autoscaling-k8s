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

## Local Setup

### 1. Install the Spark Operator

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --wait
```

The repo uses `SparkApplication` manifests with API version `sparkoperator.k8s.io/v1beta2`.

### 2. Build the Docker Image

The current strategy manifests use `jonathanr08/spark-app:1.7`.

```bash
docker build -t jonathanr08/spark-app:1.7 .
```

If you use Minikube, build inside Minikube's Docker daemon:

```bash
eval "$(minikube docker-env)"
docker build -t jonathanr08/spark-app:1.7 .
```

If you use Kind, load the image after building:

```bash
kind load docker-image jonathanr08/spark-app:1.7
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
  -f k8s/monitoring/values.yaml \
  --wait

kubectl apply -f k8s/monitoring/spark-servicemonitor.yaml
kubectl apply -f k8s/monitoring/spark-metrics-service.yaml
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
  --wait
```

### 6. Install Prometheus Adapter

```bash
helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace monitoring \
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

### 1. Build and Push the Image

The EKS manifests pull `jonathanr08/spark-app:1.7`, so the image must exist in the registry before jobs start.

```bash
docker build -t jonathanr08/spark-app:1.7 .
docker push jonathanr08/spark-app:1.7
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
