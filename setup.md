# Local Deployment Setup Guide

This guide explains how to set up the Spark application locally using Kubernetes, Helm, and Docker.

## Prerequisites

Ensure you have the following installed:
*   **Docker Desktop** (or Orbstack/Minikube) - with Kubernetes enabled.
*   **Helm** - Package manager for Kubernetes.
*   **Kubectl** - Command line tool for Kubernetes.

## 1. Setup Spark Operator (Helm)

The Spark Operator manages the lifecycle of Spark applications on Kubernetes.

```bash
# Add the Spark Operator Helm repo
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

# Update the repo
helm repo update

# Install the operator (creates a 'spark-operator' namespace)
helm install my-release spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set webhook.enable=true
```

## 2. Build Docker Image

Build the application image locally so it is available to your Kubernetes cluster.

```bash
# Build with version 1.3 to match our k8s config
docker build -t jonathan/spark-app:1.3 .
```

> **Note:** If you are using **Minikube**, you must point your shell to Minikube's docker-daemon before building:
> `eval $(minikube docker-env)`
>
> If you are using **Kind**, you must load the image:
> `kind load docker-image jonathan/spark-app:1.3`
>
> If you are using **Orbstack** or **Docker Desktop**, the standard build command works automatically.

## 3. Configure RBAC (Permissions)

Create the necessary Service Account and Roles for the Spark application.

```bash
# Create Service Account, Role, and RoleBinding
kubectl apply -f k8s/rbac/spark-serviceaccount.yaml
kubectl apply -f k8s/rbac/spark-role.yaml
kubectl apply -f k8s/rbac/spark-rolebinding.yaml
```

## 4. Run the Application

Deploy the `SparkApplication` custom resource.

```bash
# Apply the configuration
kubectl apply -f k8s/spark-app.yaml

# Check the status of the application
kubectl get sparkapp spark-app -w

#Check running pods
kubectl get pods -w

# Check the driver pod logs
kubectl logs -f spark-app-driver
```

## 5. Monitoring (Prometheus & Grafana)

We use the **kube-prometheus-stack** to install Prometheus and Grafana.

```bash
# Add the Prometheus Community Helm repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install the stack (creates a 'monitoring' namespace)
helm install monitoring prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    --create-namespace \
    --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
    --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false
```

### Enable Application Monitoring
Once the stack is installed, apply the ServiceMonitor to tell Prometheus to scrape our Spark application.

```bash
# Apply Service Monitor
kubectl apply -f k8s/monitoring/spark-servicemonitor.yaml
```

### Access Grafana
To access the Grafana dashboard:

```bash
# Port forward Grafana to localhost:3000
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring
```
*   **URL**: `http://localhost:3000`
*   **User**: `admin`
*   **Password**: `prom-operator` (default)

