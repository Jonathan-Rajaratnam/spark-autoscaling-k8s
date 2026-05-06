# Spark on Kubernetes Commands Sheet

Quick reference for the current manifests and scripts in this repo.

## Modes and Environments

Valid analysis modes:

- `top_games`
- `sentiment`
- `user_activity`
- `steam_heavy`
- `property_prices`

Benchmark environments:

- `local` - default, uses local `hostPath` data mounts.
- `eks` - uses EKS/EFS manifests ending in `-eks.yaml`.

The experiment script accepts mode and environment in either order:

```bash
./experiment/run_experiment.sh
./experiment/run_experiment.sh steam_heavy
./experiment/run_experiment.sh property_prices
./experiment/run_experiment.sh local property_prices
./experiment/run_experiment.sh eks steam_heavy
./experiment/run_experiment.sh property_prices eks
```

Results are saved to:

```text
experiment/results/<timestamp>-<mode>.csv
experiment/results/<timestamp>-<mode>-summary.txt
```

## Build and Test

```bash
docker build -t jonathanr08/spark-app:1.7 .
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pytest tests/ -v
./run_tests.sh
```

`./run_tests.sh` also expects Java 17 at `/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home`.

For EKS, push the image before running jobs:

```bash
docker push jonathanr08/spark-app:1.7
```

## RBAC

```bash
kubectl apply -f k8s/rbac/

kubectl delete -f k8s/rbac/spark-rolebinding.yaml --ignore-not-found
kubectl delete -f k8s/rbac/spark-role.yaml --ignore-not-found
kubectl delete -f k8s/rbac/spark-serviceaccount.yaml --ignore-not-found
```

## Strategy 1: Dynamic Allocation

Local:

```bash
kubectl apply -f k8s/strategies/1-dynamic-allocation/spark-app.yaml
kubectl get sparkapplication spark-app-dynamic -w
kubectl logs -f spark-app-dynamic-driver
kubectl delete -f k8s/strategies/1-dynamic-allocation/spark-app.yaml --ignore-not-found
```

EKS:

```bash
kubectl apply -f k8s/strategies/1-dynamic-allocation/spark-app-eks.yaml
kubectl get sparkapplication spark-app-dynamic -w
kubectl logs -f spark-app-dynamic-driver
kubectl delete -f k8s/strategies/1-dynamic-allocation/spark-app-eks.yaml --ignore-not-found
```

Watch executor pods:

```bash
kubectl get pods -l app-role=spark-executor,strategy=dynamic-allocation -w
```

## Strategy 2: KEDA

Local:

```bash
kubectl apply -f k8s/strategies/2-keda/spark-master.yaml
kubectl apply -f k8s/strategies/2-keda/spark-worker-deployment.yaml
kubectl apply -f k8s/strategies/2-keda/keda-scaledobject.yaml
kubectl apply -f k8s/strategies/2-keda/spark-job.yaml
```

EKS:

```bash
kubectl apply -f k8s/strategies/2-keda/spark-master.yaml
kubectl apply -f k8s/strategies/2-keda/spark-worker-deployment-eks.yaml
kubectl apply -f k8s/strategies/2-keda/keda-scaledobject.yaml
kubectl apply -f k8s/strategies/2-keda/spark-job-eks.yaml
```

Monitor:

```bash
kubectl rollout status deployment/spark-master-keda --timeout=120s
kubectl rollout status deployment/spark-worker-keda --timeout=120s
kubectl get pods -l app=spark-worker,strategy=keda -w
kubectl get scaledobject spark-worker-scaledobject
kubectl describe scaledobject spark-worker-scaledobject
kubectl logs -f job/spark-submit-keda
```

Teardown:

```bash
kubectl delete job/spark-submit-keda --ignore-not-found
kubectl delete -f k8s/strategies/2-keda/keda-scaledobject.yaml --ignore-not-found
kubectl delete -f k8s/strategies/2-keda/spark-worker-deployment.yaml --ignore-not-found
kubectl delete -f k8s/strategies/2-keda/spark-worker-deployment-eks.yaml --ignore-not-found
kubectl delete -f k8s/strategies/2-keda/spark-master.yaml --ignore-not-found
```

## Strategy 3: HPA

Local:

```bash
kubectl apply -f k8s/strategies/3-hpa/spark-master.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-worker-deployment.yaml
kubectl apply -f k8s/strategies/3-hpa/hpa.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-job.yaml
```

EKS:

```bash
kubectl apply -f k8s/strategies/3-hpa/spark-master.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-worker-deployment-eks.yaml
kubectl apply -f k8s/strategies/3-hpa/hpa-eks.yaml
kubectl apply -f k8s/strategies/3-hpa/spark-job-eks.yaml
```

Monitor:

```bash
kubectl rollout status deployment/spark-master-hpa --timeout=120s
kubectl rollout status deployment/spark-worker-hpa --timeout=120s
kubectl get pods -l app=spark-worker,strategy=hpa -w
kubectl get hpa spark-worker-hpa
kubectl describe hpa spark-worker-hpa
kubectl logs -f job/spark-submit-hpa
```

Teardown:

```bash
kubectl delete job/spark-submit-hpa --ignore-not-found
kubectl delete -f k8s/strategies/3-hpa/hpa.yaml --ignore-not-found
kubectl delete -f k8s/strategies/3-hpa/hpa-eks.yaml --ignore-not-found
kubectl delete -f k8s/strategies/3-hpa/spark-worker-deployment.yaml --ignore-not-found
kubectl delete -f k8s/strategies/3-hpa/spark-worker-deployment-eks.yaml --ignore-not-found
kubectl delete -f k8s/strategies/3-hpa/spark-master.yaml --ignore-not-found
```

## Monitoring

Prometheus:

```bash
kubectl port-forward svc/monitoring-kube-prometheus-prometheus 9091:9090 -n monitoring
open http://localhost:9091
```

Grafana:

```bash
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring
open http://localhost:3000
```

Local Grafana credentials from `k8s/monitoring/values.yaml`:

```text
User: admin
Password: admin
```

EKS Grafana credentials from `k8s/monitoring/values-cloud.yaml`:

```text
User: jonathan
Password: spark-test-fyp
```

JMX metrics from a pod:

```bash
kubectl port-forward pod/<spark-pod-name> 9404:9404
curl localhost:9404/metrics
curl localhost:9404/metrics | grep jvm_memory_bytes_used
```

## Metrics and Scaling Checks

```bash
kubectl top nodes
kubectl top pods
kubectl get events --sort-by='.lastTimestamp'
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | python3 -m json.tool
```

KEDA:

```bash
kubectl get scaledobject
kubectl describe scaledobject spark-worker-scaledobject
kubectl get hpa
```

HPA:

```bash
kubectl get hpa spark-worker-hpa
kubectl describe hpa spark-worker-hpa
```

## Logs and Debugging

```bash
kubectl get pods
kubectl describe pod <pod-name>
kubectl logs <pod-name>
kubectl logs -f job/spark-submit-keda
kubectl logs -f job/spark-submit-hpa
kubectl logs -f spark-app-dynamic-driver
```

Check mounted data:

```bash
kubectl exec <pod-name> -- ls -lah /data
kubectl exec <pod-name> -- find /data -maxdepth 2 -type f | head
```

## Cost Model

```bash
python3 experiment/cost_model.py experiment/results/<timestamp>-<mode>.csv
```

## AWS EKS Helpers

Provision EKS/EFS and install dependencies:

```bash
./aws/setup-aws.sh
```

Run an EKS benchmark:

```bash
./experiment/run_experiment.sh eks steam_heavy
```

Tear down AWS resources:

```bash
./aws/teardown-aws.sh
```
