# Spark on Kubernetes Commands Sheet

## RBAC Management

### Apply RBAC Configuration
```sh
# Apply specific RBAC files
kubectl apply -f k8s/rbac/spark-serviceaccount.yaml
kubectl apply -f k8s/rbac/spark-role.yaml
kubectl apply -f k8s/rbac/spark-rolebinding.yaml

# Or apply directory if supported by your setup (generic kubectl)
kubectl apply -f k8s/rbac/
```

### Delete RBAC Configuration
```sh
kubectl delete -f k8s/rbac/spark-rolebinding.yaml
kubectl delete -f k8s/rbac/spark-role.yaml
kubectl delete -f k8s/rbac/spark-serviceaccount.yaml
```

## Application Management

### Run Spark Application
```sh
kubectl apply -f k8s/spark-app.yaml
```

### Execution Modes
The application supports three analysis modes:
1. `top_games`: Analyzes the most popular games (default).
2. `sentiment`: Analyzes review sentiment.
3. `user_activity`: Analyzes user activity levels.

To change the mode, edit the `arguments` section in `k8s/spark-app.yaml`:
```yaml
spec:
  arguments:
    - "sentiment" # or top_games, user_activity
```

### Delete Spark Application
```sh
kubectl delete sparkapp spark-local-job
```

### View Application Status
```sh
# Watch for status changes
kubectl get sparkapp spark-local-job -w
```

## Debugging

### Check Pods
```sh
kubectl get pods
```

### View Driver Logs
```sh
# Find driver pod name (usually app-name + driver)
kubectl logs spark-local-job-driver

# Follow logs
kubectl logs -f spark-local-job-driver
```

### View Events
```sh
kubectl describe sparkapp spark-local-job
```
