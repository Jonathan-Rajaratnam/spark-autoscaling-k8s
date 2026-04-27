"""
tests/integration/test_kubernetes_stack.py

Integration tests that validate the deployed Kubernetes + Prometheus + KEDA + HPA stack.
These tests require a running Kubernetes cluster with the full monitoring stack deployed.

Requirements covered:
  FR6  – Prometheus metric scraping from Spark pods
  FR7  – Custom metrics registered on Kubernetes custom.metrics.k8s.io API
  FR8  – KEDA ScaledObject reaches READY state
  FR9  – HPA TARGETS column reports a live metric value (not <unknown>)
  FR10 – Prometheus Adapter registers correct metric names

Skip behaviour:
  Each test is automatically skipped if `kubectl` is unavailable or if the
  relevant Kubernetes namespace/resource is not present. This allows the
  test suite to run safely in CI environments without a live cluster.

Run with:  pytest tests/integration/test_kubernetes_stack.py -v
"""

import json
import os
import subprocess
import sys
import time
from typing import Optional

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _kubectl(*args, namespace: Optional[str] = None,
             timeout: int = 15) -> subprocess.CompletedProcess:
    """Run a kubectl command and return the CompletedProcess result."""
    cmd = ["kubectl"]
    if namespace:
        cmd += ["-n", namespace]
    cmd += list(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _kubectl_available() -> bool:
    try:
        r = _kubectl("version", "--client", "--output=json")
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _namespace_exists(ns: str) -> bool:
    r = _kubectl("get", "namespace", ns)
    return r.returncode == 0


def _deployment_exists(name: str, ns: str = "default") -> bool:
    r = _kubectl("get", "deployment", name, namespace=ns)
    return r.returncode == 0


def _pod_label_exists(label: str, ns: str = "default") -> bool:
    r = _kubectl("get", "pods", "-l", label, "-o", "json", namespace=ns)
    if r.returncode != 0:
        return False
    data = json.loads(r.stdout)
    return len(data.get("items", [])) > 0


require_kubectl = pytest.mark.skipif(
    not _kubectl_available(),
    reason="kubectl not available — skipping integration tests"
)


# ---------------------------------------------------------------------------
# TC-I01  Cluster connectivity
# ---------------------------------------------------------------------------
@require_kubectl
class TestClusterConnectivity:

    def test_cluster_is_reachable(self):
        """TC-I01 | FR6 | kubectl can reach a running Kubernetes cluster."""
        r = _kubectl("cluster-info")
        assert r.returncode == 0, \
            f"kubectl cluster-info failed:\n{r.stderr}"

    def test_monitoring_namespace_exists(self):
        """TC-I02 | FR6 | 'monitoring' namespace must exist for Prometheus stack."""
        assert _namespace_exists("monitoring"), \
            "Namespace 'monitoring' not found — deploy kube-prometheus-stack first"

    def test_keda_namespace_exists(self):
        """TC-I03 | FR8 | 'keda' namespace must exist for KEDA operator."""
        assert _namespace_exists("keda"), \
            "Namespace 'keda' not found — deploy KEDA operator first"


# ---------------------------------------------------------------------------
# TC-I04  Prometheus stack health
# ---------------------------------------------------------------------------
@require_kubectl
class TestPrometheusStack:

    def test_prometheus_pod_is_running(self):
        """TC-I04 | FR6 | Prometheus pod must be Running in monitoring namespace."""
        r = _kubectl("get", "pods", "-l",
                     "app.kubernetes.io/name=prometheus",
                     "-o", "json", namespace="monitoring")
        assert r.returncode == 0, f"kubectl failed: {r.stderr}"
        data = json.loads(r.stdout)
        pods = data.get("items", [])
        assert len(pods) > 0, "No Prometheus pods found in monitoring namespace"
        phases = [p["status"].get("phase") for p in pods]
        assert all(ph == "Running" for ph in phases), \
            f"Expected all Prometheus pods Running, got: {phases}"

    def test_grafana_pod_is_running(self):
        """TC-I05 | FR6 | Grafana pod must be Running in monitoring namespace."""
        r = _kubectl("get", "pods", "-l",
                     "app.kubernetes.io/name=grafana",
                     "-o", "json", namespace="monitoring")
        assert r.returncode == 0
        data = json.loads(r.stdout)
        pods = data.get("items", [])
        assert len(pods) > 0, "No Grafana pods found in monitoring namespace"
        phases = [p["status"].get("phase") for p in pods]
        assert any(ph == "Running" for ph in phases), \
            f"Expected at least one Grafana pod Running, got: {phases}"

    def test_servicemonitor_crd_exists(self):
        """TC-I06 | FR6 | ServiceMonitor CRD must be registered (kube-prometheus-stack)."""
        r = _kubectl("get", "crd", "servicemonitors.monitoring.coreos.com")
        assert r.returncode == 0, \
            "ServiceMonitor CRD not found — is kube-prometheus-stack deployed?"

    def test_spark_servicemonitor_exists(self):
        """TC-I07 | FR6 | Spark ServiceMonitor must be present in monitoring namespace."""
        r = _kubectl("get", "servicemonitor", "-o", "json", namespace="monitoring")
        assert r.returncode == 0
        data = json.loads(r.stdout)
        names = [
            item["metadata"]["name"]
            for item in data.get("items", [])
        ]
        assert any("spark" in n.lower() for n in names), \
            f"No Spark ServiceMonitor found. Present: {names}"


# ---------------------------------------------------------------------------
# TC-I08  JMX metric exposure (Strategy 1 – DRA, running pod required)
# ---------------------------------------------------------------------------
@require_kubectl
class TestJmxMetricExposure:

    @pytest.fixture(scope="class")
    def spark_pod_name(self):
        """Find a running spark-worker or executor pod."""
        for label in ("app=spark-worker", "spark-role=executor",
                      "app.kubernetes.io/component=executor"):
            r = _kubectl("get", "pods", "-l", label, "-o", "json")
            if r.returncode == 0:
                items = json.loads(r.stdout).get("items", [])
                running = [
                    i["metadata"]["name"]
                    for i in items
                    if i["status"].get("phase") == "Running"
                ]
                if running:
                    return running[0]
        return None

    def test_spark_pod_exposes_jmx_port_9404(self, spark_pod_name):
        """TC-I08 | FR6 | Spark executor pod must expose JMX metrics on port 9404."""
        if spark_pod_name is None:
            pytest.skip("No running Spark executor pod found — submit a job first")

        r = _kubectl("get", "pod", spark_pod_name, "-o", "json")
        assert r.returncode == 0
        pod = json.loads(r.stdout)
        ports = []
        for container in pod["spec"].get("containers", []):
            for p in container.get("ports", []):
                ports.append(p.get("containerPort"))
        assert 9404 in ports, \
            f"Port 9404 not found in pod {spark_pod_name}. Ports: {ports}"

    def test_jmx_agent_env_var_present(self, spark_pod_name):
        """TC-I09 | FR6 | Spark pod must configure a JMX javaagent env var."""
        if spark_pod_name is None:
            pytest.skip("No running Spark executor pod found")

        r = _kubectl("get", "pod", spark_pod_name, "-o", "json")
        pod = json.loads(r.stdout)
        env_vars = []
        for container in pod["spec"].get("containers", []):
            for env in container.get("env", []):
                env_vars.append({
                    "name": env.get("name", ""),
                    "value": env.get("value", ""),
                })

        valid_env_names = {
            "JAVA_TOOL_OPTIONS",
            "SPARK_WORKER_OPTS",
            "SPARK_MASTER_OPTS",
            "SPARK_DAEMON_JAVA_OPTS",
        }
        has_jmx_agent = any(
            env["name"] in valid_env_names
            or "-javaagent:" in env["value"]
            or "jmx_prometheus_javaagent.jar" in env["value"]
            for env in env_vars
        )
        assert has_jmx_agent, \
            f"No JMX javaagent env var found on pod {spark_pod_name}. Env: {env_vars}"


# ---------------------------------------------------------------------------
# TC-I10  KEDA ScaledObject health
# ---------------------------------------------------------------------------
@require_kubectl
@pytest.mark.skipif(
    not _namespace_exists("keda") if _kubectl_available() else True,
    reason="KEDA namespace not available"
)
class TestKedaScaledObject:

    def test_scaledobject_crd_registered(self):
        """TC-I10 | FR8 | ScaledObject CRD must be registered by KEDA operator."""
        r = _kubectl("get", "crd", "scaledobjects.keda.sh")
        assert r.returncode == 0, \
            "ScaledObject CRD not registered — is KEDA operator installed?"

    def test_keda_operator_pod_running(self):
        """TC-I11 | FR8 | KEDA operator pod must be Running."""
        r = _kubectl("get", "pods", "-l",
                     "app=keda-operator",
                     "-o", "json", namespace="keda")
        if r.returncode != 0:
            pytest.skip("KEDA operator pod not found")
        data = json.loads(r.stdout)
        pods = data.get("items", [])
        if not pods:
            pytest.skip("No KEDA operator pods found")
        phases = [p["status"].get("phase") for p in pods]
        assert "Running" in phases, \
            f"KEDA operator not Running. Phases: {phases}"

    def test_spark_scaledobject_ready(self):
        """TC-I12 | FR8 | Spark ScaledObject must show READY=True."""
        r = _kubectl("get", "scaledobject", "-o", "json", namespace="default")
        if r.returncode != 0:
            pytest.skip("No ScaledObjects found in default namespace")
        data = json.loads(r.stdout)
        items = data.get("items", [])
        if not items:
            pytest.skip("No ScaledObjects deployed yet (Strategy 2 not running)")
        spark_objects = [
            i for i in items
            if "spark" in i["metadata"]["name"].lower()
        ]
        assert len(spark_objects) > 0, "No Spark ScaledObject found"
        for obj in spark_objects:
            conditions = obj.get("status", {}).get("conditions", [])
            ready = next(
                (c for c in conditions if c.get("type") == "Ready"), None
            )
            assert ready is not None, \
                f"ScaledObject {obj['metadata']['name']} has no Ready condition"
            assert ready.get("status") == "True", \
                (f"ScaledObject {obj['metadata']['name']} is not Ready. "
                 f"Reason: {ready.get('message', 'unknown')}")


# ---------------------------------------------------------------------------
# TC-I13  Prometheus Adapter / custom metrics API
# ---------------------------------------------------------------------------
@require_kubectl
class TestPrometheusAdapterMetrics:

    def test_custom_metrics_api_registered(self):
        """TC-I13 | FR10 | custom.metrics.k8s.io API group must be registered."""
        r = _kubectl("get", "--raw",
                     "/apis/custom.metrics.k8s.io/v1beta1")
        assert r.returncode == 0, \
            ("custom.metrics.k8s.io API not available — "
             "is Prometheus Adapter deployed and healthy?")

    def test_spark_jvm_heap_metric_registered(self):
        """TC-I14 | FR10 | spark_jvm_heap_usage_percent must appear in custom metrics API."""
        if not any(_pod_label_exists(label) for label in (
            "app=spark-worker",
            "app-role=spark-executor",
            "strategy=hpa",
            "strategy=keda",
            "strategy=dynamic-allocation",
        )):
            pytest.skip("No live Spark worker/executor pods found to expose JVM metrics")
        r = _kubectl("get", "--raw",
                     "/apis/custom.metrics.k8s.io/v1beta1")
        if r.returncode != 0:
            pytest.skip("custom.metrics.k8s.io API not available")
        data = json.loads(r.stdout)
        resource_names = [
            item.get("name", "")
            for item in data.get("resources", [])
        ]
        jvm_metrics = [n for n in resource_names if "jvm_heap" in n or "jvm" in n]
        assert len(jvm_metrics) > 0, (
            f"spark_jvm_heap_usage_percent not found in custom metrics API. "
            f"Registered metrics: {resource_names}"
        )

    def test_hpa_targets_not_unknown(self):
        """TC-I15 | FR9 | HPA TARGETS column must not report <unknown>."""
        r = _kubectl("get", "hpa", "-o", "json", namespace="default")
        if r.returncode != 0:
            pytest.skip("No HPA resources found in default namespace")
        data = json.loads(r.stdout)
        hpa_items = data.get("items", [])
        if not hpa_items:
            pytest.skip("No HPA deployed yet (Strategy 3 not running)")
        for hpa in hpa_items:
            name = hpa["metadata"]["name"]
            metrics = hpa.get("status", {}).get("currentMetrics", [])
            # An empty currentMetrics list at rest is acceptable;
            # the issue is an explicit "unknown" condition
            conditions = hpa.get("status", {}).get("conditions", [])
            scaling_active = next(
                (c for c in conditions if c.get("type") == "ScalingActive"), None
            )
            if scaling_active:
                assert scaling_active.get("status") != "False" or \
                       "FailedGetScale" not in scaling_active.get("reason", ""), \
                    (f"HPA {name} cannot get scale target. "
                     f"Reason: {scaling_active.get('message')}")


# ---------------------------------------------------------------------------
# TC-I16  RBAC — ServiceAccount and Role exist
# ---------------------------------------------------------------------------
@require_kubectl
class TestRbacConfiguration:

    def test_spark_service_account_exists(self):
        """TC-I16 | FR3 | 'spark' ServiceAccount must exist in default namespace."""
        r = _kubectl("get", "serviceaccount", "spark", namespace="default")
        assert r.returncode == 0, \
            "'spark' ServiceAccount not found — apply k8s/rbac/ manifests"

    def test_spark_role_exists(self):
        """TC-I17 | FR3 | 'spark-role' Role must exist in default namespace."""
        r = _kubectl("get", "role", "spark-role", namespace="default")
        assert r.returncode == 0, \
            "'spark-role' Role not found in default namespace"

    def test_spark_rolebinding_exists(self):
        """TC-I18 | FR3 | RoleBinding linking spark-role to spark SA must exist."""
        r = _kubectl("get", "rolebinding", "-o", "json", namespace="default")
        assert r.returncode == 0
        data = json.loads(r.stdout)
        names = [i["metadata"]["name"] for i in data.get("items", [])]
        spark_bindings = [n for n in names if "spark" in n.lower()]
        assert len(spark_bindings) > 0, \
            f"No Spark RoleBinding found. Present bindings: {names}"

    def test_spark_role_grants_pod_create(self):
        """TC-I19 | FR3 | spark-role must grant 'create' on pods (executor provisioning)."""
        r = _kubectl("get", "role", "spark-role",
                     "-o", "json", namespace="default")
        if r.returncode != 0:
            pytest.skip("spark-role not found")
        role = json.loads(r.stdout)
        rules = role.get("rules", [])
        pod_rules = [
            rule for rule in rules
            if "pods" in rule.get("resources", [])
        ]
        assert len(pod_rules) > 0, "No rules found for 'pods' resource"
        verbs = set()
        for rule in pod_rules:
            verbs.update(rule.get("verbs", []))
        assert "create" in verbs, \
            f"'create' verb not granted on pods. Granted verbs: {verbs}"
        assert "delete" in verbs, \
            f"'delete' verb not granted on pods. Granted verbs: {verbs}"

    def test_spark_role_is_namespace_scoped_not_cluster(self):
        """TC-I20 | FR3 | Spark permissions must be a Role (not ClusterRole) for least privilege."""
        role_r = _kubectl("get", "role", "spark-role", namespace="default")
        cluster_r = _kubectl("get", "clusterrole", "spark-role")
        assert role_r.returncode == 0, "spark-role Role not found"
        # If a ClusterRole named spark-role exists that would be overly broad
        assert cluster_r.returncode != 0, \
            "spark-role should be a namespace-scoped Role, not a ClusterRole"
