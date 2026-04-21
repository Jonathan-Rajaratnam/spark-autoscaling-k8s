# Comparative Analysis on AutoScaling Strategies for Spark on Kubernetes 

**Interim Progression Demonstration (IPD)**

**Student Name:** Jonathan Rajaratnam  <br />
**Student ID:** w1953878  <br />
**Module:** 6COSC023W - Computer Science Final Project <br />
**Dataset:** [Game Recommendations on Steam (Kaggle)](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam?select=recommendations.csv)

This project benchmarks three distinct autoscaling strategies for Apache Spark on Kubernetes. It runs heavy data-processing workloads against each strategy — Dynamic Allocation, KEDA, and HPA — and collects executor counts, CPU, and memory metrics to compare their scaling behaviour, cost-efficiency, and performance.

---

## 📑 Table of Contents

- [Key Features](#key-features)
- [Autoscaling Strategies](#autoscaling-strategies)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Analysis Modes](#analysis-modes)
- [Installation & Setup](#installation--setup)
- [Usage Guide](#usage-guide)
- [License](#license)

---

## Key Features

*   **Cloud-Native Spark:** Deployed using the `SparkOperator` on Kubernetes, removing the need for legacy resource managers like YARN.
*   **3 Autoscaling Strategies:** Benchmarks Dynamic Allocation, KEDA, and HPA side-by-side.
*   **Infrastructure as Code (IaC):** Entire application lifecycle managed through declarative Kubernetes YAML manifests.
*   **Multi-Mode Analysis:** Single codebase supporting multiple analysis workloads via command-line arguments.
*   **Observability:** Integrated with Prometheus and Grafana for monitoring cluster metrics (JMX, cAdvisor).
*   **Automated Benchmarking:** End-to-end experiment script that runs all strategies and collects metrics into CSV for analysis.

## Autoscaling Strategies

| # | Strategy | Mechanism | Scaler |
|---|---|---|---|
| 1 | **Dynamic Allocation** | Spark Operator CRD — Spark manages its own executors natively | Spark's internal scheduler |
| 2 | **KEDA** | Spark Standalone cluster — KEDA scales worker Deployment based on Prometheus metrics (JVM heap, CPU) | KEDA ScaledObject |
| 3 | **HPA** | Spark Standalone cluster — Kubernetes HPA scales worker Deployment via Prometheus Adapter custom metrics | HorizontalPodAutoscaler |

## Tech Stack

*   **Core Engine:** Apache Spark 3.5 (PySpark)
*   **Orchestration:** Kubernetes (K8s) via Orbstack/Minikube
*   **Management:** Spark Operator (Helm Chart), KEDA, Prometheus Adapter
*   **Monitoring:** Prometheus (kube-prometheus-stack) & Grafana
*   **Containerization:** Docker
*   **Language:** Python 3.9 - 3.12

## Project Structure

```bash
spark-app/
├── app.py                  # Main PySpark application logic
├── Dockerfile              # Container definition for the Spark driver/executor
├── requirements.txt        # Python dependencies
├── setup.md                # Detailed local setup guide
├── commands.md             # kubectl command reference
├── k8s/
│   ├── spark-app.yaml      # Base SparkApplication CRD manifest (dev)
│   ├── rbac/               # Service Accounts and Role Bindings
│   ├── monitoring/         # ServiceMonitor, JMX config, metrics services
│   └── strategies/
│       ├── 1-dynamic-allocation/
│       │   └── spark-app.yaml
│       ├── 2-keda/
│       │   ├── spark-master.yaml
│       │   ├── spark-worker-deployment.yaml
│       │   ├── keda-scaledobject.yaml
│       │   └── spark-job.yaml
│       └── 3-hpa/
│           ├── spark-master.yaml
│           ├── spark-worker-deployment.yaml
│           ├── hpa.yaml
│           ├── prometheus-adapter-values.yaml
│           └── spark-job.yaml
├── experiment/
│   ├── run_experiment.sh   # Automated benchmark runner
│   ├── cost_model.py       # Cost analysis from CSV results
│   └── results/            # CSV + summary outputs
└── data/
    ├── games/              # Steam dataset (recommendations, games, users)
    └── input/              # UK Land Registry dataset (land-data.csv)
```

## Analysis Modes

The application supports five analysis modes, controlled via arguments in the SparkApplication or Job manifest:

| Mode | Dataset | Rows | Description |
|---|---|---|---|
| `top_games` | Steam | ~16M | Aggregates reviews by `app_id` to identify the most discussed games |
| `sentiment` | Steam | ~16M | Calculates the ratio of Recommended vs Not Recommended reviews |
| `user_activity` | Steam | ~16M | Groups by `user_id` to find the most active contributors |
| `steam_heavy` | Steam | ~16M + 50K + 14M | 3-way join + window functions + weighted scoring (primary benchmark) |
| `property_prices` | UK Land Registry | ~31M | 4 sub-analyses: county pricing, year-on-year trends, property type breakdown, affordability gap |

## Installation & Setup

For a detailed step-by-step guide, please refer to [setup.md](setup.md).

**Prerequisites:**
*   Docker Desktop / Orbstack
*   Kubernetes (enabled)
*   Helm
*   `kubectl`

**Quick Start:**

1.  **Install Spark Operator:**
    ```bash
    helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
    helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace --set webhook.enable=true
    ```

2.  **Build Image:**
    ```bash
    docker build -t jonathan/spark-app:1.5 .
    ```

3.  **Prepare Data:**
    Ensure datasets are in `data/games/` and `data/input/`.

## Usage Guide

1.  **Deploy a Strategy:**
    ```bash
    # Strategy 1: Dynamic Allocation
    kubectl apply -f k8s/strategies/1-dynamic-allocation/spark-app.yaml

    # Strategy 2: KEDA (see setup.md for full deploy steps)
    # Strategy 3: HPA  (see setup.md for full deploy steps)
    ```

2.  **Monitor Progress:**
    Watch the pods spin up and scale automatically:
    ```bash
    kubectl get pods -w
    ```

3.  **Run Full Benchmark:**
    ```bash
    ./experiment/run_experiment.sh steam_heavy
    ```

4.  **View Results:**
    Output files will be generated in the `data/output/` directory on your local machine.
    Experiment metrics are saved to `experiment/results/`.

## License

This project is open-source and available under the [MIT License](LICENSE).
