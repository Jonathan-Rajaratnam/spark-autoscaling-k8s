# Comparative Analysis on AutoScaling Strategies forSpark on Kubernetes 

**Interim Progression Demonstration (IPD)**

**Student Name:** Jonathan Rajaratnam  <br />
**Student ID:** w1953878  <br />
**Module:** 6COSC023W - Computer Science Final Project <br />
**Dataset:** [Game Recommendations on Steam (Kaggle)](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam?select=recommendations.csv)

This project demonstrates a data engineering pipeline running Apache Spark on Kubernetes. It analyzes a large dataset and performs transformations and aggregations to extract insights. The system is designed to run locally using Docker and Kubernetes (Orbstack/Minikube) while simulating a large-scale cluster environment through **Dynamic Resource Allocation**.

---

## 📑 Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Analysis Modes](#analysis-modes)
- [Installation & Setup](#installation--setup)
- [Usage Guide](#usage-guide)
- [License](#license)

---

## Key Features

*   **Cloud-Native Spark:** Deployed using the `SparkOperator` on Kubernetes, removing the need for legacy resource managers like YARN.
*   **Dynamic Autoscaling:** Configured with Spark's Dynamic Allocation to automatically spin up executor pods based on workload (simulated with 300 data partitions) and scale down when idle.
*   **Infrastructure as Code (IaC):** Entire application lifecycle managed through declarative Kubernetes YAML manifests.
*   **Multi-Mode Analysis:** Single codebase supporting multiple aggregation strategies via command-line arguments.
*   **Observability:** Integrated with Prometheus and Grafana for monitoring cluster metrics (JMX).

## Tech Stack

*   **Core Engine:** Apache Spark 3.5 (PySpark)
*   **Orchestration:** Kubernetes (K8s) via Orbstack/Minikube
*   **Management:** Spark Operator (Helm Chart)
*   **Containerization:** Docker
*   **Language:** Python 3.9 - 3.12

## Project Structure

```bash
spark-app/
├── app.py                  # Main PySpark application logic
├── Dockerfile              # Container definition for the Spark driver/executor
├── requirements.txt        # Python dependencies
├── setup.md                # Detailed local setup guide
├── k8s/
│   ├── spark-app.yaml      # Main SparkApplication CRD manifest
│   ├── spark-job.yaml      # Alternative CronJob approach (optional)
│   └── rbac/               # Service Accounts and Role Bindings
└── data/
    └── recommendations.csv # Input dataset (mounted to pods)
```

## Analysis Modes

The application supports three distinct analysis modes, controlled via arguments in `spark-app.yaml`:

1.  **`top_games`**: Aggregates reviews by `app_id` to identify the most discussed games.
2.  **`sentiment`**: Calculates the ratio of "Recommended" vs. "Not Recommended" reviews.
3.  **`user_activity`**: Groups by `user_id` to find the most active contributors on the platform.

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
    helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace
    ```

2.  **Build Image:**
    ```bash
    docker build -t jonathan/spark-app:1.4 .
    ```

3.  **Prepare Data:**
    Ensure `recommendations.csv` is in the `data/` directory.

## Usage Guide

1.  **Deploy the Application:**
    ```bash
    kubectl apply -f k8s/spark-app.yaml
    ```

2.  **Monitor Progress:**
    Watch the pods spin up and scale automatically:
    ```bash
    kubectl get pods -w
    ```

3.  **View Results:**
    Output files will be generated in the `data/output/` directory on your local machine.

## License

This project is open-source and available under the [MIT License](LICENSE).
