# Interview Talking Points

Use this file to prepare a concise explanation of the project for a technical interview.

## 30-Second Overview

> I built an end-to-end distributed data pipeline using Apache NiFi, HDFS, Hive, Spark MLlib, YARN, and HBase to automate heart disease risk prediction from raw clinical datasets. NiFi ingests patient data from a remote HTTP source directly into HDFS storage. Hive provides a schema-on-read SQL layer over the CSV files, which PySpark—scheduled and executed via YARN—queries to train and evaluate a classification model. Finally, Spark writes prediction vectors and evaluation metrics into HBase via Thrift for low-latency retrieval. The repository preserves the architecture, code, and execution evidence so the entire system can be demonstrated without a live cloud environment.

---

## Be Ready to Explain

### What problem or analytical task did your dataset support?

The dataset supported binary medical outcome classification: predicting whether a patient has heart disease (1) or does not have heart disease (0) based on 13 clinical and demographic attributes, including age, chest pain type (`cp`), resting blood pressure (`trestbps`), serum cholesterol (`chol`), and maximum heart rate (`thalach`).

### Walk through the complete data flow.

1. **Ingestion:** Apache NiFi fetches the raw `heart.csv` file from a remote GitHub HTTP endpoint using `InvokeHTTP`, updates the target metadata with `UpdateAttribute`, and streams it into HDFS using `PutHDFS`.
2. **Storage & Structuring:** The raw file lands in HDFS at `/user/root/data/heart.csv`. An Apache Hive external table (`heart_db.heart_disease`) maps a strongly-typed SQL schema over the raw directory without mutating the original CSV.
3. **Model Training & Execution:** A PySpark application is submitted to **YARN** (`spark-submit`), reading the Hive table into a DataFrame. Features are vectorised using `VectorAssembler` and passed to a binary classification algorithm (Logistic Regression / Random Forest).
4. **Serving & Persistence:** PySpark connects to the **HBase Thrift server** using the `happybase` Python library to write evaluation metrics (AUC-ROC, Accuracy) and individual patient prediction records into an HBase table for low-latency serving.

### Why did you use NiFi?

NiFi serves as an automated, scalable ingestion and orchestration layer. It handles scheduled HTTP fetching, metadata enrichment, backpressure management, and reliable delivery to HDFS with built-in error handling and zero custom code requirements.

### What role did HDFS play?

HDFS acted as the fault-tolerant, distributed landing zone (data lake). It decouples initial data ingestion from downstream compute frameworks, allowing both Hive and PySpark to access raw and structured files across a distributed cluster.

### How did you design the Hive table?

I designed an **EXTERNAL** table (`heart_db.heart_disease`) pointing to `/user/root/data/`. Key design choices included:
* Mapping all 14 clinical fields to explicit numerical data types (`INT` and `DOUBLE`) to prepare data directly for PySpark MLlib.
* Setting `FIELDS TERMINATED BY ','` for CSV parsing.
* Configuring `TBLPROPERTIES ("skip.header.line.count"="1")` to ensure header rows were ignored during SQL queries and aggregations.

### What data did Spark read from Hive?

Spark read the structured DataFrame directly from the Hive catalog using `spark.table("heart_db.heart_disease")`. This allowed Spark to leverage Hive's pre-defined column names and data types, eliminating manual CSV parsing or schema inference overhead in PySpark.

### Which MLlib algorithm did you use and why?

I used **Logistic Regression** (alongside Random Forest Classification for comparison). Logistic Regression is particularly well-suited for binary healthcare classification because it produces well-calibrated prediction probabilities and offers clear feature interpretability, allowing clinicians to evaluate how individual parameters (such as `thalach` or `cp`) influence predicted heart disease risk.

### How did you evaluate the model?

The dataset was split into an 80% training set and a 20% testing holdout set. The model was evaluated using:
* `BinaryClassificationEvaluator` to compute the **Area Under ROC (AUC-ROC)** (achieving ~0.88).
* `MulticlassClassificationEvaluator` to measure overall **Accuracy** (achieving ~82%).

### What did YARN do during Spark execution?

YARN served as the cluster resource manager. When `spark-submit` was executed with `--master yarn`, YARN dynamically allocated CPU cores and container memory across worker nodes, scheduled Spark executor processes, and monitored job execution until completion.

### Why did you write model metrics into HBase?

HBase provides a NoSQL column-oriented storage layer designed for real-time, low-latency key-value lookups. Storing prediction outcomes and model performance metrics in HBase enables external clinical dashboards or REST APIs to retrieve patient risk scores in milliseconds using row-key queries without scanning entire HDFS directories.

### How did the final HBase scan prove the pipeline worked?

Running a scan in HBase (or querying via `happybase`) returned structured row entries containing execution timestamps, patient IDs, predicted outcome classes, and probability scores. This verified that data successfully traversed every stage of the distributed architecture—from raw HTTP ingestion to NoSQL serving.

### What was the most difficult technical problem?

The most challenging issue was path and parameter resolution in NiFi (`/home/root/...` vs `/root/...` and `#{USER}` syntax), combined with `Connection Refused` exceptions on `PutHDFS` when Hadoop containers were initializing.

### How did you troubleshoot it?

1. **Path Resolution:** I shelled into the container, ran `find / -name core-site.xml` to discover actual configuration paths, and identified that Linux root directories reside at `/root/` rather than `/home/root/`.
2. **NiFi Syntax:** I removed parameter wrapper brackets `#{}` when supplying literal string paths.
3. **Daemon Readiness:** I inspected container status with `docker ps` and added initialization delays to ensure Hadoop NameNode RPC ports (`8020`/`9000`) were active before starting NiFi processor flows.

### What would you change for production?

If deploying this architecture into production, I would implement:
* **Security & Access Control:** Kerberos authentication across HDFS, YARN, and HBase, managed by Apache Ranger for granular Role-Based Access Control (RBAC).
* **High Availability (HA):** Active/Standby NameNodes with Quorum Journal Manager (QJM), ZooKeeper-backed HBase Master HA, and multi-node NiFi clustering.
* **Streaming Ingestion:** Replace scheduled batch HTTP pulls with Apache Kafka to ingest real-time patient telemetry streams directly into PySpark Structured Streaming.
* **Observability:** Integrate Prometheus and Grafana for monitoring JVM memory, YARN queue pressure, and NiFi processor latencies.