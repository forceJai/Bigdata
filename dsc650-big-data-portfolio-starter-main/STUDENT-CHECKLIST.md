# DSC 650 Final Project — GitHub Submission Checklist

Complete this checklist before submitting your repository URL.

## Repository

- [x] My final project is in a repository under my own GitHub account.
- [x] I did not submit changes or a pull request to the instructor's starter repository.
- [x] The instructor can access my repository.
- [x] I kept the required folder structure.
- [x] The root `README.md` renders correctly.

## Objective 1 — NiFi → HDFS

- [x] `nifi/flow-definition.json` contains my final NiFi flow.
- [x] `nifi/README.md` explains the data source, processors, processor roles, and HDFS destination.
- [x] `nifi/screenshots/nifi-flow.png` shows my final flow design.
- [x] `nifi/screenshots/nifi-running.png` shows the flow running with data visible in queues.
- [x] `nifi/screenshots/hdfs-ingestion-verification.png` shows `hdfs dfs -ls` confirming ingestion.

## Objective 2 — Hive

- [x] `hive/create_tables.sql` contains my table creation / load SQL.
- [x] `hive/queries.sql` contains my representative validation and aggregation queries.
- [x] `hive/README.md` explains my schema design and what the queries demonstrate.
- [x] `hive/screenshots/hive-load-results.png` shows successful loading.
- [x] `hive/screenshots/hive-query-results.png` shows query and aggregation results.

## Objective 3 — Environment Setup

- [x] `docs/screenshots/package-installation.png` shows the required package installation.
- [x] `docs/screenshots/hbase-thrift-server.png` shows the HBase Thrift server running.
- [x] `docs/project-summary.md` explains why the packages and Thrift server are required.

## Objective 4 — HBase Table Creation

- [x] `hbase/commands.txt` contains my HBase table-creation commands.
- [x] `hbase/README.md` explains my row key and column-family design.
- [x] `hbase/screenshots/hbase-empty-scan.png` shows the empty table before Spark writes metrics.

## Objective 5 — PySpark MLlib

- [x] My complete working PySpark MLlib source code is in `spark/`.
- [x] Spark reads the project data from Hive.
- [x] My code trains and evaluates an MLlib model.
- [x] My code generates model-performance metrics for HBase.
- [x] `spark/README.md` explains the algorithm, rationale, input data, transformations, and evaluation results.
- [x] `spark/screenshots/spark-training-output.png` shows successful training.
- [x] `spark/screenshots/spark-ml-evaluation.png` shows the evaluation metric(s).

## Objective 6 — Spark Submit / YARN

- [x] `spark/README.md` includes my `spark-submit` command.
- [x] `spark/screenshots/spark-submit-output.png` shows successful execution/log output through YARN.

## Objective 7 — HBase Verification

- [x] `hbase/screenshots/hbase-populated-scan.png` shows the populated table after Spark runs.
- [x] `hbase/README.md` explains the metrics written by Spark and how the scan verifies the pipeline.

## Overall Documentation

- [x] `architecture/architecture-diagram.png` is present.
- [x] `docs/project-summary.md` includes my implementation summary.
- [x] I documented what worked and what did not.
- [x] I documented meaningful challenges and how I addressed them.
- [x] I summarized the final results.
- [x] I included lessons learned.
- [x] I explained what I would change for a production deployment.
- [x] My dataset is my own and is not a course-provided example dataset.
- [x] My direct GitHub dataset URL is documented.

## Security

- [x] No passwords are committed.
- [x] No API keys or access tokens are committed.
- [x] No private keys or certificates are committed.
- [x] No sensitive or restricted data is committed.
- [x] No personally identifiable information is exposed.
- [x] No instructor solution material is included.
- [x] My screenshots do not expose credentials or sensitive environment information.

## Final Submission

- [x] All starter placeholders have been replaced with my own work.
- [x] All required images display correctly in GitHub.
- [x] All links in the repository work.
- [x] My repository is complete.
- [x] I submitted the GitHub repository URL in the Week 11 Final Project assignment area.
