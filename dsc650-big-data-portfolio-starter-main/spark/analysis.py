"""
DSC 650 Portfolio Starter

Replace this file with your final Spark analysis code if your project uses
a separate analysis script. Delete this file if it is not needed.
"""

import sys
import os
from datetime import datetime
import happybase
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Dynamic path configuration for import compatibility
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from spark.processing import preprocess_data
except ModuleNotFoundError:
    from processing import preprocess_data

HBASE_HOST = 'master'
HBASE_PORT = 9090
TABLE_NAME = 'model_metrics'


def write_metrics_to_hbase(run_id, metrics_dict):
    """Writes aggregate execution and evaluation metrics into HBase."""
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = connection.table(TABLE_NAME)

    row_key = f"RUN#{run_id}#{metrics_dict['algorithm']}".encode('utf-8')
    data = {
        b'metrics:algorithm': metrics_dict['algorithm'].encode('utf-8'),
        b'metrics:timestamp': metrics_dict['timestamp'].encode('utf-8'),
        b'metrics:accuracy': str(metrics_dict['accuracy']).encode('utf-8'),
        b'metrics:auc_roc': str(metrics_dict['auc_roc']).encode('utf-8'),
        b'metrics:precision': str(metrics_dict['precision']).encode('utf-8'),
        b'metrics:recall': str(metrics_dict['recall']).encode('utf-8')
    }

    table.put(row_key, data)
    connection.close()


def write_predictions_partition(partition, run_id):
    """Executes distributed writes of patient predictions from executor nodes to HBase."""
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = connection.table(TABLE_NAME)

    with table.batch(batch_size=500) as b:
        for row in partition:
            patient_id = str(row['patient_id'])
            actual = str(float(row['label']))
            prediction = str(float(row['prediction']))
            probability = str(float(row['probability'][1]))

            row_key = f"RUN#{run_id}#PATIENT#{patient_id}".encode('utf-8')
            b.put(row_key, {
                b'predictions:patient_id': patient_id.encode('utf-8'),
                b'predictions:actual_target': actual.encode('utf-8'),
                b'predictions:predicted_target': prediction.encode('utf-8'),
                b'predictions:probability': probability.encode('utf-8')
            })

    connection.close()


def main():
    spark = SparkSession.builder \
        .appName("HeartDisease_PySparkML_HBase") \
        .enableHiveSupport() \
        .getOrCreate()

    print(">>> Stage 1: Loading & Preprocessing heart_db.heart_disease Table")
    data = preprocess_data(spark, db_name="heart_db", table_name="heart_disease")
    train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

    print(">>> Stage 2: Training Logistic Regression Model")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
    model = lr.fit(train_df)
    predictions = model.transform(test_df)

    print(">>> Stage 3: Computing Performance Metrics")
    evaluator_auc = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label",
                                                  metricName="areaUnderROC")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                      metricName="accuracy")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                       metricName="weightedPrecision")
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                      metricName="weightedRecall")

    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    metrics = {
        "algorithm": "LOGISTIC_REGRESSION",
        "timestamp": timestamp_str,
        "auc_roc": round(evaluator_auc.evaluate(predictions), 4),
        "accuracy": round(evaluator_acc.evaluate(predictions), 4),
        "precision": round(evaluator_prec.evaluate(predictions), 4),
        "recall": round(evaluator_rec.evaluate(predictions), 4)
    }

    print(f"Metrics Output: {metrics}")

    print(">>> Stage 4: Writing Aggregate Metrics to HBase Driver")
    write_metrics_to_hbase(timestamp_str, metrics)

    print(">>> Stage 5: Writing Patient Predictions to HBase Workers")
    predictions.select("patient_id", "label", "prediction", "probability") \
        .rdd.foreachPartition(lambda part: write_predictions_partition(part, timestamp_str))

    print(">>> Job Complete.")
    spark.stop()


if __name__ == "__main__":
    main()