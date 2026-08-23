import time
import happybase
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Step 1: Create a Spark session with Hive support
spark = SparkSession.builder \
    .appName("MLlib HeartDiseasePrediction") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Load heart disease dataset from Hive table
heart_df = spark.sql("""
    SELECT age, sex, cp, trestbps, chol, fbs, restecg, 
           thalach, exang, oldpeak, slope, ca, thal, target 
    FROM heart_db.heart_disease
""")

# Step 3: Handle missing values
heart_df = heart_df.na.drop()

# Step 4: Prepare features vector for MLlib
feature_cols = [
    "age", "sex", "cp", "trestbps", "chol", "fbs",
    "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

assembled_df = assembler.transform(heart_df).select("features", "target")

# Step 5: Split data into training (80%) and testing (20%) sets
train_data, test_data = assembled_df.randomSplit([0.8, 0.2], seed=42)

# Step 6: Initialize and train a Logistic Regression binary classification model
lr = LogisticRegression(featuresCol="features", labelCol="target")
lr_model = lr.fit(train_data)

# Step 7: Transform test data to generate predictions
predictions = lr_model.transform(test_data)

# Step 8: Evaluate model performance (AUC-ROC and Accuracy)
evaluator_auc = BinaryClassificationEvaluator(
    labelCol="target",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc_roc = evaluator_auc.evaluate(predictions)

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="target",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator_acc.evaluate(predictions)

print(f"AUC-ROC Score: {auc_roc:.4f}")
print(f"Accuracy Score: {accuracy:.4f}")

# Step 9: Prepare execution metrics payload for HBase persistence
timestamp = time.strftime("%Y%m%d%H%M%S")
row_key = f"RUN#{timestamp}#LOGISTIC_REGRESSION"

metrics_payload = [
    (row_key, 'metrics:auc_roc', str(auc_roc)),
    (row_key, 'metrics:accuracy', str(accuracy)),
    (row_key, 'metrics:algorithm', 'LogisticRegression'),
    (row_key, 'metrics:timestamp', timestamp)
]

# Step 10: Write metrics to HBase via Thrift using happybase
def write_to_hbase_partition(partition):
    connection = happybase.Connection('master', port=9090)
    connection.open()
    table = connection.table('model_metrics')
    for row in partition:
        r_key, column, value = row
        table.put(r_key.encode('utf-8'), {column.encode('utf-8'): value.encode('utf-8')})
    connection.close()

rdd = spark.sparkContext.parallelize(metrics_payload)
rdd.foreachPartition(write_to_hbase_partition)

# Step 11: Stop Spark Session
spark.stop()