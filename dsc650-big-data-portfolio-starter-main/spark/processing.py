"""
DSC 650 Portfolio Starter

Replace this file with the Spark processing script from your final project.

Recommended portfolio comments:
- What input does the script read?
- What transformations does it perform?
- What output does it create?
- Why was Spark appropriate for this task?
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler


def get_spark_session(app_name="HeartDiseaseDataProcessing"):
    """Initialize Spark Session with Hive metastore support."""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()


def read_from_hive(spark, db_name="heart_db", table_name="heart_disease"):
    """Read dataset directly from Hive warehouse table heart_db.heart_disease."""
    df = spark.sql(f"SELECT * FROM {db_name}.{table_name}")
    return df


def build_preprocessing_pipeline(categorical_cols, numeric_cols, label_col="target"):
    """Constructs PySpark ML Pipeline for feature indexing and vector assembly."""
    stages = []

    # 1. Index categorical features
    indexed_cat_cols = []
    for c in categorical_cols:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="skip")
        stages.append(indexer)
        indexed_cat_cols.append(f"{c}_indexed")

    # 2. Index target label
    label_indexer = StringIndexer(inputCol=label_col, outputCol="label", handleInvalid="skip")
    stages.append(label_indexer)

    # 3. Assemble features into dense vector
    feature_cols = numeric_cols + indexed_cat_cols
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
    stages.append(assembler)

    # 4. Standardize numeric features
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    stages.append(scaler)

    pipeline = Pipeline(stages=stages)
    return pipeline


def preprocess_data(spark, db_name="heart_db", table_name="heart_disease"):
    """Executes Hive read and feature prep for heart_disease table."""
    raw_df = read_from_hive(spark, db_name, table_name).dropna()

    # Add unique patient identifier if not present
    if "patient_id" not in raw_df.columns:
        raw_df = raw_df.withColumn("patient_id", monotonically_increasing_id() + 1000)

    # Define features based on standard heart disease schema
    all_cols = raw_df.columns
    target_col = "target" if "target" in all_cols else ("heart_disease" if "heart_disease" in all_cols else "label")

    ignore_cols = ["patient_id", target_col]
    numeric_cols = [c for c, t in raw_df.dtypes if t in ["int", "double", "float"] and c not in ignore_cols]
    categorical_cols = [c for c, t in raw_df.dtypes if t == "string" and c not in ignore_cols]

    pipeline = build_preprocessing_pipeline(categorical_cols, numeric_cols, label_col=target_col)
    pipeline_model = pipeline.fit(raw_df)
    processed_df = pipeline_model.transform(raw_df)

    return processed_df.select("patient_id", "label", "features")