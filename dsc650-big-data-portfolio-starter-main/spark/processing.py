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
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler


def get_spark_session(app_name="HiveDataProcessing"):
    """Initialize Spark Session with Hive metastore support."""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()


def read_from_hive(spark, db_name="heart_db", table_name="heart_disease"):
    """Read raw tabular dataset directly from Hive warehouse."""
    df = spark.sql(f"SELECT * FROM {db_name}.{table_name}")
    return df


def build_preprocessing_pipeline(categorical_cols, numeric_cols, label_col="target"):
    """
    Constructs a PySpark ML Pipeline for feature indexing and vector assembly.
    """
    stages = []

    # 1. Index categorical features
    indexed_cat_cols = []
    for col in categorical_cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="skip")
        stages.append(indexer)
        indexed_cat_cols.append(f"{col}_indexed")

    # 2. Index target label if categorical
    label_indexer = StringIndexer(inputCol=label_col, outputCol="label", handleInvalid="skip")
    stages.append(label_indexer)

    # 3. Assemble all features into a dense vector
    feature_cols = numeric_cols + indexed_cat_cols
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    stages.append(assembler)

    # 4. Standardize numeric features
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    stages.append(scaler)

    pipeline = Pipeline(stages=stages)
    return pipeline


def preprocess_data(spark, db_name="default", table_name="raw_patient_data"):
    """Executes the Hive query and transformations."""
    raw_df = read_from_hive(spark, db_name, table_name).dropna()

    # Example feature schema definition
    categorical_cols = ["gender", "admission_type"]
    numeric_cols = ["age", "time_in_hospital", "num_lab_procedures"]

    pipeline = build_preprocessing_pipeline(categorical_cols, numeric_cols, label_col="readmitted")
    pipeline_model = pipeline.fit(raw_df)
    processed_df = pipeline_model.transform(raw_df)

    return processed_df.select("patient_id", "label", "features")