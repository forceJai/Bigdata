--DSC 650 Portfolio Starter
CREATE DATABASE IF NOT EXISTS heart_db;
USE heart_db;

DROP TABLE IF EXISTS heart_disease;

CREATE EXTERNAL TABLE IF NOT EXISTS heart_disease (
    age INT,
    sex INT,
    cp INT,
    trestbps INT,
    chol INT,
    fbs INT,
    restecg INT,
    thalach INT,
    exang INT,
    oldpeak DOUBLE,
    slope INT,
    ca INT,
    thal INT,
    target INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/heart.csv'
TBLPROPERTIES ("skip.header.line.count"="1");