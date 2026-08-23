-- DSC 650 Portfolio Starter

USE heart_db;

-- 1. Total record count verification
SELECT COUNT(*) AS total_records FROM heart_disease;

-- 2. Target distribution and physiological averages grouped by Heart Disease outcome
SELECT
    target,
    COUNT(*) AS patient_count,
    ROUND(AVG(age), 2) AS avg_age,
    ROUND(AVG(chol), 2) AS avg_cholesterol,
    ROUND(AVG(thalach), 2) AS avg_max_heart_rate,
    ROUND(AVG(trestbps), 2) AS avg_resting_bp
FROM heart_disease
GROUP BY target;

-- 3. Patient metrics grouped by Sex and Chest Pain (cp) type
SELECT
    sex,
    cp,
    COUNT(*) AS patient_count,
    ROUND(AVG(age), 2) AS avg_age,
    ROUND(AVG(oldpeak), 2) AS avg_st_depression
FROM heart_disease
GROUP BY sex, cp
ORDER BY sex, cp;
