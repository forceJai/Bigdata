import matplotlib.pyplot as plt

# Replace with your actual model evaluation metrics from Stage 3
metrics = {
    'AUC-ROC': 0.85,
    'Accuracy': 0.82,
    'Precision': 0.81,
    'Recall': 0.83
}

plt.figure(figsize=(8, 5))
bars = plt.bar(metrics.keys(), metrics.values(), color=['#2b5c8f', '#4682b4', '#6baed6', '#9ecae1'])
plt.ylim(0, 1.0)
plt.ylabel('Score')
plt.title('Heart Disease Logistic Regression Model Evaluation (PySpark ML)')

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.02, f"{yval:.2f}", ha='center', va='bottom')

plt.tight_layout()
plt.savefig('spark-ml-evaluation.png', dpi=300)
print("Saved spark-ml-evaluation.png")