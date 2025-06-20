import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load count values
df = pd.read_csv("count_values.csv", header=None, names=["countValue"])

# Basic statistics
mean_val = df['countValue'].mean()
median_val = df['countValue'].median()
std_dev = df['countValue'].std()

# Print stats
print(f"Mean: {mean_val:.2f}")
print(f"Median: {median_val}")
print(f"Standard Deviation: {std_dev:.2f}")
print(f"Deviation from expected (100000): {mean_val - 100000:.2f}")

# Histogram
plt.figure(figsize=(10,6))
sns.histplot(df['countValue'], bins=50, kde=True)
plt.axvline(100000, color='red', linestyle='--', label='Expected (100000)')
plt.title('Distribution of countValue')
plt.xlabel('countValue')
plt.ylabel('Frequency')
plt.legend()
plt.grid(True)
plt.show()

# Boxplot
plt.figure(figsize=(8,2))
sns.boxplot(x=df['countValue'])
plt.axvline(100000, color='red', linestyle='--', label='Expected')
plt.title('Boxplot of countValue')
plt.legend()
plt.show()
