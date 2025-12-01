import pandas as pd
import matplotlib.pyplot as plt


# Merge queries into a single bar for native and non-native categories
merged_data = {
    "Query": ["Q4", "Q23a", "Q24a", "Q50", "Q64", "Q67", "Q93", "Q95"],
    "Non-Native": [46.1, 61.7, 16.3, 51.9, 28.9, 63.6, 71.4, 6.34],
    "Native": [37.1, 41.2, 10.5, 28.8, 23.9, 45.8, 54, 2.9]
}

# Create DataFrame
df_merged = pd.DataFrame(merged_data)

# Create bar plot
x = range(len(df_merged["Query"]))
width = 0.35  # Width of the bars

plt.figure(figsize=(10, 6))
plt.bar(x, df_merged["Non-Native"], width, label="Row-based shuffle", color="skyblue")
plt.bar([i + width for i in x], df_merged["Native"], width, label="Column-based shuffle", color="orange")

# Add labels and title
plt.title("Shuffle Size Comparison (Native vs Non-Native)")
plt.xlabel("Query")
plt.ylabel("Shuffle Size (GB)")
plt.xticks([i + width / 2 for i in x], df_merged["Query"])
plt.legend(title="Category")
plt.tight_layout()

# Show the plot
plt.show()
plt.savefig("size.png")