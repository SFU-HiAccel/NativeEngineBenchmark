import matplotlib.pyplot as plt
import numpy as np

# Data for each group
queries = [
    "select id1, sum(v1)",
    "select id1, id2, sum(v1)",
    "select id3, sum(v1), mean(v3)",
    "select id4, mean(v1), mean(v2), mean(v3)",
    "select id6, sum(v1), sum(v2), sum(v3)",
    "select id3, max(v1) - min(v2)",
    "select id6, largest2_v3",
    "select id2, id4, pow(corr(v1, v2), 2)",
    "select id1, id2, id3, id4, id5, id6, sum(v3), count(*)"
]

vanilla = [
    0.332196925, 0.992852105, 2.484410151, 0.755089272, 2.110120322,
    2.009743471, 3.033588585, 0.720449824, 3.755675313
]

velox = [
    0.138244844, 0.2468958053, 0.8633678, 0.2407478137, 0.6116281293,
    0.6345765463, 0.699350815, 0.2832567507, 1.4715218207
]

clickhouse = [
    0.134124, 0.349309529, 0.9370970887, 0.2599384287, 0.804610153,
    0.7865396603, 0.6596398957, 0.2680766197, 2.3559156233
]

blaze = [
    0.327179733, 0.9334832983, 1.925866502, 0.578787399, 1.773040304,
    1.699134345, 0.9196398957, 0.502391695, 2.669429825
]

# Group data for plotting
data = [vanilla, velox, clickhouse, blaze]
labels = ['Vanilla Spark', 'Spark+Velox', 'Spark+ClickHouse', 'Spark+DataFusion']
x = np.arange(len(queries))

# Plot configuration
width = 0.2
fig, ax = plt.subplots(figsize=(12, 6))

# Create bar plots for each group with custom colors
custom_colors = ['#8ECFC9', '#FFBE7A', '#FA7F6F', '#82B0D2']  # Custom colors for each group
for i, group_data in enumerate(data):
    ax.bar(x + i * width, group_data, width, label=labels[i], color=custom_colors[i])
# Customize the plot
ax.set_xlabel('Queries', fontsize=12)
ax.set_ylabel('Average Duration (seconds)', fontsize=12)
ax.set_title('', fontsize=14)
ax.set_xticks(x + width * 1.5)
ax.set_xticklabels(['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q7', 'Q8', 'Q9', 'Q10'], rotation=45, ha='right', fontsize=10)
ax.legend()

# Display the plot
plt.tight_layout()
plt.show()
plt.savefig("./barplot.png")
