import matplotlib.pyplot as plt
import numpy as np
from math import pi
from matplotlib.lines import Line2D

# Define the categories and the values for each database
categories = [
    "High Cardinality Aggregation", "Low Cardinality Aggregation", "Hash Join", "ORC Scan", "Parquet Scan","Sort"
]

# Values for each database
values = {
    "Vanilla Spark": [0.4, 0.4, 0.6, 0.6, 0.6, 0.4],
    "Spark + Velox":      [1, 1, 1, 0.8, 1, 0.8],
    "Spark + ClickHouse": [0.6, 0.8, 1, 1, 1, 1],
    "Spark + DataFusion": [0.8, 0.8, 0.8, 0.4, 0.8, 0.6]
}

# Number of categories
N = len(categories)

# Create the angle for each category
angles = [n / float(N) * 2 * pi for n in range(N)]
angles += angles[:1]

# Initialize radar chart
fig, ax = plt.subplots(figsize=(14, 10), subplot_kw={"polar": True})
ax.spines['polar'].set_visible(False)

colors = ["#354747", "#02b0a8","#ff5d67", "#f0c917"]
markers = ['v', 'o', '^', 's']

# Plot data for each database with corresponding color
for i, (label, data) in enumerate(values.items()):
    data += data[:1]  # Repeat the first value to close the circle
    ax.plot(angles, data, linewidth=4, linestyle='solid', label=label, color=colors[i], marker=markers[i], markersize=16)
    ax.fill(angles, data, alpha=0.2, color=colors[i])

# Add the labels
ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
ax.set_yticklabels(["", "", "", "", ""], color="gray", fontsize=10)
ax.set_xticks(angles[:-1])
ax.set_xticklabels(["", "", "", "", "", ""], fontsize=14)

# Add legend
legend_elements = [
    Line2D([0], [0], color=colors[0], lw=4, marker=markers[0], label='Vanilla Spark', markersize=16),
    Line2D([0], [0], color=colors[1], lw=4, marker=markers[1], label='Spark + Velox', markersize=16),
    Line2D([0], [0], color=colors[2], lw=4, marker=markers[2], label='Spark + ClickHouse', markersize=16),
    Line2D([0], [0], color=colors[3], lw=4, marker=markers[3], label='Spark + DataFusion', markersize=16)
]
fig.subplots_adjust(right=0.6)  # Make room on the right side

plt.legend(
    handles=legend_elements,
    loc='upper left',
    bbox_to_anchor=(1.1, 0.5),  # x=1.2 puts it outside the right edge
    fontsize=20,
    frameon=False  # Optional: remove box around legend
)
# Show the plot
plt.show()
plt.savefig("./six_star.png")


