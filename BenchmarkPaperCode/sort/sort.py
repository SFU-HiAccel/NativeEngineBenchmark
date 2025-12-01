import pandas as pd
import matplotlib.pyplot as plt

# Define the data (switched to query-centric for easier plotting)
data = {
    'Query': ['Low Card', 'High Card', 'Decimal Sort', 'Top N', 'Multi Column'],
    'Vanilla Spark': [22.756, 20.118, 25.733, 4.383, 30.085],
    'Spark + Velox': [14.091, 14.156, 15.914, 1.523, 15.18],
    'Spark + ClickHouse': [8.428, 8.854, 9.65, 1.731, 12.939],
    'Spark + DataFusion': [18.04, 15.156, 20.915, 2.951, 18.536]
}

# Create DataFrame
df = pd.DataFrame(data)
df.set_index('Query', inplace=True)
df.index.name = None 

# Keep consistent engine order and colors
engine_order = ["Vanilla Spark", "Spark + Velox", "Spark + ClickHouse", "Spark + DataFusion"]
colors = {
    "Vanilla Spark": "#354747",
    "Spark + Velox": "#02b0a8",
    "Spark + ClickHouse": "#ff5d67",
    "Spark + DataFusion": "#f0c917"
}

# Plot grouped bar chart
fig, ax = plt.subplots(figsize=(24, 12))
df[engine_order].plot(kind='bar',
                      ax=ax,
                      width=0.8,
                      color=[colors[eng] for eng in engine_order])

ax.set_ylabel('Time (s)', fontsize=32)
ax.set_xticklabels(ax.get_xticklabels(), rotation=0, ha='center', fontsize=28)
ax.tick_params(axis='both', labelsize=28)
ax.grid(True, axis='y', linestyle='--', alpha=0.6)

ax.legend(fontsize=30,
          bbox_to_anchor=(0.5, 1.02),
          loc='lower center',
          ncol=4)

plt.tight_layout()
plt.subplots_adjust(top=0.92)  # leave room for legend
plt.savefig("./sort.pdf", bbox_inches='tight', dpi=350)
plt.show()

# --- Compute average speedup ---
vanilla = df["Vanilla Spark"]

speedups = {}
for engine in ["Spark + Velox", "Spark + ClickHouse", "Spark + DataFusion"]:
    speedups[engine] = (vanilla / df[engine]).mean()

print("Average Speedups over Vanilla Spark:")
for engine, sp in speedups.items():
    print(f"{engine}: {sp:.2f}x")
