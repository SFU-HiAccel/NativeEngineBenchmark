import matplotlib.pyplot as plt

# Source data
data = {
    "Velox": [0.267629574, 0.232668607, 0.23918867133333332, 0.317376085, 0.45087129266666665, 0.5627934246666667, 0.7657087320000001],
    "ClickHouse": [0.559849431, 0.532196726, 0.633537657, 0.72, 1.243185299, 1.283407522, 2.3255421253333335],
    "DataFusion": [0.6495564056666666, 0.723977573, 0.678159333, 0.6951514206666666, 1.2677986953333333, 1.3528281603333336, 1.7010644163333335],
    "Vanilla Spark": [1.163866127, 1.269925421, 1.289504131, 1.504647821, 3.74882093, 4.356411615, 4.789078844],
}

# Queries
queries = ["0.0001", "0.001", "0.01", "0.1", "1", "10", "100"]
colors = ["#ff7f0f", "#2da02d", "#d72828", "#2077b5"]
# Plotting
plt.figure(figsize=(10, 6))
for i, (label, durations) in enumerate(data.items()):
    plt.plot(queries, durations, marker='o', label=label, color=colors[i % len(colors)],linewidth=2.5)
plt.xlabel("Pecentage of Cardinality of group by key (%)", fontsize=12)
plt.ylabel("Time (s)", fontsize=12)
plt.legend(fontsize=12)
plt.grid(True, which="major", linestyle="--", linewidth=0.5, axis='y')
plt.tight_layout()
plt.show()
plt.savefig("./drawSum.png")