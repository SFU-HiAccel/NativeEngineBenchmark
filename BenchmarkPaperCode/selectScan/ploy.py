import matplotlib.pyplot as plt
import numpy as np

# Data provided (updated)
percentages = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

vanilla_orc_times = [
1.73821016,
1.778236504,
1.741155304,
1.759182385,
1.878408646,
1.806432884,
1.850424272,
1.909679149,
1.82,
1.84
]

velox_orc_times = [
1.15941143,
1.216583754,
1.176557536,
1.210820206,
1.163912593,
1.198359313,
1.366235839,
1.371512833,
1.338957697,
1.381527627
]

clickhouse_orc_times = [
1.063203936,
1.034696043,
0.967609784,
1.010656756,
0.904271948,
0.992030878,
0.972953029,
1.027398975,
1.060262162,
1.09177533
    ]

extra_datafusion_orc_times = [
    [2.429061072],
    [2.45],
    [2.40],
    [2.38],
    [2.40],
    [2.47],
    [2.51],
    [2.6],
    [2.63],
    [2.65]
]

vanilla_parquet_times = [
    3.51, 3.53, 3.606207449, 4.09,  4.186436424, 4.919358917, 4.229601918, 3.932213315, 3.58, 3.21
    ]
velox_parquet_times = [2.37, 2.55, 3.08, 3.36, 3.59, 2.96, 2.92, 2.74, 2.97, 2.965]

clickhouse_parquet_times = [1.878944675, 2.087282006, 2.031050881, 2.088408475, 2.471831342, 2.289787259, 2.308639092, 2.376252802, 2.354344245, 2.402404409]

datafusion_parquet_times = [2.84218375, 2.88722037, 2.884056331, 3.032919351, 3.292575235, 3.4018, 3.563971785, 3.832792255, 3.32, 3.194841724]

# Plotting ORC and Parquet comparisons separately as subplots
fig, axes = plt.subplots(1, 2, figsize=(20, 8))
datafusion_orc_times = [np.mean(times) for times in extra_datafusion_orc_times]

# ORC Execution Time Comparison
axes[0].plot(percentages, vanilla_orc_times, marker='v', linestyle='-', label='ORC Execution Time (Vanilla Spark)', linewidth=2)
axes[0].plot(percentages, velox_orc_times, marker='s', linestyle='--', label='ORC Execution Time (Velox)', linewidth=2)
axes[0].plot(percentages, clickhouse_orc_times, marker='^', linestyle='-.', label='ORC Execution Time (ClickHouse)', linewidth=2)
axes[0].plot(percentages, datafusion_orc_times, marker='d', linestyle=':', label='ORC Execution Time (DataFusion)', linewidth=2)

axes[0].set_xlabel('Selectivity (%)')
axes[0].set_ylabel('Execution Time (seconds)')
axes[0].set_title('Comparison of ORC Execution Time in Vanilla Spark, Velox, and ClickHouse')
axes[0].legend()
axes[0].grid(True)
axes[0].set_xticks(percentages)

# Parquet Execution Time Comparison
axes[1].plot(percentages, vanilla_parquet_times, marker='v', linestyle='-', label='Parquet Execution Time (Vanilla Spark)', linewidth=2)
axes[1].plot(percentages, velox_parquet_times, marker='s', linestyle='--', label='Parquet Execution Time (Velox)', linewidth=2)
axes[1].plot(percentages, clickhouse_parquet_times, marker='^', linestyle='-.', label='Parquet Execution Time (ClickHouse)', linewidth=2)
axes[1].plot(percentages, datafusion_parquet_times, marker='d', linestyle=':', label='Parquet Execution Time (DataFusion)', linewidth=2)

axes[1].set_xlabel('Selectivity (%)')
axes[1].set_ylabel('Execution Time (seconds)')
axes[1].set_title('Comparison of Parquet Execution Time in Vanilla Spark, Velox, ClickHouse, and DataFusion')
axes[1].legend()
axes[1].grid(True)
axes[1].set_xticks(percentages)

# Displaying the plot
plt.tight_layout()
plt.show()
plt.savefig("./orcAndParquet.png")

