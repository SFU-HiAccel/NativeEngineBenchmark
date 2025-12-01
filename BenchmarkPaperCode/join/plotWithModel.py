import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import make_interp_spline

# Input data
velox_average_durations = [
    0.168455425, 0.124807997, 0.123633871, 0.148600998,
    0.14369843, 0.188033611, 0.231805906, 0.338170841,
    0.663097762, 0.7012477, 0.7442607066666667, 0.738355543,
    0.993608365, 1.382947041, 1.843130389, 2.807774087
]

clickhouse_average_durations = [
    0.311008259, 0.39222593066666667, 0.3292900123333333, 0.32329885333333336,
    0.31172369533333333, 0.31900215933333337, 0.319944603, 0.3715336123333333,
    0.521724775, 0.509706146, 0.458336315, 0.51451307,
    0.8651957093333333, 1.079855473, 1.6068334976666667, 2.656434005
]

datafusion_average_durations = [
    0.391405858, 0.342102079, 0.319305061, 0.290103866,
    0.323648324, 0.356503949, 0.334902386, 0.340943908,
    0.742172174, 0.976957429, 0.95120667, 0.995748599,
    1.6153601560000002, 2.1310921433333334, 2.268861471666667, 3.254717184666667
]

vanilla_average_durations = [
    0.34512908, 0.386479678, 0.325562629, 0.344472449,
    0.36051064, 0.343552526, 0.354201295, 0.440515251,
    0.981470184, 1.146059241, 1.078733577, 1.104045957,
    2.298064278333333, 2.959280827, 5.78570438, 7.839287804
]

# Calculate a simulated curve that is always below the smallest value
min_values = np.minimum.reduce([
    velox_average_durations, clickhouse_average_durations,
    datafusion_average_durations, vanilla_average_durations
])
simulated_curve = [value * 0.8 for value in min_values]

# Generate x-axis labels for hash table sizes
hash_table_sizes = [
    "8 KB", "16 KB", "32 KB", "64 KB", "128 KB", "256 KB", "512 KB", "1024 KB",
    "2048 KB", "4096 KB", "8192 KB", "16384 KB", "32768 KB", "65536 KB", "131072 KB", "262144 KB"
]

# Create a smooth curve for the simulated data
x = np.arange(len(hash_table_sizes))  # Discrete indices for x-axis (hash table sizes)
smooth_x = np.linspace(x.min(), x.max(), 500)  # Generate more points for a smooth curve

# Fit a smoothing spline
simulated_spline = make_interp_spline(x, simulated_curve, k=3)  # Cubic spline
smooth_y = simulated_spline(smooth_x)

# Plot the data with a smooth simulated curve
plt.figure(figsize=(10, 6))
plt.plot(hash_table_sizes, velox_average_durations, label="Velox", marker="o")
plt.plot(hash_table_sizes, clickhouse_average_durations, label="ClickHouse", marker="o")
plt.plot(hash_table_sizes, datafusion_average_durations, label="DataFusion", marker="o")
plt.plot(hash_table_sizes, vanilla_average_durations, label="Vanilla Spark", marker="o")
#plt.plot(smooth_x / (len(hash_table_sizes) - 1) * (len(hash_table_sizes) - 1), smooth_y, label="Hash Join Model", linestyle="--")

# Add labels and title
plt.xlabel("Hash Table Size")
plt.ylabel("Time (s)")
plt.title("Hash Join Performance Comparison")
plt.xticks(range(len(hash_table_sizes)), hash_table_sizes, rotation=45)
plt.legend()
plt.grid()

# Display the plot
plt.show()
plt.savefig("./hashTableWithoutModel.png")