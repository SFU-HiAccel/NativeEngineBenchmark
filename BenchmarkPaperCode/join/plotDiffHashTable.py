import matplotlib.pyplot as plt
import matplotlib

# Set all font to Times New Roman, but fallback gracefully if not found
#L1i cache:           32K
#L2 cache:            1024K
#L3 cache:            16896K
plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Helvetica']

# Data for Velox
velox_hash_table_sizes = [
    2**3, 2**4, 2**5, 2**6, 
    2**7, 2**8, 2**9, 2**10, 
    2**11, 2**12, 2**13, 2**14, 2**15, 2**16, 2**17, 2**18
]  # Sizes in KB

velox_average_durations = [
    0.168455425, 0.124807997 , 0.123633871, 0.148600998,
    0.14369843, 0.188033611, 0.231805906, 0.338170841,
    0.663097762, 0.7012477, 0.7442607066666667, 0.738355543,
    0.993608365, 1.382947041, 1.843130389, 2.807774087
]  # Durations in seconds

# Data for ClickHouse
clickhouse_average_durations = [
    0.311008259, 0.39222593066666667, 0.3292900123333333, 0.32329885333333336,
    0.31172369533333333, 0.31900215933333337, 0.319944603, 0.3715336123333333,
    0.521724775, 0.509706146, 0.458336315, 0.51451307,
    0.8651957093333333, 1.079855473, 1.6068334976666667, 2.656434005
]  # Durations in seconds

# Data for DataFusion
datafusion_average_durations = [
    0.391405858, 0.342102079, 0.319305061, 0.290103866,
    0.323648324, 0.356503949, 0.334902386, 0.340943908,
    0.742172174, 0.976957429,  0.95120667 ,0.995748599,
    1.6153601560000002, 2.1310921433333334, 2.268861471666667, 3.254717184666667
]  # Durations in seconds

# Data for Vanilla Spark
vanilla_average_durations = [
    0.34512908, 0.386479678, 0.325562629, 0.344472449,
    0.36051064, 0.343552526, 0.354201295, 0.440515251,
    0.981470184, 1.146059241, 1.078733577, 1.104045957,
    2.298064278333333, 2.959280827, 5.78570438, 7.839287804
]  # Durations in seconds

# No conversion to ms, keep durations in seconds for y-axis as in original

# Convert hash table sizes to MB for plotting (since all are < 16MB, this is safe)
velox_hash_table_sizes_mb = [size / 1024 for size in velox_hash_table_sizes]
velox_hash_table_sizes_labels = [f"{size:.0f} MB" if size >= 1 else f"{size*1024:.0f} KB" for size in velox_hash_table_sizes_mb]

# Plot
plt.figure(figsize=(11, 6))

lines1 = plt.plot(velox_hash_table_sizes_mb, vanilla_average_durations, marker='v', label='Vanilla Spark', color='#354747', linewidth=3, markersize=13)
lines2 = plt.plot(velox_hash_table_sizes_mb, velox_average_durations, marker='o', label='Spark + Velox', color='#02b0a8', linewidth=3, markersize=13)
lines3 = plt.plot(velox_hash_table_sizes_mb, clickhouse_average_durations, marker='^', label='Spark + ClickHouse', color='#ff5d67', linewidth=3, markersize=13)
lines4 = plt.plot(velox_hash_table_sizes_mb, datafusion_average_durations, marker='s', label='Spark + DataFusion', color='#f0c917', linewidth=3, markersize=13)

# Labels and title
plt.xscale('log', base=2)
plt.xticks(velox_hash_table_sizes_mb, velox_hash_table_sizes_labels, rotation=45, fontsize=16, fontname="Times New Roman")
plt.xlabel('Hash Table Size', fontsize=24, fontname="Times New Roman")
plt.ylabel('Time (s)', fontsize=24, fontname="Times New Roman")
legend_labels = ["Vanilla Spark", "Spark+Velox", "Spark+ClickHouse", "Spark+DataFusion"]
plt.legend([lines1[0], lines2[0], lines3[0], lines4[0]],
           labels=legend_labels,
           fontsize=14,
           bbox_to_anchor=(0.5, 1.02),
           loc='lower center',
           ncol=4)

plt.grid(True, axis='y')

# Adjusting the size of the y-axis numbers
plt.yticks(fontsize=16, fontname="Times New Roman")

# Save plot before showing (to avoid blank pdf in some backends)
plt.tight_layout()
plt.savefig("./diffHashTablev4.pdf", dpi=300)
plt.show()