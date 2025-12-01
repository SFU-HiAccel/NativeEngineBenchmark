import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Helvetica']

# Data setup
selectivities = ["0.001", "0.01", "0.1", "1.0", "10.0", "100.0"]

# Vanilla
vanilla_orc = [2.064414119, 2.086971896, 2.185066206, 2.330162861, 4.174961789, 4.403636808]
vanilla_parquet = [0.847872749, 0.858629045, 0.933000584, 1.327609538, 3.511627203, 3.212432073]

# Velox
velox_orc = [1.927667094, 1.973735359, 2.07379644, 2.366815834, 3.154819973, 3.216050186]
velox_parquet = [0.23230226, 0.233914192, 0.276896485, 0.645974021, 1.466209116, 1.533060375]

# ClickHouse
clickhouse_orc = [1.818250306, 1.838212887, 1.789965862, 2.140994317, 2.616984541, 2.833720591]
clickhouse_parquet = [0.402637984, 0.406976506, 0.392095671, 0.746156638, 1.307236078, 1.226612051]

# DataFusion
datafusion_orc = [3.775367007, 3.875640081, 4.016818273, 4.662884945, 5.335402582, 5.342918642]
datafusion_parquet = [0.797939163, 0.81626595, 0.913741359, 1.258062788, 2.218737668, 2.096879106]

plt.rcParams['font.size'] = 24
plt.rcParams['axes.labelsize'] = 24
plt.rcParams['legend.fontsize'] = 24

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# Plot ORC in first subplot
lines1 = ax1.plot(selectivities, vanilla_orc, label="Vanilla", marker='v',color='#354747',linewidth=3,markersize=15)
lines2 = ax1.plot(selectivities, velox_orc, label="Velox", marker='o',color='#02b0a8',linewidth=3,markersize=15)
lines3 = ax1.plot(selectivities, clickhouse_orc, label="ClickHouse", marker='^',color='#ff5d67',linewidth=3,markersize=15)
lines4 = ax1.plot(selectivities, datafusion_orc, label="DataFusion", marker='s', color='#f0c917',linewidth=3,markersize=15)


#ax1.set_title("ORC Performance")
ax1.set_xlabel("Selectivity (%)",fontsize=32)
ax1.set_ylabel("Time (s)",fontsize=32)
#ax1.legend()
ax1.grid(True,axis='y') 
# Plot Parquet in second subplot
ax2.plot(selectivities, vanilla_parquet, label="Vanilla", marker='v', markersize=15, linewidth=3,color='#354747')
ax2.plot(selectivities, velox_parquet, label="Velox", marker='o', markersize=15, linewidth=3,color='#02b0a8')
ax2.plot(selectivities, clickhouse_parquet, label="ClickHouse", marker='^', markersize=15, linewidth=3,color='#ff5d67')
ax2.plot(selectivities, datafusion_parquet, label="DataFusion", marker='s', markersize=15, linewidth=3, color='#f0c917')
#ax2.set_title("Parquet Performance")
ax2.set_xlabel("Selectivity (%)", fontsize=32)
ax2.set_ylabel("Time (s)",fontsize=32)
#ax2.legend()
ax2.grid(True,axis='y') 

ax1.tick_params(axis='both', labelsize=32)  # Change 14 to your desired font size for ORC subplot
ax2.tick_params(axis='both', labelsize=32)  # Change 14 to your desired font size for Parquet subplot


legend_labels = ["Vanilla Spark", "Spark + Velox", "Spark + ClickHouse", "Spark + DataFusion"]
fig.legend([lines1[0], lines2[0], lines3[0], lines4[0]], 
          labels=legend_labels,
          fontsize=24,
          bbox_to_anchor=(0.5, 1.02), 
          loc='lower center', 
          ncol=4)
# Adjust layout
plt.tight_layout()
plt.subplots_adjust(top=1)

# Save and show the plot
plt.savefig("./selectScan.pdf",bbox_inches='tight',dpi=350)
plt.show()