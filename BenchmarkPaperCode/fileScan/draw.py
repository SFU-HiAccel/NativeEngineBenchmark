import matplotlib.pyplot as plt

# Data for the plot
labels = ['2^20', '2^21', '2^22', '2^23', '2^24', '2^25']

vanilla_values = [1.3, 13, 25, 28, 54, 93]
ck_values = [12, 14, 31, 55, 91, 226]
velox_values = [7.8, 12.5, 24.6, 48, 90, 144]
forc_values = [119, 139, 146, 377, 401, 507]
spark_rapids_values = [45.4, 55.5, 64.5, 72.7, 78.8, 119]

# Plotting the line chart
plt.figure(figsize=(10, 6))

plt.plot(labels, vanilla_values, label='Vanilla (JVM CPU)', marker='o', linestyle='-', color='#427AB2')
plt.plot(labels, ck_values, label='ClickHouse (C++ CPU)', marker='s', linestyle='-', color='#F09148')
plt.plot(labels, velox_values, label='Velox (C++ CPU)', marker='^', linestyle='-', color='#FF9896')
plt.plot(labels, forc_values, label='FORC (C++ FPGA)', marker='d', linestyle='-', color='#7DAEE0')
plt.plot(labels, spark_rapids_values, label='Spark RAPIDS (C++ GPU)', marker='x', linestyle='-', color='#C59D94')

plt.xlabel('Row Size')
plt.ylabel('MRow/s')
#plt.title('Comparison of Different Native Engine Performance on ORC FileScan Decoding')

# Customize x-axis labels with LaTeX formatting
plt.xticks(ticks=labels, labels=[r'$2^{20}$', r'$2^{21}$', r'$2^{22}$', r'$2^{23}$', r'$2^{24}$', r'$2^{25}$'])

# Customize the legend
plt.legend(loc='best', fontsize=10, title='Engine Types')
plt.grid(True)
plt.tight_layout()

# Save and display the plot
plt.savefig("./gpuFPGACPUThroughput.png")
plt.show()
