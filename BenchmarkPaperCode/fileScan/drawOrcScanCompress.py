import matplotlib.pyplot as plt
import numpy as np

# plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
# plt.rcParams['ps.fonttype'] = 42
# plt.rcParams['font.family'] = 'sans-serif'
# plt.rcParams['font.sans-serif'] = ['Helvetica']

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(26, 11))  # Changed from 15,6 to 9,3.6
plt.subplots_adjust(wspace=2.5)  # Increase spacing between subplots

# First subplot - Performance comparison
labels = ['2^20', '2^21', '2^22', '2^23', '2^24', '2^25', '2^26', '2^27', '2^28', '2^29']
vanilla_values = [7.3, 11.1, 11.77, 13.9, 14.17, 14.4, 14.4, 16.5, 20.6, 29.6]
clickhouse_values = [12, 14, 31, 55, 90.86, 113.8, 123, 125, 131, 135]
velox_values = [7.8, 12.5, 24.6, 48, 90.2, 103, 104, 109, 112, 117]
forc_values = [272, 307.9, 325, 349, 372.82, 402, 432, 522, 576, 639]
rapids_values = [82.93,
106.44,
126.75,
148.71,
162.8,
176.3,
196.9,
217.5,
254.2,
298.7]



ax1.plot(labels, vanilla_values, label='Vanilla Spark', marker='o', linestyle='-', color='#354747', linewidth=6, markersize=16)  # Reduced linewidth and markersize
ax1.plot(labels, clickhouse_values, label='ClickHouse', marker='s', linestyle='-', color='#ff5d67', linewidth=6, markersize=16)
ax1.plot(labels, velox_values, label='Velox', marker='^', linestyle='-', color='#02b0a8', linewidth=6, markersize=16)
ax1.plot(labels, forc_values, label='Velox + FORC(FPGA)', marker='d', linestyle='-', color='#f0c917', linewidth=6, markersize=16)
ax1.plot(labels, rapids_values, label='Spark RAPIDS', marker='x', linestyle='-', color='#2077b5', linewidth=6, markersize=16)

ax1.set_xlabel('Number of Row', fontsize=34)  # Reduced from 28
ax1.set_ylabel('MRow/s', fontsize=34)
ax1.set_xticks(range(len(labels)))
ax1.set_xticklabels([r'$2^{20}$', r'$2^{21}$', r'$2^{22}$', r'$2^{23}$', r'$2^{24}$', 
                     r'$2^{25}$', r'$2^{26}$', r'$2^{27}$', r'$2^{28}$', r'$2^{29}$'],
                     fontsize=6)  # Reduced from 10
ax1.tick_params(axis='both', labelsize=34)  # Reduced from 20
ax1.legend(loc='best', fontsize=34, frameon=False)  # Using prop dict for bold weight
ax1.grid(True, axis='y')

# Second subplot - Query times comparison
# 10.906 12.432 23.562 22.227 13.903 33.366 14.66 16.472 19.254
# Q9 (34.7%), Q16 (34.7%), Q24a (67.1%),
# Q24b (66.8%), Q50 (33.1%), Q64 (44.6%),
# Q75 (44.4%), Q88 (74.2%), Q93 (71.3%)

query_names = ['Q9', 'Q16', 'Q24a', 'Q24b', 'Q50', 'Q64', 'Q75', 'Q88']
file_scan_data = {
    'TableScan': [10.906*0.347, 12.432*0.347, 23.562*0.671, 22.227*0.668, 13.903*0.331, 33.366*0.446, 14.66*0.444, 16.472*0.742],
    'Other': [10.906-10.906*0.347, 12.432-12.432*0.347, 23.562-23.562*0.671, 22.227-22.227*0.668, 13.903-13.903*0.331, 33.366-33.366*0.446, 14.66-14.66*0.444, 16.472-16.472*0.742]
}
forc_file_scan_data = {
    'TableScan': [10.906*0.347/2.3, 12.432*0.347/2.5, 23.562*0.671/3.3, 22.227*0.668/1.9, 13.903*0.331/2.2, 33.366*0.446/2.1, 14.66*0.444/2.6, 16.472*0.742/2.4],
    'Other': [10.906-10.906*0.347, 12.432-12.432*0.347, 23.562-23.562*0.671, 22.227-22.227*0.668, 13.903-13.903*0.331, 33.366-33.366*0.446, 14.66-14.66*0.444, 16.472-16.472*0.742]
}
colors = ['#1f77b4', '#d62728']

num_queries = len(query_names)
bar_width = 0.3
indices = np.arange(num_queries)

file_scan_tablescan = file_scan_data['TableScan']
file_scan_other = file_scan_data['Other']
forc_file_scan_tablescan = forc_file_scan_data['TableScan']
forc_file_scan_other = forc_file_scan_data['Other']

ax2.bar(indices - bar_width/2, file_scan_tablescan, bar_width, label='Velox TableScan', 
        color='white', edgecolor=colors[0], hatch='--')
ax2.bar(indices - bar_width/2, file_scan_other, bar_width, bottom=file_scan_tablescan, 
        label='Other', color='white', edgecolor=colors[1], hatch='xxx')
ax2.bar(indices + bar_width/2, forc_file_scan_tablescan, bar_width, 
        label='Velox + FORC TableScan', color='white', edgecolor=colors[0], hatch='//')
ax2.bar(indices + bar_width/2, forc_file_scan_other, bar_width, bottom=forc_file_scan_tablescan, 
        label='Other', color='white', edgecolor=colors[1], hatch='xxx')

ax2.set_xlabel('Queries', fontsize=34)  # Reduced from 28
ax2.set_ylabel('Time (seconds)', fontsize=32)
ax2.set_xticks(indices)
ax2.set_xticklabels(query_names, ha="center", rotation=90)
ax2.tick_params(axis='both', labelsize=34)  # Reduced from 28

handles, labels = ax2.get_legend_handles_labels()
ax2.legend(handles=[handles[0], handles[2], handles[1]], 
          labels=['Velox TableScan', 'Velox + FORC TableScan', 'Other Operators'],
          fontsize=26,frameon=False)  # Reduced from 15

plt.tight_layout(pad=1.2)  # Reduced from 2.0
plt.savefig("combined_performance_plots.pdf", bbox_inches='tight', dpi=300)
plt.show()
