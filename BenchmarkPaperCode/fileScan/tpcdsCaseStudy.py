import matplotlib.pyplot as plt
import numpy as np

def plot_query_times(query_names, file_scan_data, forc_file_scan_data, colors):
    num_queries = len(query_names)
    bar_width = 0.3
    indices = np.arange(num_queries)

    fig, ax = plt.subplots(figsize=(10, 6))

    # 绘制 FileScan 的数据
    file_scan_tablescan = file_scan_data['TableScan']
    file_scan_other = file_scan_data['Other']

    ax.bar(indices - bar_width/2, file_scan_tablescan, bar_width, label='Velox TableScan', color='white',edgecolor=colors[0],hatch='--')
    ax.bar(indices - bar_width/2, file_scan_other, bar_width, bottom=file_scan_tablescan, label='Other', color='white',edgecolor=colors[1], hatch='xxx')

    # 绘制 FORC FileScan 的数据
    forc_file_scan_tablescan = forc_file_scan_data['TableScan']
    forc_file_scan_other = forc_file_scan_data['Other']

    ax.bar(indices + bar_width/2, forc_file_scan_tablescan, bar_width, label='Velox + FORC TableScan', color='white',edgecolor=colors[0],hatch='//')
    ax.bar(indices + bar_width/2, forc_file_scan_other, bar_width, bottom=forc_file_scan_tablescan, label='Other', color='white',edgecolor=colors[1], hatch='xxx')

    ax.set_xlabel('Queries',fontsize=24)
    ax.set_ylabel('Time (seconds)',fontsize=24)
    #ax.set_title('Comparison of TPCDS SF=100 ORC Query Performance')
    ax.set_xticks(indices)
    ax.set_xticklabels(query_names, ha="right")
    handles, labels = ax.get_legend_handles_labels()
    ax.tick_params(axis='both', labelsize=24)  # Change 14 to your desired font size for ORC subplot
    ax.tick_params(axis='both', labelsize=24)  # Change 14 to your desired font size for ORC subplot

    # 只选择前三个图例标签
    # 只选择前三个图例标签
    ax.legend(handles=[handles[0], handles[2], handles[1]], 
               labels=['Velox TableScan', 'Velox + FORC TableScan', 'Other Operators'], 
               fontsize=16)
    plt.tight_layout()
    plt.show()
    plt.savefig("./histogram_selected_queries.png")

# 数据
query_names = ['Q24', 'Q64', 'Q88', 'Q93']

# FileScan 数据
file_scan_data = {
    'TableScan': [8.34, 12.14, 5.2, 2.58],
    'Other': [4.10, 9.27, 1.8, 3.42]
}

# FORC FileScan 数据
forc_file_scan_data = {
    'TableScan': [2.3, 5.48, 1.8, 0.628],
    'Other': [4, 9.27, 1.82, 3.42]
}

# 颜色
colors = ['#1f77b4', '#d62728']  # 两种颜色：一种用于TableScan，一种用于Other

# 调用绘图函数
plot_query_times(query_names, file_scan_data, forc_file_scan_data, colors)
