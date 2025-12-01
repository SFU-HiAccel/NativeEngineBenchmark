import matplotlib.pyplot as plt  

# 定义数据
execution_types = ['Vanilla', 'Velox']
colors = ['#e14932', '#348abd']  

# 各子图的数据 
network_io = [45.0, 94.1]  # 转换为百分比 
cpu_util = [93.8, 83.3] 
disk_io = [15.2, 37.3]  

# 创建 1 行 3 列的子图，增加宽度和子图间距 
fig, axes = plt.subplots(1, 3, figsize=(12, 6)) 
plt.subplots_adjust(wspace=2)  # 减少子图之间的间距以腾出更多空间  

# 子图 1：CPU Util 
axes[0].bar(execution_types, cpu_util, color=colors, width=0.5) 
axes[0].set_ylabel('CPU Util (%)', fontsize=24) 
axes[0].tick_params(axis='y', labelsize=24) 
axes[0].tick_params(axis='x', labelsize=24)
axes[0].set_ylim(0, 110)  # 增加y轴上限，为注释文本留出空间
axes[0].grid(axis='y', color='gray', linestyle='-', alpha=0.2)  

# 为第一个子图添加数值标签 
for i, value in enumerate(cpu_util):     
    axes[0].annotate(f'{value:.1f}',                     
                    xy=(i, value),                     
                    xytext=(0, 3),                     
                    textcoords="offset points",                     
                    ha='center', va='bottom',                     
                    fontsize=20)  

# 子图 2：Network IO Util 
axes[1].bar(execution_types, network_io, color=colors, width=0.5) 
axes[1].set_ylabel('Network IO Util (%)', fontsize=24) 
axes[1].tick_params(axis='y', labelsize=24) 
axes[1].tick_params(axis='x', labelsize=24)
axes[1].set_ylim(0, 110)  # 增加y轴上限
axes[1].grid(axis='y', color='gray', linestyle='-', alpha=0.2)  

# 为第二个子图添加数值标签 
for i, value in enumerate(network_io):     
    axes[1].annotate(f'{value:.1f}',                     
                    xy=(i, value),                     
                    xytext=(0, 3),                     
                    textcoords="offset points",                     
                    ha='center', va='bottom',                     
                    fontsize=20)  

# 子图 3：Disk IO Util 
axes[2].bar(execution_types, disk_io, color=colors, width=0.5) 
axes[2].set_ylabel('Disk IO Util (%)', fontsize=24) 
axes[2].tick_params(axis='y', labelsize=24) 
axes[2].tick_params(axis='x', labelsize=24)
axes[2].set_ylim(0, 50)  # 为第三个子图设置较小的上限，因为数值较小
axes[2].grid(axis='y', color='gray', linestyle='-', alpha=0.2)  

# 为第三个子图添加数值标签 
for i, value in enumerate(disk_io):     
    axes[2].annotate(f'{value:.1f}',                     
                    xy=(i, value),                     
                    xytext=(0, 3),                     
                    textcoords="offset points",                     
                    ha='center', va='bottom',                     
                    fontsize=20)  

# 为整个图添加图例，放在图的上方外部 
handles = [plt.Rectangle((0,0),1,1, color=colors[i]) for i in range(len(colors))] 
fig.legend(handles, execution_types,             
          loc='upper center',             
          ncol=2,             
          fontsize=24)  

# 调整子图的位置以避免y轴标签重叠 
plt.tight_layout(rect=[0, 0, 1, 0.90])  # 调整rect参数，为图例腾出更多空间  

plt.savefig("./combined_metrics.pdf", bbox_inches='tight', dpi=300) 
plt.show()