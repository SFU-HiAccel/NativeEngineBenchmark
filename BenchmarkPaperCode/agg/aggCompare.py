import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Helvetica']

# Count数据
count_data = {
    "Velox": [0.271392416, 0.231542992, 0.233941790, 0.335841069, 0.454718647, 0.515882849, 0.810585041],
    "ClickHouse": [0.466827268, 0.440048082, 0.540385998, 0.559965499, 0.823688917, 1.116005204, 2.161042574],
    "DataFusion": [0.537566442, 0.536595676, 0.547191619, 0.625693904, 1.072969209, 1.190704451, 1.486776731],
    "Vanilla Spark": [0.509274084, 0.649721116, 0.718738175, 1.180708772, 1.611946998, 2.707990662, 4.283880505]
}

# Sum数据
sum_data = {
    "Velox": [0.267629574, 0.232668607, 0.23918867133333332, 0.317376085, 0.45087129266666665, 0.5627934246666667, 0.7657087320000001],
    "ClickHouse": [0.559849431, 0.532196726, 0.633537657, 0.72, 1.243185299, 1.283407522, 2.3255421253333335],
    "DataFusion": [0.6495564056666666, 0.723977573, 0.678159333, 0.6951514206666666, 1.2677986953333333, 1.3528281603333336, 1.7010644163333335],
    "Vanilla Spark": [1.163866127, 1.269925421, 1.289504131, 1.504647821, 3.74882093, 4.356411615, 5.429830656],
}

queries = ["1e-4", "1e-3", "1e-2", "1e-1", "1", "10", "100"]

# 创建包含两个子图的图表
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

# 绘制Count子图
#colors = ["#354747", "#02b0a8","#ff5d67", "#f0c917"]
#markers = ['v', 'o', '^', 's']


lines1 = ax1.plot(queries, count_data["Vanilla Spark"], marker='v', color='#354747', linewidth=3,markersize=15)
lines2 = ax1.plot(queries, count_data["Velox"], marker='o', color='#02b0a8', linewidth=3,markersize=15)
lines3 = ax1.plot(queries, count_data["ClickHouse"], marker='^', color='#ff5d67', linewidth=3,markersize=15)
lines4 = ax1.plot(queries, count_data["DataFusion"], marker='s', color='#f0c917', linewidth=3,markersize=15)

ax1.set_xlabel("Cardinality of group by key (%)", fontsize=32)
ax1.set_ylabel("Time (s)", fontsize=32)
ax1.grid(True,axis='y')

# 绘制Sum子图
ax2.plot(queries, sum_data["Vanilla Spark"], marker='v', color='#354747', linewidth=3,markersize=15)
ax2.plot(queries, sum_data["Velox"], marker='o', color='#02b0a8', linewidth=3,markersize=15)
ax2.plot(queries, sum_data["ClickHouse"], marker='^', color='#ff5d67', linewidth=3,markersize=15)
ax2.plot(queries, sum_data["DataFusion"], marker='s', color='#f0c917', linewidth=3,markersize=15)

ax2.set_xlabel("Cardinality of group by key (%)", fontsize=32)
ax2.set_ylabel("Time (s)", fontsize=32)
ax2.grid(True,axis='y') 

ax1.tick_params(axis='both', labelsize=32)  # Change 14 to your desired font size for ORC subplot
ax2.tick_params(axis='both', labelsize=32)  # Change 14 to your desired font size for Parquet subplot


# Add shared legend
legend_labels = ["Vanilla Spark", "Spark+Velox", "Spark+ClickHouse", "Spark+DataFusion"]
fig.legend([lines1[0], lines2[0], lines3[0], lines4[0]], 
          labels=legend_labels,
          fontsize=24,
          bbox_to_anchor=(0.52, 1.02), 
          loc='lower center', 
          ncol=4)

# 调整布局
plt.tight_layout()
plt.subplots_adjust(top=1)

# 保存和显示图表
plt.savefig("./agg_comparison.pdf", bbox_inches='tight',dpi=350)
plt.show()