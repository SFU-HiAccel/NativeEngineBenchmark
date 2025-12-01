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

# Average数据
avg_data = {
    "Vanilla Spark": [0.313, 0.405, 0.528, 0.5538, 1.059, 1.711, 2.0],
    "Velox": [0.315, 0.325, 0.3718, 0.3741, 0.422, 0.842, 0.937],
    "ClickHouse": [0.2935, 0.354, 0.378, 0.3837, 0.5618, 0.9389, 1.6671],
    "DataFusion": [0.289, 0.39, 0.396, 0.454, 0.943, 1.203, 1.464]
}

queries = ["1e-4", "1e-3", "1e-2", "1e-1", "1", "10", "100"]

# 创建包含三个子图的图表
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(27, 8))

# 绘制Count子图
lines1 = ax1.plot(queries, count_data["Vanilla Spark"], marker='v', color='#354747', linewidth=3, markersize=15)
lines2 = ax1.plot(queries, count_data["Velox"], marker='o', color='#02b0a8', linewidth=3, markersize=15)
lines3 = ax1.plot(queries, count_data["ClickHouse"], marker='^', color='#ff5d67', linewidth=3, markersize=15)
lines4 = ax1.plot(queries, count_data["DataFusion"], marker='s', color='#f0c917', linewidth=3, markersize=15)

ax1.set_xlabel("Cardinality of group by key (%)", fontsize=32)
ax1.set_ylabel("Time (s)", fontsize=32)
ax1.grid(True, axis='y')

# 绘制Sum子图
ax2.plot(queries, sum_data["Vanilla Spark"], marker='v', color='#354747', linewidth=3, markersize=15)
ax2.plot(queries, sum_data["Velox"], marker='o', color='#02b0a8', linewidth=3, markersize=15)
ax2.plot(queries, sum_data["ClickHouse"], marker='^', color='#ff5d67', linewidth=3, markersize=15)
ax2.plot(queries, sum_data["DataFusion"], marker='s', color='#f0c917', linewidth=3, markersize=15)

ax2.set_xlabel("Cardinality of group by key (%)", fontsize=32)
ax2.set_ylabel("Time (s)", fontsize=32)
ax2.grid(True, axis='y')

# 绘制Average子图
ax3.plot(queries, avg_data["Vanilla Spark"], marker='v', color='#354747', linewidth=3, markersize=15)
ax3.plot(queries, avg_data["Velox"], marker='o', color='#02b0a8', linewidth=3, markersize=15)
ax3.plot(queries, avg_data["ClickHouse"], marker='^', color='#ff5d67', linewidth=3, markersize=15)
ax3.plot(queries, avg_data["DataFusion"], marker='s', color='#f0c917', linewidth=3, markersize=15)

ax3.set_xlabel("Cardinality of group by key (%)", fontsize=32)
ax3.set_ylabel("Time (s)", fontsize=32)
ax3.grid(True, axis='y')

# 设置所有子图的刻度标签大小
ax1.tick_params(axis='both', labelsize=32)
ax2.tick_params(axis='both', labelsize=32)
ax3.tick_params(axis='both', labelsize=32)

# Add shared legend
legend_labels = ["Vanilla Spark", "Spark+Velox", "Spark+ClickHouse", "Spark+DataFusion"]
fig.legend([lines1[0], lines2[0], lines3[0], lines4[0]],
           labels=legend_labels,
           fontsize=38,
           bbox_to_anchor=(0.52, 1.02),
           loc='lower center',
           ncol=4)

# 调整布局
plt.tight_layout()
plt.subplots_adjust(top=0.9)

# 保存和显示图表
plt.savefig("./agg_comparison_three_plots.pdf", bbox_inches='tight', dpi=350)
plt.show()
