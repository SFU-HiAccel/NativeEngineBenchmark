import matplotlib.pyplot as plt

# 整理后的数据 (使用平均持续时间)
data = {
    "Velox": [0.271392416, 0.231542992, 0.233941790, 0.335841069, 0.454718647, 0.515882849, 0.810585041],
    "ClickHouse": [0.466827268, 0.440048082, 0.540385998, 0.559965499, 0.823688917, 1.116005204, 2.161042574],
    "DataFusion": [0.537566442, 0.536595676, 0.547191619, 0.625693904, 1.072969209, 1.190704451, 1.486776731],
    "Vanilla Spark": [0.509274084, 0.649721116, 0.718738175, 1.180708772, 1.611946998, 2.707990662, 4.283880505]
}

# 查询标签（对应不同的数据文件 df1-df7）
queries = ["0.0001", "0.001", "0.01", "0.1", "1", "10", "100"]
colors = ["#ff7f0f", "#2da02d", "#d72828", "#2077b5"]

# 创建图表
plt.figure(figsize=(10, 6))

# 绘制每个系统的性能曲线
for i, (label, durations) in enumerate(data.items()):
    plt.plot(queries, durations, marker='o', label=label, color=colors[i % len(colors)], linewidth=2.5)

# 设置图表属性
plt.xlabel("Percentage of Cardinality of group by key (%)", fontsize=12)
plt.ylabel("Time (s)", fontsize=12)
plt.legend(fontsize=12)
plt.grid(True, which="major", linestyle="--", linewidth=0.5, axis='y')
plt.tight_layout()

# 显示图表
plt.show()
plt.savefig("./drawCount.png")