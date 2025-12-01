import matplotlib.pyplot as plt

# 各个查询的平均执行时间（已写入代码）
labels = [
    "select id1, sum(v1) as v1 from df1 group by id1",
    "select id1, id2, sum(v1) as v1 from df1 group by id1, id2",
    "select id3, sum(v1) as v1, mean(v3) as v3 from df1 group by id3",
    "select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from df1 group by id4",
    "select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from df1 group by id6",
    "select id3, max(v1) - min(v2) as range_v1_v2 from df1 group by id3",
    "select id6, largest2_v3 from sub_query where order_v3 <= 2",
    "select id2, id4, pow(corr(v1, v2), 2) as r2 from df1 group by id2, id4",
    "select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from df1 group by id1, id2, id3, id4, id5, id6"
]

# 平均执行时间（card=1e1）
df3_vanilla_times = [4.450997014333334, 6.546234095, 0.5397461950000001, 4.192543303666667, 0.30, 0.4910382343333333, 25.365571150666668, 5.583434748333333, 7.999418653333334]
df3_velox_times = [2.065393438, 3.3912315233333334, 0.16457163666666666, 2.1028456933333337, 0.17258679166666668, 0.13161174566666667, 13.465314302666668, 3.061586107, 4.757329783666666]
df3_ck_times = [1.8881867093333333, 4.342799318666667, 0.20697860933333334, 2.1471669213333335, 0.14638703566666667, 0.19242451666666668, 6.0137363776666675, 5.010662544333333, 6.252320892666667]
df3_datafusion_times = [3.067584371666667, 4.8828332696666665, 0.3803519343333333, 3.5566186539999998, 0.294, 0.3295896166666667, 5.438362338666667, 6.003696863333334, 7.049984003333333]

# 平均执行时间（card=1e7）
df1_vanilla_times = [0.555757334, 0.8370161443333334, 3.052535304333333, 0.632058855, 2.1417651663333332, 2.6340034536666668, 5.0715483243333335, 0.6755615989999999, 7.742082267]
df1_velox_times = [0.209410709, 0.184129103, 1.6771339426666667, 0.20790936000000002, 1.110139636, 1.4726926216666667, 3.6578152653333333, 0.2855538606666667, 4.739395039999999]
df1_ck_times = [0.257059805, 0.404362917, 1.2197821723333333, 0.2549225933333333, 0.8746337196666666, 1.1029024023333334, 1.6237335146666665, 0.346329811, 6.263644112333334]
df1_datafusion_times = [0.378731564, 0.46518175133333334, 2.5419058563333334, 0.461633377, 2.2478267563333336, 2.229608629, 2.0789029993333332, 0.9263482036666666, 6.602943070333333]

# 提取 card=1e1 下的 q3, q5, q7, q10（索引 2, 4, 6, 8）
indices_1e1 = [2, 4, 5]
subset_labels_1e1 = [labels[i] for i in indices_1e1]
df3_vanilla_subset = [df3_vanilla_times[i] for i in indices_1e1]
df3_velox_subset = [df3_velox_times[i] for i in indices_1e1]
df3_ck_subset = [df3_ck_times[i] for i in indices_1e1]
df3_datafusion_subset = [df3_datafusion_times[i] for i in indices_1e1]

# 提取 card=1e7 下的 q1, q4（索引 0, 3）
indices_1e7 = [0, 3]
subset_labels_1e7 = [labels[i] for i in indices_1e7]
df1_vanilla_subset = [df1_vanilla_times[i] for i in indices_1e7]
df1_velox_subset = [df1_velox_times[i] for i in indices_1e7]
df1_ck_subset = [df1_ck_times[i] for i in indices_1e7]
df1_datafusion_subset = [df1_datafusion_times[i] for i in indices_1e7]

# 设置子图和柱状条位置
bar_width = 0.15

# 绘制 card=1e1 下的 q3, q5, q7, q10 子图
fig, ax = plt.subplots(figsize=(10, 8))

x = range(len(subset_labels_1e1))
ax.bar([i - bar_width * 1.5 for i in x], df3_vanilla_subset, width=bar_width, label='vanilla', color='#8ECFC9')
ax.bar([i - bar_width * 0.5 for i in x], df3_velox_subset, width=bar_width, label='velox', color='#FFBE7A')
ax.bar([i + bar_width * 0.5 for i in x], df3_ck_subset, width=bar_width, label='ck', color='#FA7F6F')
ax.bar([i + bar_width * 1.5 for i in x], df3_datafusion_subset, width=bar_width, label='datafusion', color='#82B0D2')

ax.set_xlabel('Query')
ax.set_title('Card = 1e1 (q3, q5, q7, q10)')
ax.set_xticks(x)
ax.set_xticklabels(subset_labels_1e1, rotation=45, ha='right')
ax.set_ylabel('Average Duration (seconds)')
ax.legend()

plt.tight_layout()
plt.savefig("subset_query_durations_card_1e1.png")
plt.show()

# 绘制 card=1e7 下的 q1, q4 子图
fig, ax = plt.subplots(figsize=(10, 8))

x = range(len(subset_labels_1e7))
ax.bar([i - bar_width * 1.5 for i in x], df1_vanilla_subset, width=bar_width, label='vanilla', color='#8ECFC9')
ax.bar([i - bar_width * 0.5 for i in x], df1_velox_subset, width=bar_width, label='velox', color='#FFBE7A')
ax.bar([i + bar_width * 0.5 for i in x], df1_ck_subset, width=bar_width, label='ck', color='#FA7F6F')
ax.bar([i + bar_width * 1.5 for i in x], df1_datafusion_subset, width=bar_width, label='datafusion', color='#82B0D2')

ax.set_xlabel('Query')
ax.set_title('Card = 1e7 (q1, q4)')
ax.set_xticks(x)
ax.set_xticklabels(subset_labels_1e7, rotation=45, ha='right')
ax.set_ylabel('Average Duration (seconds)')
ax.legend()

plt.tight_layout()
plt.savefig("subset_query_durations_card_1e7.png")
plt.show()

