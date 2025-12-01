import matplotlib.pyplot as plt
import numpy as np

# Join types
join_types = [
    "broadcastHashJoin",
    "broadcastHashJoinSemiJoin",
    "sortMergeJoin",
    "shuffleHashJoin",
    "broadcastNestedLoopJoin"
]

# Average execution times for each system
vanilla_spark_times = [
    0.805475775,
    0.374007230,
    2.286280104,
    1.58848292,
    0.245875788,
]

velox_times = [
    0.414373353,
    0.2127610586666667,
    0.5192754033333333,
    0.47021308533333334,
    0.18052919933333333
]

clickhouse_times = [
    0.37788015700000005,
    0.18905671400000001,
    0.4607142183333333,
    0.5944873003333334,
    0.233464728
]

datafusion_times = [
    0.487814,
    0.32,
    0.941344143,
    0.62240862,
    0.23430992400000004
]

# X axis positions
x = np.arange(len(join_types))

# Bar width
width = 0.2

# Plotting the bar chart
plt.figure(figsize=(12, 8))
plt.bar(x - 1.5*width, vanilla_spark_times, width, label='Vanilla Spark', color='#8ECFC9')
plt.bar(x - 0.5*width, velox_times, width, label='Velox', color='#FFBE7A')
plt.bar(x + 0.5*width, clickhouse_times, width, label='ClickHouse', color='#82B0D2')
plt.bar(x + 1.5*width, datafusion_times, width, label='DataFusion', color='#FA7F6F')

# Adding labels and title
plt.xlabel('Join Types')
plt.ylabel('Average Execution Speedup')
plt.title('Comparison of Average Execution Time for Different Join Types Across Systems')

# Adding x-ticks
plt.xticks(x, join_types, rotation=15)

# Adding legend
plt.legend()

# Display the plot
plt.show()

plt.savefig("./joinSpeedup.png")
