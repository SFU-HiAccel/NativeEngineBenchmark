import os
import matplotlib.pyplot as plt
import numpy as np

def parse_operator_times(file_content):
    operator_times = {}
    for line in file_content.splitlines():
        if line.strip():  # skip empty lines
            operator, time = line.split(":")
            operator = operator.strip()
            time_in_seconds = float(time.strip().split()[0])
            operator_times[operator] = time_in_seconds
    return operator_times

def plot_operator_times(directory, colors):
    files = [f for f in os.listdir(directory) if f.endswith('.txt')]
    operator_labels = set()
    query_names = []
    operator_time_data = []

    for file in files:
        file_name, _ = os.path.splitext(file)  # 去掉 .txt 扩展名
        with open(os.path.join(directory, file), 'r') as f:
            content = f.read()
            operator_times = parse_operator_times(content)
            operator_labels.update(operator_times.keys())
            query_names.append(file_name)
            operator_time_data.append(operator_times)

    operator_labels = sorted(operator_labels)
    num_queries = len(query_names)
    bar_width = 0.7
    indices = np.arange(num_queries)

    fig, ax = plt.subplots(figsize=(12, 8))
    bottom = np.zeros(num_queries)

    for i, operator in enumerate(operator_labels):
        times = [op_times.get(operator, 0) for op_times in operator_time_data]
        color = colors[i % len(colors)]  # Use colors cyclically if there are more operators than colors
        ax.bar(indices, times, bar_width, bottom=bottom, label=operator, color=color)
        bottom += times

    ax.set_xlabel('Queries')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Operator Time Distribution of TPCDS ORC SF=1T with Velox Native Execution')
    ax.set_xticks(indices)
    ax.set_xticklabels(query_names, rotation=45, ha="right")
    ax.legend(title='Operators')

    plt.tight_layout()
    plt.show()
    plt.savefig("./histogram_velox_withexchange.png")

# Usage
directory = './'  # Replace with your directory path
colors = ['#82B0D2', '#BEB8DC', '#E7DAD2', '#8983BF', '#8ECFC9','#D0E8E0', '#FFBE7A', '#FA7F6F','#BB9727']  # Replace with your desired colors

plot_operator_times(directory, colors)

