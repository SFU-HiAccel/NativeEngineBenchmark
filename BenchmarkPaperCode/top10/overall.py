import matplotlib.pyplot as plt
import numpy as np

# Data
names = ['vanilla Spark', 'Velox', 'ClickHouse', 'DataFusion']
times = [1550.48, 628.58, 908.23, 946.49]
base = times[0]

# Calculating the speedup (i.e., higher values indicate better performance)
speedup = [base / time for time in times]

# Plotting the bar chart
plt.figure(figsize=(8, 6))
bars = plt.bar(names, speedup, color=['blue', 'lightblue', 'lightblue', 'lightblue'])

# Adding text annotations for each bar
for bar, performance in zip(bars, speedup):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{performance:.2f}', 
             ha='center', va='bottom')

# Title and labels
plt.title("TPC-DS Benchmarks\nSF1T Performance", fontsize=14)
plt.ylabel("Speedup (Higher is better)", fontsize=12, color='brown')

plt.show()
plt.savefig("./e2eBenchmark.png")

