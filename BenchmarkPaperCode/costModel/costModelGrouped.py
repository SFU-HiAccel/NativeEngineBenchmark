import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.patches as mpatches

# Sample data
data = {
    'Query Number': ['Q13', 'Q16', 'Q17', 'Q25', 'Q29', 'Q64', 'Q72', 'Q85', 'Q95'],
    'Native Join Time (Absolute)': [10.12421, 8.987278592, 1.586, 2.03,
     7.9, 16.67, 
    77.3, 2.902857143, 59.2],
    'Native Non Join Time (Absolute)': [7.12, 5.54, 3.318390646, 2.29, 
    5.83, 15.0, 
    6.7, 2.177142857, 13.2],
    'Cost Model w/o optimization Data Conversion Absolute Time': [0.70597222, 1.0, 0, 0, 
    0, 2.004705882, 
    3.23, 0.011137931, 0.133],
    'Cost Model w/o optimization Native Execution Absolute Time': [1.151221, 2.32, 1.586, 2.2323,
    7.90, 5.21, 
    5.2, 2.673103448, 59.2],
    'Cost Model w/o optimization Codegen Absolute Time': [3.964305556, 7.41, 0, 0, 
    0, 10.53,
    27.3, 0, 0],
    'Cost Model w/o optimization Other Absolute Time': [7.564763889, 5.36, 3.48390646, 2.57,
     5.934309357, 15.15, 
    6.8, 2.167, 13.4],
    'Cost Model with optimization Data Conversion Absolute Time': [0.1555554, 0.70997439, 0, 0, 
    0.245059372, 0, 
    1.84, 0, 2.63803945],
    'Cost Model with optimization Native Execution Absolute Time': [0, 0, 1.586, 2.02, 0,
    16.8, 0, 0, 0],
    'Cost Model with optimization Codegen Absolute Time': [3.964, 8.43, 0.24, 0, 
    5.01, 0, 
    31.1, 1.31, 28.24],
    'Cost Model with optimization Other Absolute Time': [7.312, 5.5325, 3.318390646, 2.58, 
    5.98, 14.9, 
    6.7, 2.105068966, 13.34],
    'Vanilla Spark Codegen operator (Absolute)': [3.964, 8.653858137, 3.095939086, 2.231913876,
     5.15, 41.4,
    31.0, 1.282139037, 28.34],
    'Vanilla Spark Other operator (Absolute)': [19.50753767, 16.14614186, 7.604060914, 4.338086124, 
    12.94, 93.9-41.4,
    20.7, 5.197860963, 28.91],
}

# Create DataFrame
df = pd.DataFrame(data)

# Reorder rows with Q17, Q25, Q64 at the beginning
query_order = ['Q17', 'Q25', 'Q64', 'Q13', 'Q16', 'Q29', 'Q72', 'Q85', 'Q95']
df = df.set_index('Query Number').loc[query_order].reset_index()

# Create figure with extra space at the top for legends
fig = plt.figure(figsize=(28, 20))
gs = fig.add_gridspec(2, 1, height_ratios=[0.5, 6], hspace=0.05)

# Create subplots
ax_legend = fig.add_subplot(gs[0])
ax = fig.add_subplot(gs[1])

x = np.arange(len(df['Query Number']))
width = 0.2

# Colors and patterns for combined components
combined_colors = {
    'Data Conversion': '#1f77b4',
    'Native Execution Join': '#2ca02c',
    'Codegen Join': '#ff7f0e',
    'Native Other': '#d62728',
    'Codegen Other': '#9467bd'
}
hatches = ['///', 'ooo', '---', 'xxx']

# Cost Model with Optimization (Stacked)
cost_with_opt_dc = df['Cost Model with optimization Data Conversion Absolute Time']
cost_with_opt_ne = df['Cost Model with optimization Native Execution Absolute Time']
cost_with_opt_codegen = df['Cost Model with optimization Codegen Absolute Time']
cost_with_opt_other = df['Cost Model with optimization Other Absolute Time']
ax.bar(x - width * 1.5, cost_with_opt_dc, width, label='Data Conversion', 
       color='white', edgecolor=combined_colors['Data Conversion'], hatch=hatches[0])
ax.bar(x - width * 1.5, cost_with_opt_ne, width, bottom=cost_with_opt_dc, 
       color='white', edgecolor=combined_colors['Native Execution Join'], hatch=hatches[1])
ax.bar(x - width * 1.5, cost_with_opt_codegen, width, bottom=cost_with_opt_dc + cost_with_opt_ne, 
       color='white', edgecolor=combined_colors['Codegen Join'], hatch=hatches[2])
ax.bar(x - width * 1.5, cost_with_opt_other, width, bottom=cost_with_opt_dc + cost_with_opt_ne + cost_with_opt_codegen, 
       color='white', edgecolor=combined_colors['Native Other'], hatch=hatches[3])

# Cost Model w/o Optimization (Stacked)
cost_wo_opt_dc = df['Cost Model w/o optimization Data Conversion Absolute Time']
cost_wo_opt_ne = df['Cost Model w/o optimization Native Execution Absolute Time']
cost_wo_opt_codegen = df['Cost Model w/o optimization Codegen Absolute Time']
cost_wo_opt_other = df['Cost Model w/o optimization Other Absolute Time']
ax.bar(x - width / 2, cost_wo_opt_dc, width, 
       color='white', edgecolor=combined_colors['Data Conversion'], hatch=hatches[0])
ax.bar(x - width / 2, cost_wo_opt_ne, width, bottom=cost_wo_opt_dc, 
       color='white', edgecolor=combined_colors['Native Execution Join'], hatch=hatches[1])
ax.bar(x - width / 2, cost_wo_opt_codegen, width, bottom=cost_wo_opt_dc + cost_wo_opt_ne, 
       color='white', edgecolor=combined_colors['Codegen Join'], hatch=hatches[2])
ax.bar(x - width / 2, cost_wo_opt_other, width, bottom=cost_wo_opt_dc + cost_wo_opt_ne + cost_wo_opt_codegen, 
       color='white', edgecolor=combined_colors['Native Other'], hatch=hatches[3])

# Native Execution (Stacked: Native Join + Native Non Join)
native_join = df['Native Join Time (Absolute)']
native_non_join = df['Native Non Join Time (Absolute)']
ax.bar(x + width / 2, native_join, width, 
       color='white', edgecolor=combined_colors['Native Execution Join'], hatch=hatches[1])
ax.bar(x + width / 2, native_non_join, width, bottom=native_join, 
       color='white', edgecolor=combined_colors['Native Other'], hatch=hatches[3])

# Vanilla Spark (Stacked: Codegen and Codegen Other)
vanilla_codegen = df['Vanilla Spark Codegen operator (Absolute)']
vanilla_other = df['Vanilla Spark Other operator (Absolute)']
ax.bar(x + width * 1.5, vanilla_codegen, width, 
       color='white', edgecolor=combined_colors['Codegen Join'], hatch=hatches[2])
ax.bar(x + width * 1.5, vanilla_other, width, bottom=vanilla_codegen, 
       color='white', edgecolor=combined_colors['Codegen Other'], hatch=hatches[3])

# Creating custom legend handles
legend_handles = [
    mpatches.Patch(facecolor='white', edgecolor=combined_colors['Data Conversion'], 
                   linewidth=2, hatch=hatches[0], label='Data Conversion'),
    mpatches.Patch(facecolor='white', edgecolor=combined_colors['Native Execution Join'], 
                   linewidth=2, hatch=hatches[1], label='Native Vectorized Join'),
    mpatches.Patch(facecolor='white', edgecolor=combined_colors['Codegen Join'], 
                   linewidth=2, hatch=hatches[2], label='Codegen Join'),
    mpatches.Patch(facecolor='white', edgecolor=combined_colors['Native Other'], 
                   linewidth=2, hatch=hatches[3], label='Native Other'),
    mpatches.Patch(facecolor='white', edgecolor=combined_colors['Codegen Other'], 
                   linewidth=2, hatch=hatches[3], label='Codegen Other')
]

# Create the component type legend in the top subplot
legend1 = ax_legend.legend(handles=legend_handles, 
                          loc='lower center',
                          ncol=3,
                          frameon=True,
                          fontsize=40)

# Hide the legend axes
ax_legend.axis('off')

# Configure main plot
ax.set_xlabel('Queries', fontsize=40)
ax.set_ylabel('Execution Time (s)', fontsize=40)
ax.set_xticks(x)
ax.set_xticklabels(df['Query Number'])
ax.grid(axis='y')
ax.tick_params(axis='both', labelsize=40)

# Adjust layout
plt.tight_layout()

# Show and save
plt.show()
plt.savefig("./stacked_updated.png", bbox_inches='tight', dpi=300)
