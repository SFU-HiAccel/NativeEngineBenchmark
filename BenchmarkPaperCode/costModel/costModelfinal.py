import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.patches as mpatches
import matplotlib as mpl

# Set global hatch linewidth (default is 1.0)
mpl.rcParams['hatch.linewidth'] = 3.0  # Increase the thickness of hatch lines
# Sample data
data = {
    'Query Number': ['Q13', 'Q16', 'Q17', 'Q25', 'Q29', 'Q64', 'Q72', 'Q85', 'Q95'],
    'Native Join Time (Absolute)': [10.12421, 8.987278592, 1.586, 2.03,
     7.9, 16.67, 
    77.3, 2.902857143, 59.2],
    'Native Non Join Time (Absolute)': [7.12, 5.54, 3.318390646, 2.29, 
    5.83, 15.0, 
    6.7, 2.177142857, 13.2],
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
    12.94, 52.5,
    20.7, 5.197860963, 28.91],
}

# Create DataFrame
df = pd.DataFrame(data)

# Reorder rows with Q17, Q25, Q64 at the beginning
query_order = ['Q13', 'Q16', 'Q17', 'Q25', 'Q29', 'Q64', 'Q72', 'Q85', 'Q95']
df = df.set_index('Query Number').loc[query_order].reset_index()

# Create figure with extra space at the top for legends
fig = plt.figure(figsize=(30, 23))
gs = fig.add_gridspec(2, 1, height_ratios=[0.5, 6], hspace=0.05)

# Create subplots
ax_legend = fig.add_subplot(gs[0])
ax = fig.add_subplot(gs[1])

x = np.arange(len(df['Query Number']))
width = 0.25

# Colors and patterns for combined components
combined_colors = {
    'CostModelNonJoin': '#ec7063',
    'CostModelVectorizedJoin': '#FADBD8',
    'CostModelCodegenJoin': '#fea443',
    'NativeJoin': '#d4e6f1', #'#d4e6f1',
    'NativeNonJoin': '#5599c7',
    'CodegenJoin': '#A3D9CE',
    'CodegenNonJoin': '#8ECFC9',

    'Data Conversion': '#f0c284',
}
hatches = ['///', 'ooo', '---', 'xxx']

# Cost Model with Optimization (Stacked)
cost_with_opt_dc = df['Cost Model with optimization Data Conversion Absolute Time']
cost_with_opt_ne = df['Cost Model with optimization Native Execution Absolute Time']
cost_with_opt_codegen = df['Cost Model with optimization Codegen Absolute Time']
cost_with_opt_other = df['Cost Model with optimization Other Absolute Time']

# Set a consistent linewidth for all borders
border_linewidth = 2.0 

ax.bar(x - width / 2, cost_with_opt_ne, width, label='Native Execution', 
       facecolor=combined_colors['CostModelNonJoin'], edgecolor='black', 
       hatch='-', linewidth=border_linewidth)

ax.bar(x - width / 2, cost_with_opt_codegen, width, bottom=cost_with_opt_ne, 
       facecolor='combined_colors['CostModelNonJoin']', edgecolor='black', 
       hatch='/', linewidth=border_linewidth)

ax.bar(x - width / 2, cost_with_opt_dc, width, bottom=cost_with_opt_ne + cost_with_opt_codegen, 
       facecolor=combined_colors['CostModelNonJoin'], edgecolor='black', 
       hatch='o', linewidth=border_linewidth)

ax.bar(x - width / 2, cost_with_opt_other, width, bottom=cost_with_opt_ne + cost_with_opt_codegen + cost_with_opt_dc, 
       facecolor='white', edgecolor='black', hatch='x', linewidth=border_linewidth)

# Native Execution (Stacked: Native Join + Native Non Join)
native_join = df['Native Join Time (Absolute)']
native_non_join = df['Native Non Join Time (Absolute)']
ax.bar(x + width / 2, native_join, width, 
       color=combined_colors['NativeNonJoin'], hatch='-', edgecolor='black', facecolor=combined_colors['NativeNonJoin'], linewidth=border_linewidth
       )
ax.bar(x + width / 2, native_non_join, width, bottom=native_join, 
       facecolor='white', color=combined_colors['NativeNonJoin'], hatch='x', edgecolor='black', linewidth=border_linewidth)


# Vanilla Spark (Stacked: Codegen and Codegen Other)
vanilla_codegen = df['Vanilla Spark Codegen operator (Absolute)']
vanilla_other = df['Vanilla Spark Other operator (Absolute)']
ax.bar(x + width * 1.5, vanilla_codegen, width, 
       color=combined_colors['CodegenNonJoin'], hatch='/', edgecolor='black', facecolor=combined_colors['CodegenNonJoin'], linewidth=border_linewidth
       )

ax.bar(x + width * 1.5, vanilla_other, width, bottom=vanilla_codegen, 
       facecolor='white', color=combined_colors['CodegenNonJoin'], hatch='x', edgecolor='black', linewidth=border_linewidth
       )


legend_labels = {
    'Native Join': combined_colors['NativeJoin'],
    'Native Others': combined_colors['NativeNonJoin'],
    'Codegen Join': combined_colors['CodegenJoin'],
    'Codegen Others': combined_colors['CodegenNonJoin'],
    'Cost Model Others': combined_colors['CostModelNonJoin'],
    'Cost Model Vectorized Join': combined_colors['CostModelVectorizedJoin'],
    'Cost Model Codegen Join': combined_colors['CostModelCodegenJoin'],
    'Data Conversion': combined_colors['Data Conversion']
}

# Create legend patches
# Create legend patches for colors
legend_patches_colors = [
    mpatches.Patch(color=combined_colors['CostModelNonJoin'], label='Our Optimized Model'),
    mpatches.Patch(color=combined_colors['NativeNonJoin'], label='Velox Execution'),
    mpatches.Patch(color=combined_colors['CodegenNonJoin'], label='Vanilla Spark Execution')
]

# Create legend lines for hatches (second row)
legend_patches_hatches = [
    mpatches.Patch(facecolor='none', edgecolor='black', hatch='-', label='Native Join'),
    mpatches.Patch(facecolor='none', edgecolor='black', hatch='/', label='Codegen Join'),
    mpatches.Patch(facecolor='none', edgecolor='black', hatch='o', label='Data Conversion'),
    mpatches.Patch(facecolor='none', edgecolor='black', hatch='x', label='Other Operator')
]

# Create two separate legends
legend_patches_first_row = legend_patches_colors
legend_patches_second_row = legend_patches_hatches
 
# Add the first legend (colors)
ax_legend.legend(
    handles=legend_patches_first_row, 
    loc='upper center', 
    ncol=3, 
    fontsize=40, 
    frameon=False, 
    bbox_to_anchor=(0.5, 1.5)
)

# Add the second legend (hatches)
ax_legend_second = fig.add_subplot(gs[0])  # Create an additional legend subplot
ax_legend_second.legend(
    handles=legend_patches_second_row, 
    loc='upper center', 
    ncol=4, 
    fontsize=40, 
    frameon=False, 
    bbox_to_anchor=(0.5, 0.8)
)
ax_legend.axis('off')  # Hide axes for the first legend subplot
ax_legend_second.axis('off')  # Hide axes for the second legend subplot

# Configure main plot
ax.set_xlabel('Queries', fontsize=70)
ax.set_ylabel('Execution Time (s)', fontsize=70)
ax.set_xticks(x)
ax.set_xticklabels(df['Query Number'])


ax.grid(axis='y')
# Set both x and y tick label sizes
ax.tick_params(axis='x', labelsize=50)  # X-axis tick labels
ax.tick_params(axis='y', labelsize=50)  # Y-axis tick labels

# Adjust layout and show
plt.tight_layout()
plt.savefig("./test.pdf", bbox_inches='tight', dpi=350)