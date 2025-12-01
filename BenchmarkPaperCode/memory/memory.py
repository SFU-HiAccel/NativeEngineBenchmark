# First figure - Combined Metrics
import matplotlib.pyplot as plt

# Define data 
execution_types = ['Vanilla', 'Velox']
colors = ['#e14932', '#348abd']

# Data for each subplot
network_io = [45.0, 94.1]
cpu_util = [93.8, 83.3]
disk_io = [15.2, 37.3]

# Create 1 row 3 columns of subplots with consistent size
fig1, axes = plt.subplots(1, 3, figsize=(15, 6))
plt.subplots_adjust(wspace=0.3)

# Subplot 1: CPU Util
axes[0].bar(execution_types, cpu_util, color=colors, width=0.5)
axes[0].set_ylabel('CPU Util (%)', fontsize=24)
axes[0].tick_params(axis='y', labelsize=24)
axes[0].tick_params(axis='x', labelsize=24)
axes[0].set_ylim(0, 110)
axes[0].grid(axis='y', color='gray', linestyle='-', alpha=0.2)

# Add value labels
for i, value in enumerate(cpu_util):
    axes[0].annotate(f'{value:.1f}',
                     xy=(i, value),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom',
                     fontsize=20)

# Subplot 2: Network IO Util
axes[1].bar(execution_types, network_io, color=colors, width=0.5)
axes[1].set_ylabel('Network IO Util (%)', fontsize=24)
axes[1].tick_params(axis='y', labelsize=24)
axes[1].tick_params(axis='x', labelsize=24)
axes[1].set_ylim(0, 110)
axes[1].grid(axis='y', color='gray', linestyle='-', alpha=0.2)

# Add value labels
for i, value in enumerate(network_io):
    axes[1].annotate(f'{value:.1f}',
                     xy=(i, value),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom',
                     fontsize=20)

# Subplot 3: Disk IO Util
axes[2].bar(execution_types, disk_io, color=colors, width=0.5)
axes[2].set_ylabel('Disk IO Util (%)', fontsize=24)
axes[2].tick_params(axis='y', labelsize=24)
axes[2].tick_params(axis='x', labelsize=24)
axes[2].set_ylim(0, 50)
axes[2].grid(axis='y', color='gray', linestyle='-', alpha=0.2)

# Add value labels
for i, value in enumerate(disk_io):
    axes[2].annotate(f'{value:.1f}',
                     xy=(i, value),
                     xytext=(0, 3),
                     textcoords="offset points",
                     ha='center', va='bottom',
                     fontsize=20)

# Create legend for the entire figure
handles = [plt.Rectangle((0,0),1,1, color=colors[i]) for i in range(len(colors))]
fig1.legend(handles, execution_types,
           loc='upper center',
           ncol=2,
           fontsize=24,
           bbox_to_anchor=(0.5, 1.05))  # Moved legend higher up

# Adjust subplot positions
plt.tight_layout(rect=[0, 0, 1, 0.95])  # Increased top margin for legend

plt.savefig("./combined_metrics.pdf", bbox_inches='tight', dpi=300)
plt.show()

# Second figure - Memory Usage
import matplotlib.pyplot as plt
import numpy as np

# Categories (x-axis)
categories = ['Q4', 'Q23a', 'Q24a', 'Q50', 'Q64', 'Q67', 'Q93', 'Q72', 'Q88', 'Q95']

# Values
nonnative_values = [162, 215, 183, 173, 70, 120, 187.5, 175, 172, 164]  # Vanilla (red)
native_values = [110, 106, 122, 222, 40, 112, 111.5, 162, 120, 137]  # Velox (blue)

# Set up figure with the same dimensions as the first figure
fig2, ax = plt.subplots(figsize=(15, 6))

# Bar settings
bar_width = 0.35
indices = np.arange(len(categories))

# Create bars
bar1 = ax.bar(indices - bar_width/2, nonnative_values, bar_width, color='#e14932', label='Vanilla')
bar2 = ax.bar(indices + bar_width/2, native_values, bar_width, color='#348abd', label='Velox')

# Style settings
ax.set_xticks(indices)
ax.set_xticklabels(categories, rotation=45, ha='right')
ax.set_ylim(0, 250)

# Remove unnecessary spines
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)

# Labels
ax.set_xlabel('Query Number', fontsize=24)
ax.set_ylabel('Average Memory Usage (GB)', fontsize=24)
plt.yticks(fontsize=24)
plt.xticks(fontsize=24)

# Add horizontal grid
ax.yaxis.grid(True, linestyle='--', alpha=0.7)

# Legend with same position as first figure
handles = [plt.Rectangle((0,0),1,1, color=c) for c in ['#e14932', '#348abd']]
fig2.legend(handles, ['Vanilla', 'Velox'],
           loc='upper center',
           ncol=2,
           fontsize=24,
           bbox_to_anchor=(0.5, 0.98))  # Same position as first figure

# Adjust layout - same parameters as first figure
plt.tight_layout(rect=[0, 0, 1, 0.92])  # Same rect parameter as first figure

plt.savefig("memoryUsage.pdf", dpi=300, bbox_inches='tight')
plt.show()