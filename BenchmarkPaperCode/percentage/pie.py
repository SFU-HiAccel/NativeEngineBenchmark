import matplotlib.pyplot as plt

# Set the global font to Times New Roman
plt.rcParams['font.family'] = 'Times New Roman'

# Reordering data to place TableScan sections together
operations = ['TableScan:Decompress', 'TableScan:Decode', 'TableScan:IO', 'HashJoin', 'HashAggregation', 'Project', 'Other']
cpu_times = [22.3, 16.5, 2.7, 35.6, 17.5, 3.7, 1.7]

# Setup for exploding the TableScan sections as a group
explode = [0.15, 0.15, 0.15, 0, 0, 0, 0]  # Explode the TableScan sections

# Colors for the segments - Making TableScan sections a cohesive red-orange palette
colors = ['#ff5252', '#ff7043', '#ffab40', '#3888c0', '#c5e9c7', '#f7c0c5', '#d1e2f2']

# Plot
fig, ax = plt.subplots(figsize=(12, 8))

# Create pie chart with TableScan sections grouped together and exploded
wedges, texts, autotexts = ax.pie(
    cpu_times,
    labels=operations,
    autopct='%1.1f%%',
    startangle=70,  # Adjusted to better position the TableScan sections
    colors=colors,
    wedgeprops={'edgecolor': 'white', 'linewidth': 1},
    textprops={'fontsize': 14},
    explode=explode,  # Use the explode array defined earlier
    counterclock=False  # Use clockwise direction to ensure sections are together
)

# Adjust label font style and size
for text in texts:
    text.set_fontsize(14)
    
# Adjust percentage font style and size
for autotext in autotexts:
    autotext.set_fontsize(12)
    autotext.set_fontweight('bold')
    autotext.set_color('black')

# Add title with emphasis on TableScan operations
#plt.title('CPU Time Distribution by Operation\n(TableScan Operations Highlighted)', fontsize=20, pad=20)

# Add legend with custom position
ax.legend(wedges, operations, title="Operations", 
          loc="center left", 
          bbox_to_anchor=(1, 0, 0.5, 1), 
          fontsize=14, 
          title_fontsize=16)

# Adjust layout to prevent legend cutoff
plt.tight_layout()

# Save the chart before displaying
plt.savefig("pieWithGroupedTableScan.png", bbox_inches='tight', dpi=300)

# Display the chart
plt.show()