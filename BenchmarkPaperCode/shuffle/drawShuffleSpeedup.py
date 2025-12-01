import pandas as pd
import matplotlib.pyplot as plt

# Input data
data = {
    "Query": ["Q4", "Q23a", "Q24a", "Q50", "Q64", "Q67", "Q93", "Q95"],
    "Baseline_Time": [268.37, 180.34, 188.11, 81.42, 125.68, 421.73, 134.1, 79.15],
    "Baseline_MRPS": [169.0731, 43.28, 54.55, 36.639, 37.73, 80.55, 46.9, 18.837],
    "Native_Time": [39.9, 65.44, 30.84, 19.58, 31.86, 231, 25.25, 60],
    "Native_MRPS": [25.1,25.5, 3.084, 5.874, 2.13, 44.2, 8.837, 13.8]
}

# Create DataFrame
df = pd.DataFrame(data)

# Calculate the shuffle acceleration's proportion in the total acceleration
df['Shuffle_Proportion_In_Total'] = (df['Baseline_MRPS'] - df['Native_MRPS']) / (df['Baseline_Time'] - df['Native_Time'])

# Adjusted speedup calculation for plotting
df['Speedup'] = df['Baseline_Time'] / df['Native_Time']
df['Remaining_Acceleration'] = df['Speedup'] - 1  # Total acceleration minus baseline

# Plotting
fig, ax = plt.subplots(figsize=(10, 6))
ax.axhline(y=1, color='red', linestyle=':', alpha=1)

baseline_bar = ax.bar(df['Query'], [1] * len(df), label="Baseline",hatch='---',color='white',edgecolor='#2ca02c')
shuffle_bar = ax.bar(
    df['Query'],
    df['Remaining_Acceleration'] * df['Shuffle_Proportion_In_Total'],
    bottom=1,color='white',
    label="Shuffle Optimization",
    hatch='///',edgecolor='#ff7f0e'
)
remaining_bar = ax.bar(
    df['Query'],
    df['Remaining_Acceleration'] * (1 - df['Shuffle_Proportion_In_Total']),
    bottom=1 + df['Remaining_Acceleration'] * df['Shuffle_Proportion_In_Total'],color='white',
    label="Other Optimization",hatch='xxx',edgecolor='#1f77b4'
)

ax.set_ylabel("Speedup")
ax.set_title("Contribution of Shuffle Acceleration in Total Speedup")
ax.legend()

# Add annotations
#for bar, accel in zip(shuffle_bar, df['Shuffle_Proportion_In_Total']):
#    height = bar.get_height() + 1
#    ax.text(bar.get_x() + bar.get_width() / 2, height, f"{accel:.2f}", ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.show()
plt.savefig("./speedup.png")