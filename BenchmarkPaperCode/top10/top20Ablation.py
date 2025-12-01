import os
import re
import matplotlib.pyplot as plt
from matplotlib import font_manager

def extract_execution_time(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            if len(lines) >= 3:
                try:
                    execution_time = float(lines[3].split(":")[-1].strip().split()[0])
                    return execution_time
                except ValueError:
                    print(f"Error extracting execution time from {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return None

def extract_numeric_part(query_number):
    match = re.search(r'\d+', query_number)
    return int(match.group()) if match else 0

def main():
    # Query times with the five systems you specified
    query_times = {
        'q64': {'vanilla spark 3.5': 93.987, 'Spark 3.5 + Velox': 33.366, 'vanilla spark 4.0': 62.997, 'Vanilla Presto': 160.315, "Presto + Velox": 81.303},
        'q72': {'vanilla spark 3.5': 51.712, 'Spark 3.5 + Velox': 84.336, 'vanilla spark 4.0': 48.913, 'Vanilla Presto': 62.312, "Presto + Velox": 103.29},
        'q95': {'vanilla spark 3.5': 56.756, 'Spark 3.5 + Velox': 72.561, 'vanilla spark 4.0': 56.016, 'Vanilla Presto': 74.974, "Presto + Velox": 90.125},
    }

    # Set font to a standard Type 1 font (e.g., Helvetica) to avoid Type 3 fonts in PDF
    plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['font.family'] = 'sans-serif'
    #plt.rcParams['font.sans-serif'] = ['Helvetica']

    # Convert dictionary to the format needed for plotting
    query_data = [
        (query, times['vanilla spark 3.5'], times['Spark 3.5 + Velox'], times['vanilla spark 4.0'], times['Vanilla Presto'], times['Presto + Velox'])
        for query, times in query_times.items()
    ]

    # Sort by query number for better visualization
    query_data.sort(key=lambda x: extract_numeric_part(x[0]))
    
    sorted_query_numbers = [data[0] for data in query_data]
    vanilla_spark_35_times = [data[1] for data in query_data]
    spark_35_velox_times = [data[2] for data in query_data]
    vanilla_spark_40_times = [data[3] for data in query_data]
    vanilla_presto_times = [data[4] for data in query_data]
    presto_velox_times = [data[5] for data in query_data]

    fig, ax = plt.subplots(figsize=(30, 15))
    bar_width = 0.15  # Width of the bars (adjusted for 5 bars)
    group_width = bar_width * 6  # Width of each group with spacing

    index = [i * group_width for i in range(len(sorted_query_numbers))]

    # Plot bars for execution times
    bars1 = ax.bar([i for i in index], vanilla_spark_35_times, bar_width, label='Vanilla Spark 3.5', color='#354747')
    bars2 = ax.bar([i + bar_width for i in index], vanilla_spark_40_times, bar_width, label='Vanilla Spark 4.0', color='#ff5d67')
    bars3 = ax.bar([i + 2 * bar_width for i in index], spark_35_velox_times, bar_width, label='Spark 3.5 + Velox', color='#02b0a8')
    bars4 = ax.bar([i + 3 * bar_width for i in index], vanilla_presto_times, bar_width, label='Vanilla Presto', color='#f0c917')
    bars5 = ax.bar([i + 4 * bar_width for i in index], presto_velox_times, bar_width, label='Presto + Velox', color='#9b59b6')

    # Use Helvetica font for all text elements
    fontdict_xlabel = {'fontsize': 50}
    fontdict_ylabel = {'fontsize': 40}
    fontdict_xtick = {'fontsize': 50}
    fontdict_ytick = {'fontsize': 50}
    fontdict_legend = {'fontsize': 40, 'prop': font_manager.FontProperties()}

    ax.set_xlabel('Query Number', **fontdict_xlabel)
    ax.set_ylabel('Execution Time (seconds)', **fontdict_ylabel)
    ax.set_xticks([i + 2*bar_width for i in index])  # Center the x-tick labels
    ax.set_xticklabels(sorted_query_numbers, rotation=90, ha='center', fontsize=55)
    ax.tick_params(axis='both', labelsize=55)
    ax.legend(fontsize=40)
    ax.set_ylim(0, 200)  # Adjusted y-limit based on data range

    ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
    ax.set_axisbelow(True)  # Put grid lines behind the bars

    # Calculate speedup statistics for each system compared to Vanilla Spark 3.5
    spark_35_velox_speedup_count = sum(1 for i in range(len(vanilla_spark_35_times)) if spark_35_velox_times[i] < vanilla_spark_35_times[i] and spark_35_velox_times[i] > 0)
    vanilla_spark_40_speedup_count = sum(1 for i in range(len(vanilla_spark_35_times)) if vanilla_spark_40_times[i] < vanilla_spark_35_times[i] and vanilla_spark_40_times[i] > 0)
    vanilla_presto_speedup_count = sum(1 for i in range(len(vanilla_spark_35_times)) if vanilla_presto_times[i] < vanilla_spark_35_times[i] and vanilla_presto_times[i] > 0)
    presto_velox_speedup_count = sum(1 for i in range(len(vanilla_spark_35_times)) if presto_velox_times[i] < vanilla_spark_35_times[i] and presto_velox_times[i] > 0)

    total_vanilla_spark_35_time = sum(vanilla_spark_35_times)
    total_spark_35_velox_time = sum(spark_35_velox_times)
    total_vanilla_spark_40_time = sum(vanilla_spark_40_times)
    total_vanilla_presto_time = sum(vanilla_presto_times)
    total_presto_velox_time = sum(presto_velox_times)

    summary_text = (f'Queries with Spark 3.5 + Velox Speedup: {spark_35_velox_speedup_count}/{len(vanilla_spark_35_times)}\n'
                    f'Queries with Vanilla Spark 4.0 Speedup: {vanilla_spark_40_speedup_count}/{len(vanilla_spark_35_times)}\n'
                    f'Queries with Vanilla Presto Speedup: {vanilla_presto_speedup_count}/{len(vanilla_spark_35_times)}\n'
                    f'Queries with Presto + Velox Speedup: {presto_velox_speedup_count}/{len(vanilla_spark_35_times)}\n'
                    f'Total Vanilla Spark 3.5 Time: {total_vanilla_spark_35_time:.2f} seconds\n'
                    f'Total Spark 3.5 + Velox Time: {total_spark_35_velox_time:.2f} seconds\n'
                    f'Total Vanilla Spark 4.0 Time: {total_vanilla_spark_40_time:.2f} seconds\n'
                    f'Total Vanilla Presto Time: {total_vanilla_presto_time:.2f} seconds\n'
                    f'Total Presto + Velox Time: {total_presto_velox_time:.2f} seconds')

    print(summary_text)

    plt.tight_layout()
    plt.savefig("query_join_performance_comparison.pdf", bbox_inches='tight', dpi=300)
    plt.show()

if __name__ == "__main__":
    main()