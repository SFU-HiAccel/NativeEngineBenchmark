import re
import matplotlib.pyplot as plt
from matplotlib import font_manager

# Function to generate a custom sorting key: (numeric part, letter suffix)
# This ensures 'q23a' (23, 'a') comes before 'q23b' (23, 'b').
def custom_query_sort_key(query_number):
    """
    Extracts the numeric part and optional letter suffix for stable sorting.
    Example: 'q23a' -> (23, 'a'), 'q9' -> (9, '')
    """
    # Pattern: 'q' + one or more digits (\d+) + zero or more letters ([a-z]*)
    match = re.match(r'q(\d+)([a-z]*)', query_number)
    if match:
        num = int(match.group(1))
        suffix = match.group(2)
        return (num, suffix)
    # Fallback for unexpected formats
    return (0, query_number)

def main():
    # Updated query times with Auron replacing DataFusion
    query_times = {
        'q72': {'vanilla': 51.712, 'clickhouse': 128.158, 'velox': 84.336, 'auron': 126.521},
        'q67': {'vanilla': 139.077, 'clickhouse': 102.975, 'velox': 66.335, 'auron': 92.64},
        'q23b': {'vanilla': 182.247, 'clickhouse': 160.442, 'velox': 66.223, 'auron': 85.566},
        'q95': {'vanilla': 56.756, 'clickhouse': 93.915, 'velox': 72.561, 'auron': 76.512},
        'q23a': {'vanilla': 181.65, 'clickhouse': 108.315, 'velox': 56.465, 'auron': 79.449},
        'q4': {'vanilla': 78.536, 'clickhouse': 68.791, 'velox': 34.66, 'auron': 53.793},
        'q64': {'vanilla': 93.987, 'clickhouse': 39.312, 'velox': 33.366, 'auron': 34.537},
        'q14a': {'vanilla': 63.628, 'clickhouse': 44.117, 'velox': 35.392, 'auron': 58.974},
        'q14b': {'vanilla': 58.874, 'clickhouse': 39.103, 'velox': 33.037, 'auron': 55.305},
        'q78': {'vanilla': 73.554, 'clickhouse': 23.896, 'velox': 45.146, 'auron': 51.877},
        'q24b': {'vanilla': 40.616, 'clickhouse': 27.383, 'velox': 22.227, 'auron': 30.357},
        'q93': {'vanilla': 97.086, 'clickhouse': 18.726, 'velox': 19.254, 'auron': 31.341},
        'q24a': {'vanilla': 44.927, 'clickhouse': 22.462, 'velox': 23.562, 'auron': 34.593},
        'q11': {'vanilla': 34.457, 'clickhouse': 27.287, 'velox': 20.598, 'auron': 27.647},
        'q50': {'vanilla': 59.023, 'clickhouse': 19.746, 'velox': 13.903, 'auron': 12.758},
        'q16': {'vanilla': 28.116, 'clickhouse': 12.219, 'velox': 12.432, 'auron': 18.338},
        'q28': {'vanilla': 35.743, 'clickhouse': 28.039, 'velox': 20.054, 'auron': 23.099},
        'q88': {'vanilla': 24.099, 'clickhouse': 14.553, 'velox': 16.472, 'auron': 22.609},
        'q75': {'vanilla': 31.778, 'clickhouse': 23.706, 'velox': 14.66, 'auron': 17.719},
        'q9': {'vanilla': 16.747, 'clickhouse': 10.396, 'velox': 10.906, 'auron': 14.091}
    }

    # Set font to a standard Type 1 font (e.g., Helvetica) to avoid Type 3 fonts in PDF
    plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['font.family'] = 'sans-serif'

    # Convert dictionary to the format needed for plotting
    query_data = [
        (query, times['vanilla'], times['clickhouse'], times['velox'], times['auron'])
        for query, times in query_times.items()
    ]

    # --- FIX: Sort by custom key to ensure 'q23a' comes before 'q23b' ---
    query_data.sort(key=lambda x: custom_query_sort_key(x[0]))
    
    sorted_query_numbers = [data[0] for data in query_data]
    spark_execution_times = [data[1] for data in query_data]
    ck_execution_times = [data[2] for data in query_data]
    velox_execution_times = [data[3] for data in query_data]
    auron_execution_times = [data[4] for data in query_data]

    fig, ax = plt.subplots(figsize=(30, 15))
    bar_width = 0.2  # Width of the bars
    group_width = bar_width * 5  # Width of each group

    index = [i * group_width for i in range(len(sorted_query_numbers))]

    # Plot bars for execution times
    bars1 = ax.bar([i for i in index], spark_execution_times, bar_width, label='Vanilla Spark', color='#354747')
    bars2 = ax.bar([i + bar_width for i in index], velox_execution_times, bar_width, label='Spark+Velox', color='#02b0a8')
    bars3 = ax.bar([i + 2 * bar_width for i in index], ck_execution_times, bar_width, label='Spark+ClickHouse', color='#ff5d67')
    # --- FIX: Updated label from 'Spark+DataFusion' to 'Spark+Auron' ---
    bars4 = ax.bar([i + 3 * bar_width for i in index], auron_execution_times, bar_width, label='Spark+Auron', color='#f0c917')

    # Use Helvetica-like font settings for all text elements
    fontdict_xlabel = {'fontsize': 50}
    fontdict_ylabel = {'fontsize': 40}
    fontdict_xtick = {'fontsize': 50}
    fontdict_ytick = {'fontsize': 50}

    ax.set_xlabel('Query Number', **fontdict_xlabel)
    ax.set_ylabel('Execution Time (seconds)', **fontdict_ylabel)
    ax.set_xticks([i + 1.5*bar_width for i in index])
    ax.set_xticklabels(sorted_query_numbers, rotation=90, ha='center', fontsize=55)
    ax.tick_params(axis='both', labelsize=55)
    ax.legend(fontsize=40)
    ax.set_ylim(0, 200)  # Adjusted y-limit based on new data range

    ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
    ax.set_axisbelow(True)  # Put grid lines behind the bars

    # Calculate speedup statistics
    velox_speedup_count = sum(1 for i in range(len(spark_execution_times)) if velox_execution_times[i] < spark_execution_times[i])
    auron_speedup_count = sum(1 for i in range(len(spark_execution_times)) if auron_execution_times[i] < spark_execution_times[i])
    ck_speedup_count = sum(1 for i in range(len(spark_execution_times)) if ck_execution_times[i] < spark_execution_times[i])

    total_spark_time = sum(spark_execution_times)
    total_velox_time = sum(velox_execution_times)
    total_auron_time = sum(auron_execution_times)
    total_ck_time = sum(ck_execution_times)

    summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{len(spark_execution_times)}\n'
                    f'Queries with Auron Speedup: {auron_speedup_count}/{len(spark_execution_times)}\n'
                    f'Queries with CK Speedup: {ck_speedup_count}/{len(spark_execution_times)}\n'
                    f'Total Spark Time: {total_spark_time:.2f} seconds\n'
                    f'Total Gluten-Velox Time: {total_velox_time:.2f} seconds\n'
                    f'Total Gluten-CK Time: {total_ck_time:.2f} seconds\n'
                    f'Total Auron Time: {total_auron_time:.2f} seconds')

    print(summary_text)

    plt.tight_layout()
    plt.savefig("query_top20_performance_comparison_v2.pdf", bbox_inches='tight', dpi=300)
    plt.savefig("query_top20_performance_comparison_v2.png", bbox_inches='tight', dpi=300)
    plt.show()

if __name__ == "__main__":
    main()
