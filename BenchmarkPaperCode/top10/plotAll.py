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
    # q67 clickhouse could be 67 due to unnecessary R2C and C2R
    query_times = {
        'q72': {'vanilla': 51.7, 'clickhouse': 128.1, 'velox': 84.0, 'datafusion': 126.53},
        'q95': {'vanilla': 56.8, 'clickhouse': 93.9, 'velox': 72.45, 'datafusion': 76.5},
        'q64': {'vanilla': 93.08, 'clickhouse': 39.33, 'velox': 31.86, 'datafusion': 77.03},
        'q24a': {'vanilla': 188.11, 'clickhouse': 48.56, 'velox': 30.84, 'datafusion': 105.47},
        'q50': {'vanilla': 81.42, 'clickhouse': 16.15, 'velox': 19.58, 'datafusion': 55.7},
        'q88': {'vanilla': 32.58, 'clickhouse': 21.4, 'velox': 22.99, 'datafusion': 24.57},
        'q93': {'vanilla': 134.1, 'clickhouse': 36, 'velox': 23.8, 'datafusion': 56.9},
        'q4': {'vanilla': 268.37, 'clickhouse': 184.0, 'velox': 39.9, 'datafusion': 105.57},
        'q23a': {'vanilla': 235.2, 'clickhouse': 201.28, 'velox': 90.689721657, 'datafusion': 160.84},
        'q67': {'vanilla': 421.73, 'clickhouse': 306.0, 'velox': 231.0, 'datafusion': 298.1},

    }

    # Set font to a standard Type 1 font (e.g., Helvetica) to avoid Type 3 fonts in PDF
    plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['Helvetica']

    # Convert dictionary to the format needed for plotting
    query_data = [
        (query, times['vanilla'], times['clickhouse'], times['velox'], times['datafusion'])
        for query, times in query_times.items()
    ]

    #query_data.sort(key=lambda x: extract_numeric_part(x[0]))
    sorted_query_numbers = [data[0] for data in query_data]
    spark_execution_times = [data[1] for data in query_data]
    ck_execution_times = [data[2] for data in query_data]
    velox_execution_times = [data[3] for data in query_data]
    datafusion_comet_execution_times = [data[4] for data in query_data]

    fig, ax = plt.subplots(figsize=(30, 15))
    bar_width = 0.2  # Width of the bars
    group_width = bar_width * 5  # Width of each group

    index = [i * group_width for i in range(len(sorted_query_numbers))]

    # Plot bars for execution times
    bars1 = ax.bar([i for i in index], spark_execution_times, bar_width, label='Vanilla Spark', color='#354747')
    bars2 = ax.bar([i + bar_width for i in index], velox_execution_times, bar_width, label='Spark+Velox', color='#02b0a8')
    bars3 = ax.bar([i + 2 * bar_width for i in index], ck_execution_times, bar_width, label='Spark+ClickHouse', color='#ff5d67')
    bars4 = ax.bar([i + 3 * bar_width for i in index], datafusion_comet_execution_times, bar_width, label='Spark+DataFusion', color='#f0c917')

    # Use Helvetica font for all text elements
    fontdict_xlabel = {'fontsize': 55, 'fontname': 'Helvetica'}
    fontdict_ylabel = {'fontsize': 45, 'fontname': 'Helvetica'}
    fontdict_xtick = {'fontsize': 55, 'fontname': 'Helvetica'}
    fontdict_ytick = {'fontsize': 55, 'fontname': 'Helvetica'}
    fontdict_legend = {'fontsize': 45, 'prop': font_manager.FontProperties(family='Helvetica')}

    ax.set_xlabel('Query Number', **fontdict_xlabel)
    ax.set_ylabel('Execution Time (seconds)', **fontdict_ylabel)
    #ax.set_title('Execution Time Comparison for Queries')
    ax.set_xticks([i + 1.5*bar_width for i in index])
    ax.set_xticklabels(sorted_query_numbers, rotation=45, ha='right',fontsize=55)
    ax.tick_params(axis='both', labelsize=55)  # Add this line to set y-axis tick label size
    ax.legend(fontsize=45)
    ax.set_ylim(0, 500)
    # Annotate each bar with its y-coordinate value
    #for bars in [bars1, bars2, bars3, bars4]:
    #    for bar in bars:
    #        height = bar.get_height()
    #        ax.text(bar.get_x() + bar.get_width() / 2.0, height, round(height, 2), ha='center', va='bottom', fontname='Helvetica')

    velox_speedup_count = sum(1 for i in range(len(spark_execution_times)) if velox_execution_times[i] < spark_execution_times[i])
    comet_speedup_count = sum(1 for i in range(len(spark_execution_times)) if datafusion_comet_execution_times[i] < spark_execution_times[i])
    ck_speedup_count = sum(1 for i in range(len(spark_execution_times)) if ck_execution_times[i] < spark_execution_times[i])

    total_spark_time = sum(spark_execution_times)
    total_velox_time = sum(velox_execution_times)
    total_comet_time = sum(datafusion_comet_execution_times)
    total_ck_time = sum(ck_execution_times)

    summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{len(spark_execution_times)}\n'
                    f'Queries with DataFusion Speedup: {comet_speedup_count}/{len(spark_execution_times)}\n'
                    f'Queries with CK Speedup: {ck_speedup_count}/{len(spark_execution_times)}\n'
                    f'Total Spark Time: {total_spark_time:.2f} seconds\n'
                    f'Total Gluten-Velox Time: {total_velox_time:.2f} seconds\n'
                    f'Total Gluten-CK Time: {total_ck_time:.2f} seconds\n'
                    f'Total DataFusion Time: {total_comet_time:.2f} seconds')

    #plt.figtext(0.1, 0.8, summary_text, wrap=True, horizontalalignment='center', fontsize=12, fontname='Helvetica')

    plt.tight_layout()
    plt.savefig("top10_4node_parquet_partition.pdf", bbox_inches='tight', dpi=300)

    #plt.savefig("top10_4node_parquet_partition.png")
    plt.show()

if __name__ == "__main__":
    main()
