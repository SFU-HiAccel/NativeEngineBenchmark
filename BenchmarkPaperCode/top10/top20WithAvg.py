# import re
# import matplotlib.pyplot as plt
# from matplotlib import font_manager

# # Function to generate a custom sorting key: (numeric part, letter suffix)
# # This ensures 'q23a' (23, 'a') comes before 'q23b' (23, 'b').
# def custom_query_sort_key(query_number):
#     """
#     Extracts the numeric part and optional letter suffix for stable sorting.
#     Example: 'q23a' -> (23, 'a'), 'q9' -> (9, '')
#     """
#     # Pattern: 'q' + one or more digits (\d+) + zero or more letters ([a-z]*)
#     match = re.match(r'q(\d+)([a-z]*)', query_number)
#     if match:
#         num = int(match.group(1))
#         suffix = match.group(2)
#         return (num, suffix)
#     # Fallback for unexpected formats
#     return (0, query_number)

# def main():
#     # Updated query times with Auron replacing DataFusion
#     query_times = {
#         'q72': {'vanilla': 51.712, 'clickhouse': 128.158, 'velox': 84.336, 'auron': 126.521},
#         'q67': {'vanilla': 139.077, 'clickhouse': 102.975, 'velox': 66.335, 'auron': 92.64},
#         'q23b': {'vanilla': 182.247, 'clickhouse': 160.442, 'velox': 66.223, 'auron': 85.566},
#         'q95': {'vanilla': 56.756, 'clickhouse': 93.915, 'velox': 72.561, 'auron': 76.512},
#         'q23a': {'vanilla': 181.65, 'clickhouse': 108.315, 'velox': 56.465, 'auron': 79.449},
#         'q4': {'vanilla': 78.536, 'clickhouse': 68.791, 'velox': 34.66, 'auron': 53.793},
#         'q64': {'vanilla': 93.987, 'clickhouse': 39.312, 'velox': 33.366, 'auron': 34.537},
#         'q14a': {'vanilla': 63.628, 'clickhouse': 44.117, 'velox': 35.392, 'auron': 58.974},
#         'q14b': {'vanilla': 58.874, 'clickhouse': 39.103, 'velox': 33.037, 'auron': 55.305},
#         'q78': {'vanilla': 73.554, 'clickhouse': 23.896, 'velox': 45.146, 'auron': 51.877},
#         'q24b': {'vanilla': 40.616, 'clickhouse': 27.383, 'velox': 22.227, 'auron': 30.357},
#         'q93': {'vanilla': 97.086, 'clickhouse': 18.726, 'velox': 19.254, 'auron': 31.341},
#         'q24a': {'vanilla': 44.927, 'clickhouse': 22.462, 'velox': 23.562, 'auron': 34.593},
#         'q11': {'vanilla': 34.457, 'clickhouse': 27.287, 'velox': 20.598, 'auron': 27.647},
#         'q50': {'vanilla': 59.023, 'clickhouse': 19.746, 'velox': 13.903, 'auron': 12.758},
#         'q16': {'vanilla': 28.116, 'clickhouse': 12.219, 'velox': 12.432, 'auron': 18.338},
#         'q28': {'vanilla': 35.743, 'clickhouse': 28.039, 'velox': 20.054, 'auron': 23.099},
#         'q88': {'vanilla': 24.099, 'clickhouse': 14.553, 'velox': 16.472, 'auron': 22.609},
#         'q75': {'vanilla': 31.778, 'clickhouse': 23.706, 'velox': 14.66, 'auron': 17.719},
#         'q9': {'vanilla': 16.747, 'clickhouse': 10.396, 'velox': 10.906, 'auron': 14.091}
#     }

#     # Define the order based on the bottleneck operator table
#     query_order = [
#         'q14a', 'q14b', 'q64', 'q72', 'q95',  # Hash Join
#         'q9', 'q16', 'q24a', 'q24b', 'q50', 'q75', 'q88',  # Table Scan
#         'q4', 'q11', 'q23a', 'q23b', 'q28',  # High-cardinality Hash Aggregation
#         'q67',  # Low-cardinality Hash Aggregation
#         'q78', 'q93'  # Sort
#     ]

#     # Set font to a standard Type 1 font (e.g., Helvetica) to avoid Type 3 fonts in PDF
#     plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
#     plt.rcParams['ps.fonttype'] = 42
#     plt.rcParams['font.family'] = 'sans-serif'

#     # Convert dictionary to the format needed for plotting using the specified order
#     query_data = [
#         (query, query_times[query]['vanilla'], query_times[query]['clickhouse'], 
#          query_times[query]['velox'], query_times[query]['auron'])
#         for query in query_order
#     ]
    
#     # Calculate averages for each engine
#     avg_vanilla = sum(query_times[q]['vanilla'] for q in query_order) / len(query_order)
#     avg_clickhouse = sum(query_times[q]['clickhouse'] for q in query_order) / len(query_order)
#     avg_velox = sum(query_times[q]['velox'] for q in query_order) / len(query_order)
#     avg_auron = sum(query_times[q]['auron'] for q in query_order) / len(query_order)
    
#     # Add average as the last entry
#     query_data.append(('Average', avg_vanilla, avg_clickhouse, avg_velox, avg_auron))
    
#     sorted_query_numbers = [data[0] for data in query_data]
#     spark_execution_times = [data[1] for data in query_data]
#     ck_execution_times = [data[2] for data in query_data]
#     velox_execution_times = [data[3] for data in query_data]
#     auron_execution_times = [data[4] for data in query_data]

#     fig, ax = plt.subplots(figsize=(32, 15))
#     bar_width = 0.2  # Width of the bars
#     group_width = bar_width * 5  # Width of each group

#     index = [i * group_width for i in range(len(sorted_query_numbers))]

#     # Plot bars for execution times
#     bars1 = ax.bar([i for i in index], spark_execution_times, bar_width, label='Vanilla Spark', color='#354747')
#     bars2 = ax.bar([i + bar_width for i in index], velox_execution_times, bar_width, label='Spark+Velox', color='#02b0a8')
#     bars3 = ax.bar([i + 2 * bar_width for i in index], ck_execution_times, bar_width, label='Spark+ClickHouse', color='#ff5d67')
#     bars4 = ax.bar([i + 3 * bar_width for i in index], auron_execution_times, bar_width, label='Spark+Auron', color='#f0c917')

#     # Use Helvetica-like font settings for all text elements
#     fontdict_xlabel = {'fontsize': 50}
#     fontdict_ylabel = {'fontsize': 40}
#     fontdict_xtick = {'fontsize': 50}
#     fontdict_ytick = {'fontsize': 50}

#     ax.set_xlabel('Query Number', **fontdict_xlabel)
#     ax.set_ylabel('Execution Time (seconds)', **fontdict_ylabel)
#     ax.set_xticks([i + 1.5*bar_width for i in index])
#     ax.set_xticklabels(sorted_query_numbers, rotation=90, ha='center', fontsize=55)
#     ax.tick_params(axis='both', labelsize=55)
#     ax.legend(fontsize=40)
#     ax.set_ylim(0, 200)  # Adjusted y-limit based on new data range

#     ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
#     ax.set_axisbelow(True)  # Put grid lines behind the bars

#     # Calculate speedup statistics (excluding the average)
#     query_count = len(query_order)  # Exclude average from count
#     velox_speedup_count = sum(1 for i in range(query_count) if velox_execution_times[i] < spark_execution_times[i])
#     auron_speedup_count = sum(1 for i in range(query_count) if auron_execution_times[i] < spark_execution_times[i])
#     ck_speedup_count = sum(1 for i in range(query_count) if ck_execution_times[i] < spark_execution_times[i])

#     # Calculate how many queries each engine runs fastest
#     velox_fastest_count = 0
#     ck_fastest_count = 0
#     auron_fastest_count = 0
#     vanilla_fastest_count = 0
    
#     for i in range(query_count):
#         times = [spark_execution_times[i], velox_execution_times[i], ck_execution_times[i], auron_execution_times[i]]
#         min_time = min(times)
#         if velox_execution_times[i] == min_time:
#             velox_fastest_count += 1
#         elif ck_execution_times[i] == min_time:
#             ck_fastest_count += 1
#         elif auron_execution_times[i] == min_time:
#             auron_fastest_count += 1
#         else:  # vanilla is fastest
#             vanilla_fastest_count += 1

#     total_spark_time = sum(spark_execution_times[:-1])  # Exclude average
#     total_velox_time = sum(velox_execution_times[:-1])  # Exclude average
#     total_auron_time = sum(auron_execution_times[:-1])  # Exclude average
#     total_ck_time = sum(ck_execution_times[:-1])  # Exclude average

#     summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{query_count}\n'
#                     f'Queries with Auron Speedup: {auron_speedup_count}/{query_count}\n'
#                     f'Queries with CK Speedup: {ck_speedup_count}/{query_count}\n'
#                     f'Queries where Velox runs fastest: {velox_fastest_count}/{query_count}\n'
#                     f'Queries where ClickHouse runs fastest: {ck_fastest_count}/{query_count}\n'
#                     f'Queries where Auron runs fastest: {auron_fastest_count}/{query_count}\n'
#                     f'Queries where Vanilla runs fastest: {vanilla_fastest_count}/{query_count}\n'
#                     f'Total Spark Time: {total_spark_time:.2f} seconds\n'
#                     f'Total Gluten-Velox Time: {total_velox_time:.2f} seconds\n'
#                     f'Total Gluten-CK Time: {total_ck_time:.2f} seconds\n'
#                     f'Total Auron Time: {total_auron_time:.2f} seconds\n'
#                     f'Average Times - Vanilla: {avg_vanilla:.2f}s, Velox: {avg_velox:.2f}s, ClickHouse: {avg_clickhouse:.2f}s, Auron: {avg_auron:.2f}s')

#     print(summary_text)

#     plt.tight_layout()
#     plt.savefig("query_top20_performance_comparison_v2.pdf", bbox_inches='tight', dpi=300)
#     plt.savefig("query_top20_performance_comparison_v2.png", bbox_inches='tight', dpi=300)
#     plt.show()

# if __name__ == "__main__":
#     main()

import re
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np

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

    # Define query types based on the bottleneck operator table
    query_types = {
        'Hash Join': ['q14a', 'q14b', 'q64', 'q72', 'q95'],
        'Table Scan': ['q9', 'q16', 'q24a', 'q24b', 'q50', 'q75', 'q88'],
        'Hash Agg': ['q4', 'q11', 'q23a', 'q23b', 'q28', 'q67'],
        'Sort': ['q78', 'q93']
    }

    # Create ordered query list by type with numeric sorting within each type
    ordered_queries = []
    type_positions = {}
    current_pos = 0
    
    for query_type, queries in query_types.items():
        type_positions[query_type] = (current_pos, current_pos + len(queries))
        # Sort queries within each type by numeric part and suffix
        sorted_queries = sorted(queries, key=custom_query_sort_key)
        ordered_queries.extend(sorted_queries)
        current_pos += len(queries)

    # Set font to a standard Type 1 font (e.g., Helvetica) to avoid Type 3 fonts in PDF
    plt.rcParams['pdf.fonttype'] = 42  # TrueType (Type 42)
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['font.family'] = 'sans-serif'

    # Convert dictionary to the format needed for plotting using the grouped order
    query_data = [
        (query, query_times[query]['vanilla'], query_times[query]['clickhouse'], 
         query_times[query]['velox'], query_times[query]['auron'])
        for query in ordered_queries
    ]
    
    # Calculate averages for each engine
    avg_vanilla = sum(query_times[q]['vanilla'] for q in ordered_queries) / len(ordered_queries)
    avg_clickhouse = sum(query_times[q]['clickhouse'] for q in ordered_queries) / len(ordered_queries)
    avg_velox = sum(query_times[q]['velox'] for q in ordered_queries) / len(ordered_queries)
    avg_auron = sum(query_times[q]['auron'] for q in ordered_queries) / len(ordered_queries)
    
    # Add average as the last entry
    query_data.append(('Average', avg_vanilla, avg_clickhouse, avg_velox, avg_auron))
    
    sorted_query_numbers = [data[0] for data in query_data]
    spark_execution_times = [data[1] for data in query_data]
    ck_execution_times = [data[2] for data in query_data]
    velox_execution_times = [data[3] for data in query_data]
    auron_execution_times = [data[4] for data in query_data]

    fig, ax = plt.subplots(figsize=(32, 16))
    bar_width = 0.3  # Adjusted width of the bars
    group_spacing = 1  # Increased spacing between query type groups
    
    # Calculate positions for bars with wider gaps between groups
    positions = []
    type_centers = []
    current_x = 0
    
    for query_type, queries in query_types.items():
        type_start = current_x
        group_queries = [q for q in queries if q in ordered_queries]
        
        for i, query in enumerate(group_queries):
            positions.append(current_x)
            current_x += bar_width * 5  # 4 bars + small space between individual queries
        
        current_x += group_spacing * 0.5  # Large space after each group
        type_centers.append((type_start + current_x - group_spacing * 1) / 2)
    
    # Add position for average bar at the end
    avg_position = current_x
    positions.append(avg_position)

    # Individual query positions (exclude average)
    query_positions = positions[:-1]

    # Plot bars for execution times
    bars1 = ax.bar([p for p in query_positions], spark_execution_times[:-1], bar_width, 
                   label='Vanilla Spark', color='#354747')
    bars2 = ax.bar([p + bar_width for p in query_positions], velox_execution_times[:-1], bar_width, 
                   label='Spark+Velox', color='#02b0a8')
    bars3 = ax.bar([p + 2 * bar_width for p in query_positions], ck_execution_times[:-1], bar_width, 
                   label='Spark+ClickHouse', color='#ff5d67')
    bars4 = ax.bar([p + 3 * bar_width for p in query_positions], auron_execution_times[:-1], bar_width, 
                   label='Spark+Auron', color='#f0c917')

    # Plot average bars
    avg_pos = positions[-1]
    ax.bar(avg_pos, spark_execution_times[-1], bar_width, color='#354747')
    ax.bar(avg_pos + bar_width, velox_execution_times[-1], bar_width, color='#02b0a8')
    ax.bar(avg_pos + 2 * bar_width, ck_execution_times[-1], bar_width, color='#ff5d67')
    ax.bar(avg_pos + 3 * bar_width, auron_execution_times[-1], bar_width, color='#f0c917')

    # Use Helvetica-like font settings for all text elements
    fontdict_xlabel = {'fontsize': 40}
    fontdict_ylabel = {'fontsize': 48}

    ax.set_ylabel('Execution Time (seconds)', **fontdict_ylabel)
    
    # Set x-tick positions and labels
    all_positions = [p + 1.5 * bar_width for p in query_positions] + [avg_pos + 1.5 * bar_width]
    ax.set_xticks(all_positions)
    ax.set_xticklabels(sorted_query_numbers, rotation=90, ha='center', fontsize=52)
    ax.tick_params(axis='both', labelsize=35)
    ax.legend(fontsize=44, loc='upper right')
    ax.set_ylim(0, 200)
    ax.set_xlim(-bar_width, avg_pos + bar_width * 5.2)

    ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    # Add query type labels BELOW the x-axis labels
    # Calculate the position below the x-axis labels
    label_y_position = -ax.get_ylim()[1] * 0.1  # Position below x-axis
    
    for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
        center_x = type_centers[i]
        ax.text(center_x, label_y_position, query_type, 
               ha='center', va='top', fontsize=48, fontweight='bold', rotation=0)

    # Add horizontal lines under query type labels to group them
    line_y_position = -ax.get_ylim()[1] * 5  # Position below the labels
    
    for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
        group_queries = [q for q in query_types[query_type] if q in ordered_queries]
        if group_queries:
            start_query_idx = ordered_queries.index(group_queries[0])
            end_query_idx = ordered_queries.index(group_queries[-1])
            
            start_x = query_positions[start_query_idx] - bar_width * 0.5
            end_x = query_positions[end_query_idx] + bar_width * 4.5
            
            ax.plot([start_x, end_x], [line_y_position, line_y_position], 
                   color='black', linewidth=2, alpha=0.7)
    
    # Add line under Average bar
    avg_start_x = avg_pos - bar_width * 0.5
    avg_end_x = avg_pos + bar_width * 4.5
    ax.plot([avg_start_x, avg_end_x], [line_y_position, line_y_position], 
           color='black', linewidth=2, alpha=0.7)

    # Calculate speedup statistics (excluding the average)
    query_count = len(ordered_queries)
    velox_speedup_count = sum(1 for i in range(query_count) if velox_execution_times[i] < spark_execution_times[i])
    auron_speedup_count = sum(1 for i in range(query_count) if auron_execution_times[i] < spark_execution_times[i])
    ck_speedup_count = sum(1 for i in range(query_count) if ck_execution_times[i] < spark_execution_times[i])

    # Calculate how many queries each engine runs fastest
    velox_fastest_count = 0
    ck_fastest_count = 0
    auron_fastest_count = 0
    vanilla_fastest_count = 0
    
    for i in range(query_count):
        times = [spark_execution_times[i], velox_execution_times[i], ck_execution_times[i], auron_execution_times[i]]
        min_time = min(times)
        if velox_execution_times[i] == min_time:
            velox_fastest_count += 1
        elif ck_execution_times[i] == min_time:
            ck_fastest_count += 1
        elif auron_execution_times[i] == min_time:
            auron_fastest_count += 1
        else:  # vanilla is fastest
            vanilla_fastest_count += 1

    total_spark_time = sum(spark_execution_times[:-1])
    total_velox_time = sum(velox_execution_times[:-1])
    total_auron_time = sum(auron_execution_times[:-1])
    total_ck_time = sum(ck_execution_times[:-1])

    summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{query_count}\n'
                    f'Queries with Auron Speedup: {auron_speedup_count}/{query_count}\n'
                    f'Queries with CK Speedup: {ck_speedup_count}/{query_count}\n'
                    f'Queries where Velox runs fastest: {velox_fastest_count}/{query_count}\n'
                    f'Queries where ClickHouse runs fastest: {ck_fastest_count}/{query_count}\n'
                    f'Queries where Auron runs fastest: {auron_fastest_count}/{query_count}\n'
                    f'Queries where Vanilla runs fastest: {vanilla_fastest_count}/{query_count}\n'
                    f'Total Spark Time: {total_spark_time:.2f} seconds\n'
                    f'Total Gluten-Velox Time: {total_velox_time:.2f} seconds\n'
                    f'Total Gluten-CK Time: {total_ck_time:.2f} seconds\n'
                    f'Total Auron Time: {total_auron_time:.2f} seconds\n'
                    f'Average Times - Vanilla: {avg_vanilla:.2f}s, Velox: {avg_velox:.2f}s, ClickHouse: {avg_clickhouse:.2f}s, Auron: {avg_auron:.2f}s')

    print(summary_text)

    plt.tight_layout()
    plt.savefig("query_top20_performance_comparison_grouped.pdf", bbox_inches='tight', dpi=300)
    plt.savefig("query_top20_performance_comparison_grouped.png", bbox_inches='tight', dpi=300)
    plt.show()

if __name__ == "__main__":
    main()