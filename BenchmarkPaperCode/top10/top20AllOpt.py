# import os
# import re
# import matplotlib.pyplot as plt
# from matplotlib import font_manager
# import numpy as np

# def extract_execution_time(file_path):
#     try:
#         with open(file_path, 'r') as file:
#             lines = file.readlines()
#             if len(lines) >= 3:
#                 try:
#                     execution_time = float(lines[3].split(":")[-1].strip().split()[0])
#                     return execution_time
#                 except ValueError:
#                     print(f"Error extracting execution time from {file_path}")
#     except Exception as e:
#         print(f"Error reading file {file_path}: {e}")
#     return None

# def extract_numeric_part(query_number):
#     match = re.search(r'\d+', query_number)
#     return int(match.group()) if match else 0

# def calculate_combined_opt(velox_time, opt1_time, opt2_time):
#     """Calculate Spark+Velox+Opt1+Opt2 = velox - (velox - opt1) - (velox - opt2)"""
#     opt1_improvement = velox_time - opt1_time
#     opt2_improvement = velox_time - opt2_time
#     return velox_time - opt1_improvement - opt2_improvement

# def main():    
#     query_times = {
#         'q72': {'vanilla': 51.712,  'velox': 84.336, 'Spark+Velox+Opt1': 39.6, 'Spark+Velox+Opt2': 81.02},
#         'q67': {'vanilla': 139.077, 'velox': 66.335, 'Spark+Velox+Opt1': 66.18,'Spark+Velox+Opt2': 60.921},
#         'q23b': {'vanilla': 182.247, 'velox': 66.223, 'Spark+Velox+Opt1': 66.01,'Spark+Velox+Opt2': 60.93},
#         'q95': {'vanilla': 56.756, 'velox': 72.561, 'Spark+Velox+Opt1': 44.22,'Spark+Velox+Opt2': 69.19},
#         'q23a': {'vanilla': 181.65,  'velox': 56.465, 'Spark+Velox+Opt1': 55.98,'Spark+Velox+Opt2': 51.66},
#         'q4': {'vanilla': 78.536,  'velox': 34.66, 'Spark+Velox+Opt1': 33.98,'Spark+Velox+Opt2': 30.41},
#         'q64': {'vanilla': 93.987,  'velox': 33.366, 'Spark+Velox+Opt1': 31.7,'Spark+Velox+Opt2': 25.583},
#         'q14a': {'vanilla': 63.628,  'velox': 35.392, 'Spark+Velox+Opt1': 35.37, 'Spark+Velox+Opt2': 33.47},
#         'q14b': {'vanilla': 58.874,  'velox': 33.037, 'Spark+Velox+Opt1': 33.03, 'Spark+Velox+Opt2': 31.84},
#         'q78': {'vanilla': 73.554,  'velox': 23.896, 'Spark+Velox+Opt1': 23.15, 'Spark+Velox+Opt2': 17.617},
#         'q24b': {'vanilla': 40.616,  'velox': 22.227, 'Spark+Velox+Opt1': 21.98, 'Spark+Velox+Opt2': 15.189},
#         'q93': {'vanilla': 97.086,  'velox': 19.254, 'Spark+Velox+Opt1': 19.204, 'Spark+Velox+Opt2': 16.614},
#         'q24a': {'vanilla': 44.927,  'velox': 23.562, 'Spark+Velox+Opt1': 22.98, 'Spark+Velox+Opt2': 12.534},
#         'q11': {'vanilla': 34.457,  'velox': 20.598, 'Spark+Velox+Opt1': 17.35, 'Spark+Velox+Opt2': 16.85},
#         'q50': {'vanilla': 59.023,  'velox': 13.903, 'Spark+Velox+Opt1': 13.88, 'Spark+Velox+Opt2': 11.387},
#         'q16': {'vanilla': 28.116,  'velox': 12.432, 'Spark+Velox+Opt1': 12.67, 'Spark+Velox+Opt2': 9.837},
#         'q28': {'vanilla': 35.743,  'velox': 20.054, 'Spark+Velox+Opt1': 20.04, 'Spark+Velox+Opt2': 18.84},
#         'q88': {'vanilla': 24.099,  'velox': 16.472, 'Spark+Velox+Opt1': 16.27, 'Spark+Velox+Opt2': 9.339},
#         'q75': {'vanilla': 31.778,  'velox': 14.66, 'Spark+Velox+Opt1': 14.71, 'Spark+Velox+Opt2': 10.652},
#         'q9': {'vanilla': 16.747, 'velox': 10.906, 'Spark+Velox+Opt1': 10.76, 'Spark+Velox+Opt2': 8.769}
#     }
    
#     # Define query types based on the table
#     query_types = {
#         'Hash Join': ['q14a', 'q14b', 'q64', 'q72', 'q95'],
#         'Table Scan': ['q9', 'q16', 'q24a', 'q24b', 'q50', 'q75', 'q88'],
#         'High-cardinality Hash Aggregation': ['q4', 'q11', 'q23a', 'q23b', 'q28'],
#         'Low-cardinality Hash Aggregation': ['q67'],
#         'Sort': ['q78', 'q93']
#     }
    
#     # Remove the minus 5 second adjustment

#     # Calculate Spark+Velox+Opt1+Opt2 for each query
#     for query, times in query_times.items():
#         combined_time = calculate_combined_opt(
#             times['velox'], 
#             times['Spark+Velox+Opt1'], 
#             times['Spark+Velox+Opt2']
#         )
#         times['Spark+Velox+Opt1+Opt2'] = combined_time

#     # Calculate maximum speedups over Velox
#     opt1_speedups = []
#     opt2_speedups = []
#     combined_speedups = []
    
#     for query, times in query_times.items():
#         opt1_speedup = times['velox'] / times['Spark+Velox+Opt1']
#         opt2_speedup = times['velox'] / times['Spark+Velox+Opt2']
#         combined_speedup = times['velox'] / times['Spark+Velox+Opt1+Opt2']
        
#         opt1_speedups.append(opt1_speedup)
#         opt2_speedups.append(opt2_speedup)
#         combined_speedups.append(combined_speedup)
    
#     max_opt1_speedup = max(opt1_speedups)
#     max_opt2_speedup = max(opt2_speedups)
#     max_combined_speedup = max(combined_speedups)
    
#     print(f"Maximum speedups over Velox:")
#     print(f"  Opt1 max speedup: {max_opt1_speedup:.2f}x")
#     print(f"  Opt2 max speedup: {max_opt2_speedup:.2f}x")
#     print(f"  Opt1+Opt2 max speedup: {max_combined_speedup:.2f}x")
#     print()

#     # Create ordered query list by type
#     ordered_queries = []
#     type_positions = {}
#     current_pos = 0
    
#     for query_type, queries in query_types.items():
#         type_positions[query_type] = (current_pos, current_pos + len(queries))
#         # Sort queries within each type by numeric part
#         sorted_queries = sorted(queries, key=extract_numeric_part)
#         ordered_queries.extend(sorted_queries)
#         current_pos += len(queries)
    
#     # Convert dictionary to the format needed for plotting
#     query_data = []
#     for query in ordered_queries:
#         times = query_times[query]
#         query_data.append((query, times['vanilla'], times['velox'], times['Spark+Velox+Opt1'], 
#                           times['Spark+Velox+Opt2'], times['Spark+Velox+Opt1+Opt2']))

#     sorted_query_numbers = [data[0] for data in query_data]
#     vanilla_execution_times = [data[1] for data in query_data]
#     velox_execution_times = [data[2] for data in query_data]
#     opt1_execution_times = [data[3] for data in query_data]
#     opt2_execution_times = [data[4] for data in query_data]
#     combined_execution_times = [data[5] for data in query_data]

#     # Calculate overall average execution times
#     all_vanilla_avg = np.mean([query_times[q]['vanilla'] for q in query_times.keys()])
#     all_velox_avg = np.mean([query_times[q]['velox'] for q in query_times.keys()])
#     all_opt1_avg = np.mean([query_times[q]['Spark+Velox+Opt1'] for q in query_times.keys()])
#     all_opt2_avg = np.mean([query_times[q]['Spark+Velox+Opt2'] for q in query_times.keys()])
#     all_combined_avg = np.mean([query_times[q]['Spark+Velox+Opt1+Opt2'] for q in query_times.keys()])
    
#     overall_averages = {
#         'vanilla': all_vanilla_avg,
#         'velox': all_velox_avg,
#         'opt1': all_opt1_avg,
#         'opt2': all_opt2_avg,
#         'combined': all_combined_avg
#     }

#     # Create the plot
#     fig, ax = plt.subplots(figsize=(35, 15))
#     bar_width = 0.12  # Width of the bars
#     spacing = 0.02    # Space between query types
    
#     # Calculate positions for bars
#     positions = []
#     type_centers = []
#     current_x = 0
    
#     for query_type, queries in query_types.items():
#         type_start = current_x
#         for i, query in enumerate(sorted([q for q in queries if q in ordered_queries], key=lambda x: ordered_queries.index(x))):
#             positions.append(current_x)
#             current_x += bar_width * 6  # 5 bars + space between queries
        
#         current_x += spacing * 10  # Space after type group
#         type_centers.append((type_start + current_x - spacing * 10) / 2)
    
#     # Add one average bar at the end
#     avg_position = current_x + spacing * 5
#     positions.append(avg_position)

#     # Plot individual query bars
#     query_positions = positions[:-1]  # Remove average position
    
#     bars1 = ax.bar([p for p in query_positions], vanilla_execution_times, bar_width, 
#                    label='Vanilla Spark', color='#354747')
#     bars2 = ax.bar([p + bar_width for p in query_positions], velox_execution_times, bar_width, 
#                    label='Spark+Velox', color='#02b0a8')
#     bars3 = ax.bar([p + 2 * bar_width for p in query_positions], opt1_execution_times, bar_width, 
#                    label='Spark+Velox+Opt1', color='#ff5d67')
#     bars4 = ax.bar([p + 3 * bar_width for p in query_positions], opt2_execution_times, bar_width, 
#                    label='Spark+Velox+Opt2', color='#f0c917')
#     bars5 = ax.bar([p + 4 * bar_width for p in query_positions], combined_execution_times, bar_width, 
#                    label='Spark+Velox+Opt1+Opt2', color='#9b59b6')

#     # Plot single overall average bar
#     avg_pos = positions[-1]
#     ax.bar(avg_pos, overall_averages['vanilla'], bar_width, color='#354747')
#     ax.bar(avg_pos + bar_width, overall_averages['velox'], bar_width, color='#02b0a8')
#     ax.bar(avg_pos + 2 * bar_width, overall_averages['opt1'], bar_width, color='#ff5d67')
#     ax.bar(avg_pos + 3 * bar_width, overall_averages['opt2'], bar_width, color='#f0c917')
#     ax.bar(avg_pos + 4 * bar_width, overall_averages['combined'], bar_width, color='#9b59b6')

#     # Set up the plot
#     #ax.set_xlabel('Query Number', fontsize=40)
#     ax.set_ylabel('Execution Time (seconds)', fontsize=40)
    
#     # Create x-tick labels
#     all_labels = sorted_query_numbers + ['Average']
#     all_positions = [p + 2 * bar_width for p in query_positions] + [avg_pos + 2 * bar_width]
    
#     ax.set_xticks(all_positions)
#     ax.set_xticklabels(all_labels, rotation=90, ha='right', fontsize=28)
#     ax.tick_params(axis='both', labelsize=35)
    
#     # Add horizontal lines under x-axis labels to group query types
#     line_y_position = -ax.get_ylim()[1] * 0.25  # Position below the group labels
    
#     for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
#         # Calculate start and end positions for the group
#         group_queries = [q for q in query_types[query_type] if q in ordered_queries]
#         if group_queries:
#             start_query_idx = ordered_queries.index(group_queries[0])
#             end_query_idx = ordered_queries.index(group_queries[-1])
            
#             start_x = query_positions[start_query_idx] - bar_width * 0.5
#             end_x = query_positions[end_query_idx] + bar_width * 5.5
            
#             # Draw horizontal line under the group
#             ax.plot([start_x, end_x], [line_y_position, line_y_position], 
#                    color='black', linewidth=2, alpha=0.7)
    
#     # Add line under Average bar
#     avg_start_x = avg_pos - bar_width * 0.5
#     avg_end_x = avg_pos + bar_width * 5.5
#     ax.plot([avg_start_x, avg_end_x], [line_y_position, line_y_position], 
#            color='black', linewidth=2, alpha=0.7)
    
#     # Add query type labels under x-axis (without frames)
#     type_label_mapping = {
#         'Hash Join': 'Hash Join',
#         'Table Scan': 'Table Scan',
#         'High-cardinality Hash Aggregation': 'High-Card HashAgg',
#         'Low-cardinality Hash Aggregation': 'Low-Card HashAgg',
#         'Sort': 'Sort'
#     }
    
#     for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
#         center_x = type_centers[i]
#         # Position text below the x-axis without frame
#         ax.text(center_x, -ax.get_ylim()[1] * 0.15, type_label_mapping[query_type], 
#                ha='center', va='top', fontsize=24, fontweight='bold', 
#                rotation=90)  # Add slight rotation for better readability

#     ax.legend(fontsize=32, loc='upper right')
#     ax.set_ylim(0, 200)
#     ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
#     ax.set_axisbelow(True)

#     # Calculate and print statistics
#     velox_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if velox_execution_times[i] < vanilla_execution_times[i])
#     opt1_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if opt1_execution_times[i] < vanilla_execution_times[i])
#     opt2_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if opt2_execution_times[i] < vanilla_execution_times[i])
#     combined_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if combined_execution_times[i] < vanilla_execution_times[i])

#     total_vanilla_time = sum(vanilla_execution_times)
#     total_velox_time = sum(velox_execution_times)
#     total_opt1_time = sum(opt1_execution_times)
#     total_opt2_time = sum(opt2_execution_times)
#     total_combined_time = sum(combined_execution_times)

#     summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{len(vanilla_execution_times)}\n'
#                     f'Queries with Opt1 Speedup: {opt1_speedup_count}/{len(vanilla_execution_times)}\n'
#                     f'Queries with Opt2 Speedup: {opt2_speedup_count}/{len(vanilla_execution_times)}\n'
#                     f'Queries with Combined Speedup: {combined_speedup_count}/{len(vanilla_execution_times)}\n'
#                     f'Total Vanilla Spark Time: {total_vanilla_time:.2f} seconds\n'
#                     f'Total Spark+Velox Time: {total_velox_time:.2f} seconds\n'
#                     f'Total Spark+Velox+Opt1 Time: {total_opt1_time:.2f} seconds\n'
#                     f'Total Spark+Velox+Opt2 Time: {total_opt2_time:.2f} seconds\n'
#                     f'Total Spark+Velox+Opt1+Opt2 Time: {total_combined_time:.2f} seconds')

#     print(summary_text)
#     print()
    
#     # Print overall average speedups
#     print("Overall average speedups (over Velox):")
#     overall_opt1_speedup = overall_averages['velox'] / overall_averages['opt1']
#     overall_opt2_speedup = overall_averages['velox'] / overall_averages['opt2']
#     overall_combined_speedup = overall_averages['velox'] / overall_averages['combined']
    
#     print(f"  Overall Opt1 avg speedup: {overall_opt1_speedup:.2f}x")
#     print(f"  Overall Opt2 avg speedup: {overall_opt2_speedup:.2f}x")
#     print(f"  Overall Combined avg speedup: {overall_combined_speedup:.2f}x")

#     plt.tight_layout()
#     plt.savefig("query_top20_performance_comparison_opt_enhanced.pdf", bbox_inches='tight', dpi=300)
#     plt.savefig("query_top20_performance_comparison_opt_enhanced.png", bbox_inches='tight', dpi=300)
#     plt.show()

# if __name__ == "__main__":
#     main()

import os
import re
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np

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

def calculate_combined_opt(velox_time, opt1_time, opt2_time):
    """Calculate Spark+Velox+Opt1+Opt2 = velox - (velox - opt1) - (velox - opt2)"""
    opt1_improvement = velox_time - opt1_time
    opt2_improvement = velox_time - opt2_time
    return velox_time - opt1_improvement - opt2_improvement

def main():    
    query_times = {
        'q72': {'vanilla': 51.712,  'velox': 84.336, 'Spark+Velox+Opt1': 39.6, 'Spark+Velox+Opt2': 80.02},
        'q67': {'vanilla': 139.077, 'velox': 66.335, 'Spark+Velox+Opt1': 66.18,'Spark+Velox+Opt2': 63.921},
        'q23b': {'vanilla': 182.247, 'velox': 66.223, 'Spark+Velox+Opt1': 66.01,'Spark+Velox+Opt2': 60.93},
        'q95': {'vanilla': 56.756, 'velox': 72.561, 'Spark+Velox+Opt1': 44.22,'Spark+Velox+Opt2': 67.19},
        'q23a': {'vanilla': 181.65,  'velox': 56.465, 'Spark+Velox+Opt1': 55.98,'Spark+Velox+Opt2': 51.66},
        'q4': {'vanilla': 78.536,  'velox': 34.66, 'Spark+Velox+Opt1': 33.98,'Spark+Velox+Opt2': 32.41},
        'q64': {'vanilla': 93.987,  'velox': 33.366, 'Spark+Velox+Opt1': 31.7,'Spark+Velox+Opt2': 25.583},
        'q14a': {'vanilla': 63.628,  'velox': 35.392, 'Spark+Velox+Opt1': 35.37, 'Spark+Velox+Opt2': 33.47},
        'q14b': {'vanilla': 58.874,  'velox': 33.037, 'Spark+Velox+Opt1': 33.03, 'Spark+Velox+Opt2': 31.84},
        'q78': {'vanilla': 73.554,  'velox': 23.896, 'Spark+Velox+Opt1': 23.15, 'Spark+Velox+Opt2': 20.617},
        'q24b': {'vanilla': 40.616,  'velox': 22.227, 'Spark+Velox+Opt1': 21.98, 'Spark+Velox+Opt2': 15.189},
        'q93': {'vanilla': 97.086,  'velox': 19.254, 'Spark+Velox+Opt1': 19.204, 'Spark+Velox+Opt2': 15.614},
        'q24a': {'vanilla': 44.927,  'velox': 23.562, 'Spark+Velox+Opt1': 22.98, 'Spark+Velox+Opt2': 12.534},
        'q11': {'vanilla': 34.457,  'velox': 20.598, 'Spark+Velox+Opt1': 17.35, 'Spark+Velox+Opt2': 19.85},
        'q50': {'vanilla': 59.023,  'velox': 13.903, 'Spark+Velox+Opt1': 13.88, 'Spark+Velox+Opt2': 11.387},
        'q16': {'vanilla': 28.116,  'velox': 12.432, 'Spark+Velox+Opt1': 12.67, 'Spark+Velox+Opt2': 9.837},
        'q28': {'vanilla': 35.743,  'velox': 20.054, 'Spark+Velox+Opt1': 20.04, 'Spark+Velox+Opt2': 18.84},
        'q88': {'vanilla': 24.099,  'velox': 16.472, 'Spark+Velox+Opt1': 16.27, 'Spark+Velox+Opt2': 9.339},
        'q75': {'vanilla': 31.778,  'velox': 14.66, 'Spark+Velox+Opt1': 14.71, 'Spark+Velox+Opt2': 10.652},
        'q9': {'vanilla': 16.747, 'velox': 10.906, 'Spark+Velox+Opt1': 10.76, 'Spark+Velox+Opt2': 8.769}
    }
    
    # Define query types based on the table
    query_types = {
        'Hash Join': ['q14a', 'q14b', 'q64', 'q72', 'q95'],
        'Table Scan': ['q9', 'q16', 'q24a', 'q24b', 'q50', 'q75', 'q88'],
        'Hash Agg': ['q4', 'q11', 'q23a', 'q23b', 'q28','q67'],
        #'Low-cardinality Hash Aggregation': [],
        'Sort': ['q78', 'q93']
    }
    
    # Remove the minus 5 second adjustment

    # Calculate Spark+Velox+Opt1+Opt2 for each query
    for query, times in query_times.items():
        combined_time = calculate_combined_opt(
            times['velox'], 
            times['Spark+Velox+Opt1'], 
            times['Spark+Velox+Opt2']
        )
        times['Spark+Velox+Opt1+Opt2'] = combined_time

    # Calculate maximum speedups over Velox
    opt1_speedups = []
    opt2_speedups = []
    combined_speedups = []
    
    for query, times in query_times.items():
        opt1_speedup = times['velox'] / times['Spark+Velox+Opt1']
        opt2_speedup = times['velox'] / times['Spark+Velox+Opt2']
        combined_speedup = times['velox'] / times['Spark+Velox+Opt1+Opt2']
        
        opt1_speedups.append(opt1_speedup)
        opt2_speedups.append(opt2_speedup)
        combined_speedups.append(combined_speedup)
    
    max_opt1_speedup = max(opt1_speedups)
    max_opt2_speedup = max(opt2_speedups)
    max_combined_speedup = max(combined_speedups)
    
    print(f"Maximum speedups over Velox:")
    print(f"  Opt1 max speedup: {max_opt1_speedup:.2f}x")
    print(f"  Opt2 max speedup: {max_opt2_speedup:.2f}x")
    print(f"  Opt1+Opt2 max speedup: {max_combined_speedup:.2f}x")
    print()

    # Create ordered query list by type
    ordered_queries = []
    type_positions = {}
    current_pos = 0
    
    for query_type, queries in query_types.items():
        type_positions[query_type] = (current_pos, current_pos + len(queries))
        # Sort queries within each type by numeric part
        sorted_queries = sorted(queries, key=extract_numeric_part)
        ordered_queries.extend(sorted_queries)
        current_pos += len(queries)
    
    # Convert dictionary to the format needed for plotting
    query_data = []
    for query in ordered_queries:
        times = query_times[query]
        query_data.append((query, times['vanilla'], times['velox'], times['Spark+Velox+Opt1'], 
                          times['Spark+Velox+Opt2'], times['Spark+Velox+Opt1+Opt2']))

    sorted_query_numbers = [data[0] for data in query_data]
    vanilla_execution_times = [data[1] for data in query_data]
    velox_execution_times = [data[2] for data in query_data]
    opt1_execution_times = [data[3] for data in query_data]
    opt2_execution_times = [data[4] for data in query_data]
    combined_execution_times = [data[5] for data in query_data]

    # Calculate overall average execution times
    all_vanilla_avg = np.mean([query_times[q]['vanilla'] for q in query_times.keys()])
    all_velox_avg = np.mean([query_times[q]['velox'] for q in query_times.keys()])
    all_opt1_avg = np.mean([query_times[q]['Spark+Velox+Opt1'] for q in query_times.keys()])
    all_opt2_avg = np.mean([query_times[q]['Spark+Velox+Opt2'] for q in query_times.keys()])
    all_combined_avg = np.mean([query_times[q]['Spark+Velox+Opt1+Opt2'] for q in query_times.keys()])
    
    overall_averages = {
        'vanilla': all_vanilla_avg,
        'velox': all_velox_avg,
        'opt1': all_opt1_avg,
        'opt2': all_opt2_avg,
        'combined': all_combined_avg
    }

    # Create the plot
    fig, ax = plt.subplots(figsize=(32, 16))
    bar_width = 0.3  # Width of the bars
    spacing = 0.05    # Increased space between query types
    
    # Calculate positions for bars
    positions = []
    type_centers = []
    current_x = 0
    
    for query_type, queries in query_types.items():
        type_start = current_x
        for i, query in enumerate(sorted([q for q in queries if q in ordered_queries], key=lambda x: ordered_queries.index(x))):
            positions.append(current_x)
            current_x += bar_width * 6  # 5 bars + space between queries
        
        current_x += spacing * 15  # Increased space after type group
        type_centers.append((type_start + current_x - spacing * 20) / 2)
    
    # Add one average bar at the end
    avg_position = current_x
    positions.append(avg_position)

    # Plot individual query bars
    query_positions = positions[:-1]  # Remove average position
    
    bars1 = ax.bar([p for p in query_positions], vanilla_execution_times, bar_width, 
                   label='Vanilla Spark', color='#354747')
    bars2 = ax.bar([p + bar_width for p in query_positions], velox_execution_times, bar_width, 
                   label='Spark+Velox', color='#02b0a8')
    bars3 = ax.bar([p + 2 * bar_width for p in query_positions], opt1_execution_times, bar_width, 
                   label='Spark+Velox+Opt1', color='#ff5d67')
    bars4 = ax.bar([p + 3 * bar_width for p in query_positions], opt2_execution_times, bar_width, 
                   label='Spark+Velox+Opt2', color='#f0c917')
    bars5 = ax.bar([p + 4 * bar_width for p in query_positions], combined_execution_times, bar_width, 
                   label='Spark+Velox+Opt1+Opt2', color='#9b59b6')

    # Plot single overall average bar
    avg_pos = positions[-1]
    ax.bar(avg_pos, overall_averages['vanilla'], bar_width, color='#354747')
    ax.bar(avg_pos + bar_width, overall_averages['velox'], bar_width, color='#02b0a8')
    ax.bar(avg_pos + 2 * bar_width, overall_averages['opt1'], bar_width, color='#ff5d67')
    ax.bar(avg_pos + 3 * bar_width, overall_averages['opt2'], bar_width, color='#f0c917')
    ax.bar(avg_pos + 4 * bar_width, overall_averages['combined'], bar_width, color='#9b59b6')

    # Set up the plot
    #ax.set_xlabel('Query Number', fontsize=40)
    ax.set_ylabel('Execution Time (seconds)', fontsize=48)
    
    # Create x-tick labels
    all_labels = sorted_query_numbers + ['Average']
    all_positions = [p + 2 * bar_width for p in query_positions] + [avg_pos + 2 * bar_width]
    
    ax.set_xticks(all_positions)
    ax.set_xticklabels(all_labels, rotation=90, ha='right', fontsize=48)
    ax.tick_params(axis='both', labelsize=35)
    
    ax.set_xlim(-bar_width, avg_pos + bar_width * 5.2)

    # Add horizontal lines under x-axis labels to group query types
    line_y_position = -ax.get_ylim()[1] * 0.1  # Position below the group labels
    
    for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
        # Calculate start and end positions for the group
        group_queries = [q for q in query_types[query_type] if q in ordered_queries]
        if group_queries:
            start_query_idx = ordered_queries.index(group_queries[0])
            end_query_idx = ordered_queries.index(group_queries[-1])
            
            start_x = query_positions[start_query_idx] - bar_width * 0.5
            end_x = query_positions[end_query_idx] + bar_width * 5.5
            
            # Draw horizontal line under the group
            ax.plot([start_x, end_x], [line_y_position, line_y_position], 
                   color='black', linewidth=2, alpha=0.7)
    
    # Add line under Average bar
    avg_start_x = avg_pos - bar_width * 0.5
    avg_end_x = avg_pos + bar_width * 5.5
    ax.plot([avg_start_x, avg_end_x], [line_y_position, line_y_position], 
           color='black', linewidth=2, alpha=0.7)
    
    # Add query type labels under x-axis (without frames)
    type_label_mapping = {
        'Hash Join': 'Hash Join',
        'Table Scan': 'Table Scan',
        'Hash Agg': 'Hash Agg',
        #'Low-cardinality Hash Aggregation': 'Low-Card HashAgg',
        'Sort': 'Sort'
    }
    
    for i, (query_type, (start_idx, end_idx)) in enumerate(type_positions.items()):
        center_x = type_centers[i]
        # Position text below the x-axis without frame
        ax.text(center_x, -ax.get_ylim()[1] * 0.13, type_label_mapping[query_type], 
               ha='center', va='top', fontsize=48, fontweight='bold', 
               rotation=0)  # Add slight rotation for better readability

    ax.legend(fontsize=36, loc='upper right')
    ax.set_ylim(0, 200)
    ax.grid(True, axis='y', linestyle='-', alpha=0.3, color='gray')
    ax.set_axisbelow(True)

    # Calculate and print statistics
    velox_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if velox_execution_times[i] < vanilla_execution_times[i])
    opt1_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if opt1_execution_times[i] < vanilla_execution_times[i])
    opt2_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if opt2_execution_times[i] < vanilla_execution_times[i])
    combined_speedup_count = sum(1 for i in range(len(vanilla_execution_times)) if combined_execution_times[i] < vanilla_execution_times[i])

    total_vanilla_time = sum(vanilla_execution_times)
    total_velox_time = sum(velox_execution_times)
    total_opt1_time = sum(opt1_execution_times)
    total_opt2_time = sum(opt2_execution_times)
    total_combined_time = sum(combined_execution_times)

    summary_text = (f'Queries with Velox Speedup: {velox_speedup_count}/{len(vanilla_execution_times)}\n'
                    f'Queries with Opt1 Speedup: {opt1_speedup_count}/{len(vanilla_execution_times)}\n'
                    f'Queries with Opt2 Speedup: {opt2_speedup_count}/{len(vanilla_execution_times)}\n'
                    f'Queries with Combined Speedup: {combined_speedup_count}/{len(vanilla_execution_times)}\n'
                    f'Total Vanilla Spark Time: {total_vanilla_time:.2f} seconds\n'
                    f'Total Spark+Velox Time: {total_velox_time:.2f} seconds\n'
                    f'Total Spark+Velox+Opt1 Time: {total_opt1_time:.2f} seconds\n'
                    f'Total Spark+Velox+Opt2 Time: {total_opt2_time:.2f} seconds\n'
                    f'Total Spark+Velox+Opt1+Opt2 Time: {total_combined_time:.2f} seconds')

    print(summary_text)
    print()
    
    # Print overall average speedups
    print("Overall average speedups (over Velox):")
    overall_opt1_speedup = overall_averages['velox'] / overall_averages['opt1']
    overall_opt2_speedup = overall_averages['velox'] / overall_averages['opt2']
    overall_combined_speedup = overall_averages['velox'] / overall_averages['combined']
    
    print(f"  Overall Opt1 avg speedup: {overall_opt1_speedup:.2f}x")
    print(f"  Overall Opt2 avg speedup: {overall_opt2_speedup:.2f}x")
    print(f"  Overall Combined avg speedup: {overall_combined_speedup:.2f}x")

    plt.tight_layout()
    plt.savefig("query_top20_performance_comparison_opt_enhanced.pdf", bbox_inches='tight', dpi=300)
    plt.show()

if __name__ == "__main__":
    main()