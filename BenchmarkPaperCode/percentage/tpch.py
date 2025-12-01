import pandas as pd
import matplotlib.pyplot as plt
import re

# Directly define the data from the CSV file
csv_data = {
    'Query': ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12', 'Q13', 'Q14', 'Q15', 'Q16', 'Q17', 'Q18', 'Q19', 'Q20', 'Q21', 'Q22'],
    'Scan+Filter': [426.0, 66.0, 390.0, 330.0, 346.8, 45.3, 120.4, 78.1, 90.0, 134.2, 230.1, 56.4, 42.3, 78.8, 89.6, 123.5, 87.4, 65.3, 34.7, 110.9, 95.6, 130.4],
    'HashJoin': [None, 12.6, 81.0, 84.0, 64.9, 22.1, 18.5, None, 45.3, 32.1, 60.7, None, 50.2, 40.4, None, 72.5, 80.1, None, 35.4, None, 78.3, 67.8],
    'HashAggregate': [32.9, 29.3, 12.0, 0.8, 0.4, 5.7, 9.8, 15.3, 20.1, 7.6, 8.5, 9.4, 6.5, 7.8, 12.3, 15.7, 10.9, 8.4, 9.6, 12.1, 11.2, 13.4],
    'Project': [None, 1.87, 10.20, 8.60, 17.20, 3.5, 4.3, 2.1, 5.7, 4.8, 6.2, 3.9, 2.6, 4.1, 5.3, 6.7, 3.8, 4.2, 3.1, 5.4, 6.3, 7.1],
    'Exchange': [None, 37.2, 176.5, 62.0, 369.2, 50.1, 40.3, 45.2, 60.5, 55.4, 66.7, 33.1, 22.4, 35.5, 40.8, 50.3, 45.2, 55.1, 30.4, 40.2, 50.1, 70.3],
    'Other': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1.6, None, None, None, None, None]
}

# Create a DataFrame
data = pd.DataFrame(csv_data)

# Calculate the total time spent on each operator across all queries
operator_totals = data.iloc[:, 1:].sum()

# Drop any operators with NaN totals
operator_totals = operator_totals.dropna()

# Create the pie chart
plt.figure(figsize=(10, 8))
plt.pie(
    operator_totals,
    labels=operator_totals.index,
    autopct="%1.1f%%",
    startangle=140
)
plt.title("TPCH SF=100 Spark+Velox 4 node clusters")
plt.axis('equal')  # Equal aspect ratio ensures the pie chart is circular
plt.show()
plt.savefig("./tpch.png")