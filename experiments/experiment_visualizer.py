import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
data = pd.read_csv('experiments/binary_vs_btree_results.csv')

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(data['Data Size (MB)'], data['Binary Search Throughput (queries/sec)'], label='Binary Search', marker='o')
plt.plot(data['Data Size (MB)'], data['B-Tree Throughput (queries/sec)'], label='B-Tree', marker='s')
plt.xlabel('Data Size (MB)')
plt.ylabel('Query Throughput (queries/sec)')
plt.title('Binary Search vs. B-Tree Query Throughput')
plt.legend()
plt.grid()
plt.savefig('report/query_throughput_comparison.png')
plt.show()
