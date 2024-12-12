import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
operations = ["put", "get", "scan"]
for operation in operations:
    filename = 'step3' + operation + '.csv'
    data = pd.read_csv(filename)
    print(data)

    x = data['Key']
    if operation == "put":
        y = data['Key'] / data['Value']
    else:
        y = 1 / data['Value']
    # Create the plot
    plt.figure(figsize=(8, 6))  # Optional: Adjust figure size
    plt.plot(range(len(x)), y, label="Memtable Size: 1 MB", marker='o', color='b')  # Line with points

    # Add labels, title, and legend
    plt.xlabel("Data Size")
    plt.ylabel("Throughput (MB/Sec)")
    plt.title(operation +" throughput over different data sizes")
    plt.legend()

    plt.xticks(ticks=range(len(x)), labels=x)

    # Show the grid
    # plt.grid(True)

    # Save the plot to a file
    output_file = "step2" + operation + ".png"  # Replace with your desired file name and format
    plt.savefig(output_file, dpi=300, bbox_inches='tight')  # High resolution and tight bounding box

    # Optional: Close the figure to release memory
    plt.close()