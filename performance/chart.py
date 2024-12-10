import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
script_dir = os.path.dirname(os.path.abspath(__file__))

# Read CSV files using script directory
local_df = pd.read_csv(os.path.join(script_dir, 'local_results.csv'))
pull_df = pd.read_csv(os.path.join(script_dir, 'pull_results.csv'))
push_df = pd.read_csv(os.path.join(script_dir, 'push_results.csv'))

# Create figure with 3 subplots
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 5))

# Plot Local Results
tasks = [5, 10, 20, 40]  
workers = [1, 2, 4, 8]   
ax1.plot(tasks, local_df['time_taken'], marker='o', label='Latency')

# Add worker count annotations
for task, worker, latency in zip(tasks, workers, local_df['time_taken']):
    ax1.annotate(f'{worker} Wks', 
                xy=(task, latency),
                xytext=(5, 5),  
                textcoords='offset points')

# Set y-axis limits to reflect smoothed data
mean_latency = local_df['time_taken'].mean()
ax1.set_ylim([mean_latency - 1, mean_latency + 1])  
ax1.set_xticks(tasks)  
ax1.set_xlabel('Number of Tasks / Throughput')
ax1.set_ylabel('Latency (seconds)')
ax1.set_title('Local Performance')
ax1.grid(True)
ax1.legend()

# Plot Pull Results
tasks = [5, 10, 20, 40]  
workers = [1, 2, 4, 8]   
for proc in [1, 2, 4, 8]:
    data = pull_df[pull_df['num_processor'] == proc]
    ax2.plot(tasks, data['time_taken'], marker='o', label=f'{proc} processors')
    for task, worker, latency in zip(tasks, workers, data['time_taken']):
        ax2.annotate(f'{worker} Wks', 
                    xy=(task, latency),
                    xytext=(5, 5),
                    textcoords='offset points')

ax2.set_xticks(tasks)  
ax2.set_xlabel('Number of Tasks / Throughput')
ax2.set_ylabel('Latency (seconds)')
ax2.set_title('Pull Model Performance')
ax2.grid(True)
ax2.legend()

# Plot Push Results
for proc in [1, 2, 4, 8]:
    data = push_df[push_df['num_processor'] == proc]
    ax3.plot(tasks, data['time_taken'], marker='o', label=f'{proc} processors')
    for task, worker, latency in zip(tasks, workers, data['time_taken']):
        ax3.annotate(f'{worker} Wks', 
                    xy=(task, latency),
                    xytext=(5, 5),
                    textcoords='offset points')

ax3.set_xticks(tasks)  
ax3.set_xlabel('Number of Tasks / Throughput')
ax3.set_ylabel('Latency (seconds)')
ax3.set_title('Push Model Performance')
ax3.grid(True)
ax3.legend()

# Display the plot
plt.tight_layout()
plt.show()