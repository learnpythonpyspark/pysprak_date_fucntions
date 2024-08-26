import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from matplotlib.dates import DateFormatter

# Set the style and color palette
plt.style.use('ggplot')  # Using a more widely available style
sns.set_palette("Set2")

# Generate sample data
np.random.seed(42)
dates = pd.date_range(start='2024-01-01', periods=21)
unique_party_count = np.random.randint(1000, 2000, 21)
unique_feature_count = np.random.randint(50, 150, 21)

# Create DataFrame
df = pd.DataFrame({
    'date': dates,
    'unique_party_count': unique_party_count,
    'unique_feature_count': unique_feature_count
})

# Create the plot
fig, ax1 = plt.subplots(figsize=(16, 9))
fig.patch.set_facecolor('#F0F0F0')
ax1.set_facecolor('#F8F8F8')

# Plot unique party count
color1 = sns.color_palette("Set2")[0]
ax1.plot(df['date'], df['unique_party_count'], color=color1, linewidth=3, marker='o', markersize=8, label='Unique Party IDs')
ax1.fill_between(df['date'], df['unique_party_count'], alpha=0.3, color=color1)
ax1.set_xlabel('Date', fontsize=14, fontweight='bold')
ax1.set_ylabel('Unique Party Count', color=color1, fontsize=14, fontweight='bold')
ax1.tick_params(axis='y', labelcolor=color1, labelsize=12)

# Create a second y-axis
ax2 = ax1.twinx()

# Plot unique feature count
color2 = sns.color_palette("Set2")[1]
ax2.plot(df['date'], df['unique_feature_count'], color=color2, linewidth=3, marker='s', markersize=8, label='Unique Features')
ax2.fill_between(df['date'], df['unique_feature_count'], alpha=0.3, color=color2)
ax2.set_ylabel('Unique Feature Count', color=color2, fontsize=14, fontweight='bold')
ax2.tick_params(axis='y', labelcolor=color2, labelsize=12)

# Add title
plt.title('Trends of Unique Party IDs and Features Over Time', fontsize=20, fontweight='bold', pad=20)

# Format the date on x-axis
ax1.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
fig.autofmt_xdate()  # Rotate and align the tick labels

# Add annotations for max values
max_party = df['unique_party_count'].max()
max_feature = df['unique_feature_count'].max()
max_party_date = df.loc[df['unique_party_count'].idxmax(), 'date']
max_feature_date = df.loc[df['unique_feature_count'].idxmax(), 'date']

ax1.annotate(f'Max Party IDs: {max_party}', 
             xy=(max_party_date, max_party), xytext=(10, 10),
             textcoords='offset points', ha='left', va='bottom',
             bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
             arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))

ax2.annotate(f'Max Features: {max_feature}', 
             xy=(max_feature_date, max_feature), xytext=(10, -10),
             textcoords='offset points', ha='left', va='top',
             bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
             arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'))

# Enhance the grid
ax1.grid(True, linestyle='--', alpha=0.7)

# Add legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12, 
           bbox_to_anchor=(0.02, 0.98), frameon=True, facecolor='white', edgecolor='gray')

# Enhance the overall look
plt.tight_layout()

# Show the plot
plt.show()
