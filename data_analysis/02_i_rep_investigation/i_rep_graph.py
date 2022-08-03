

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # for improving our visualizations
import matplotlib.lines as mlines
import seaborn as sns
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading/saving
currPath = os.getcwd()
inPath = 'E:\\Transcranial_Solutions\\Analysis\\i_rep_and_bond_requirement\\'


# read data
g1 = pd.read_csv(os.path.join(inPath, 'graph_1.csv'))
g2 = pd.read_csv(os.path.join(inPath, 'projection.csv'))
g3 = pd.read_csv(os.path.join(inPath, 'irep.csv'))


def add_error_and_sort(df, add_error=0, error=0):
    df = df.groupby(['Date']).mean().reset_index()

    if add_error == 1:
        df['Error'] = g1['Bond Requirement (%)'] * error
    else:
        pass

    df['date'] = pd.to_datetime(g1['Date']).dt.strftime("%Y-%m-%d")
    df = df.sort_values(by='date')
    return(df)

g1 = add_error_and_sort(g1, 1, 0.05)
g2 = add_error_and_sort(g2, 1, 0.15)
g3 = add_error_and_sort(g3)



sns.set(style="darkgrid")
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(8, 5))
ax = sns.lineplot(x="Date", y="irep", data=g3, color="red", sort=False)
# plt.grid(b=None)
ax.grid(False)

ax.set_ylabel('i_rep')
ax.set_ylim(0,)
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('Change in i_rep Over Time', fontsize=12, weight='bold')
ax.yaxis.grid(True)

n = 30 # Keeps every 7th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()


plt.savefig(os.path.join(inPath, "irep_change.png"))


sns.set(style="darkgrid")
## current
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(g1['Date'], g1['Bond Requirement (%)'], yerr=g1['Error'], align='center', alpha=0.5, ecolor='white', capsize=0)
ax.grid(False)
n = 15 # Keeps every 7th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
ax.set_ylabel('Bond Requirement (%)')
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('% of bond saved over time (6 months)', fontsize=12, weight='bold')
ax.yaxis.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(inPath, "bond_in_6_months.png"))


sns.set(style="darkgrid")

## projection
# sns.set(style="darkgrid")
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(16, 5))
ax.bar(g2['Date'], g2['Bond Requirement (%)'], yerr=g2['Error'], align='center', alpha=0.5, ecolor='white', capsize=0)
# ax.grid(False)
ax.grid(False)
n = 30
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
ax.set_ylabel('Bond Requirement (%)')
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('Linear projection earnings for 14k i_rep (1 year)', fontsize=12, weight='bold')
ax.yaxis.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(inPath, "bond_in_1_year.png"))


