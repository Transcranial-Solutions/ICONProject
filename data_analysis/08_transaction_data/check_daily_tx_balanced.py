#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 08:56:25 2025

@author: tono
"""
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import os
import glob
import re
import seaborn as sns
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.ticker as ticker
import matplotlib.lines as mlines
from tqdm import tqdm


# path
#currPath = os.getcwd()
currPath = '/home/tono/ICONProject/data_analysis/'
if not "08_transaction_data" in currPath:
    projectPath = os.path.join(currPath, "08_transaction_data")
    if not os.path.exists(projectPath):
        os.mkdir(projectPath)
else:
    projectPath = currPath

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)


csv_files = list(Path(dataPath).glob('*tx_detail_with_group_info*.csv'))


all_df =[]
for dat in tqdm(csv_files):
    print(dat)
    df = pd.read_csv(dat, low_memory=False)
    df = df[['from','to','tx_date']]
    all_df.append(df)


df = pd.concat(all_df)
melted = df.melt(id_vars=['tx_date'], value_vars=['from', 'to'], value_name='new_column')
filtered_df = melted[melted['new_column'].str.startswith('hx')]
df = filtered_df.rename(columns = {'tx_date': 'date', 'new_column': 'wallet'}).reset_index(drop=True)
df = df.drop(columns='variable')



df['date'] = pd.to_datetime(df['date'])
ucount_per_day = df.groupby(['date'])['wallet'].nunique()


how_many_months = 6
end_date = df['date'].max()
start_date = end_date - pd.DateOffset(months=how_many_months)

df_last_x_months = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

unique_counts = df_last_x_months.groupby('date')['wallet'].nunique()




moving_avg = unique_counts.rolling(window=7).mean()
average_unique_counts = unique_counts.mean()


plt.figure(figsize=(14, 7))
plt.bar(unique_counts.index, unique_counts.values, color='skyblue', label='Unique Active Wallets')
plt.plot(moving_avg.index, moving_avg.values, color='r', linestyle='--', linewidth=2, label='7-Day Moving Average')
plt.title(f'Unique Active Wallets per Day (Last {how_many_months} Months)')
plt.xlabel('Date')
plt.ylabel('Number of Unique Active Wallets')
plt.grid(True, linestyle='--', alpha=0.7)

plt.axhline(y=average_unique_counts, color='green', linestyle='--', linewidth=1.5, label=f'Average: {average_unique_counts:.0f}')
plt.text(x=unique_counts.index[-1], y=average_unique_counts, s=f'Average: {average_unique_counts:.0f}', color='green', va='bottom', ha='right', fontsize=10)

plt.xticks(ticks=unique_counts.index[::10], rotation=45)
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.tight_layout(rect=[0, 0, 0.95, 1])
plt.show()