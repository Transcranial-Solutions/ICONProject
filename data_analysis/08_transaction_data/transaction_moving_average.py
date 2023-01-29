#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 28 18:23:21 2023

@author: tono
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
import seaborn as sns
from datetime import date, datetime, timedelta
from scipy.interpolate import interp1d


dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(projectPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)

    
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")
days_to_plot = 365

def x_days_ago(doi = "2021-08-20", x_days='180'):
    x_days_ago = datetime.fromisoformat(doi) - timedelta(x_days)
    return x_days_ago.strftime('%Y-%m-%d')

starting_date = x_days_ago(date_today, days_to_plot)

df_tx = pd.read_csv(os.path.join(resultPath, 'compiled_tx_summary_date.csv'), low_memory=False)

df_tx['Regular & Interal Txs'] = df_tx['Regular Tx'] + df_tx['Internal Tx']
df_tx['Regular & Interal Txs (MA7)'] = df_tx['Regular & Interal Txs'].rolling(window=7).mean()
df_tx['Regular & Interal Txs (MA30)'] = df_tx['Regular & Interal Txs'].rolling(window=30).mean()

df_tx['Regular Tx (MA7)'] = df_tx['Regular Tx'].rolling(window=7).mean()
df_tx['Regular Tx (MA30)'] = df_tx['Regular Tx'].rolling(window=30).mean()

df_tx['Internal Tx (MA7)'] = df_tx['Internal Tx'].rolling(window=7).mean()
df_tx['Internal Tx (MA30)'] = df_tx['Internal Tx'].rolling(window=30).mean()

df_tx['Internal Event (MA7)'] = df_tx['Internal Event (excluding Tx)'].rolling(window=7).mean()
df_tx['Internal Event (MA30)'] = df_tx['Internal Event (excluding Tx)'].rolling(window=30).mean()

df_tx['Fees burned (MA7)'] = df_tx['Fees burned'].rolling(window=7).mean()
df_tx['Fees burned (MA30)'] = df_tx['Fees burned'].rolling(window=30).mean()

df_tx = df_tx[df_tx['date'] >= starting_date]

# plt.plot(df_tx['date'], df_tx['Regular Tx'])
# plt.plot(df_tx['date'], df_tx['Regular Tx (MA7)'])
# plt.plot(df_tx['date'], df_tx['Regular Tx (MA30)'])
# plt.plot(df_tx['date'], df_tx['Internal Tx'])

# # plt.plot(df_tx['date'], df_tx['Internal Tx (MA7)'])
# plt.plot(df_tx['date'], df_tx['Internal Tx (MA30)'])
# # plt.plot(df_tx['date'], df_tx['Internal Event (MA7)'])
# plt.plot(df_tx['date'], df_tx['Internal Event (MA30)'])
df_tx = df_tx.reset_index(drop=True)

plt.plot(df_tx['date'], df_tx['Regular & Interal Txs'])
plt.plot(df_tx['date'], df_tx['Regular & Interal Txs (MA7)'])
plt.plot(df_tx['date'], df_tx['Regular & Interal Txs (MA30)'])

# plt.plot(df_tx['date'], df_tx['Fees burned'])
plt.plot(df_tx['date'], df_tx['Fees burned (MA7)'])
plt.plot(df_tx['date'], df_tx['Fees burned (MA30)'])

sns.set(style="dark")
plt.style.use("dark_background")
lines = plt.plot(df_tx['date'], df_tx['Fees burned (MA7)'], marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black', markersize=8)



sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='date', y='Fees burned (MA7)', data=df_tx, palette=sns.color_palette('husl', n_colors=2))
h,l = ax.get_legend_handles_labels()


plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
# ax.legend(h[1:],l[1:],ncol=1,
#           # title="Voting Activity Stagnation",
#           fontsize=10,
#           loc='upper left')
n = 10  # Keeps every n-th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()
