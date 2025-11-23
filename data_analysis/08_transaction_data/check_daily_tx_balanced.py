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
import numpy as np
import os
import glob
import re
import seaborn as sns
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.ticker as ticker
import matplotlib.lines as mlines
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

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


# csv_files = sorted(list(Path(dataPath).glob('*tx_detail_with_group_info*.csv')))
csv_files = sorted(list(Path(dataPath).glob('*tx_detail_with_group_info_2025*.csv')))

# csv_files = sorted(list(Path(dataPath).glob('*tx_detail_2025*.csv')))



# =============================================================================
# Get balanced contract addresses
# =============================================================================
url = 'https://github.com/balancednetwork/balanced-java-contracts/wiki/Contract-Addresses'

response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    list_items = soup.find_all('li')

    balanced_contract_group = {} 
    balanced_contract_group_grouped = {}

    pattern = re.compile(r'"([^"]+)"\s*[:\-]?\s*("?(cx[a-fA-F0-9]+)"?)')

    for item in list_items:
        text = item.get_text(strip=True)

        match = pattern.search(text)
        if match:
            contract_name = match.group(1).strip()
            contract_address = match.group(2).strip('"').strip()

            balanced_contract_group[contract_name] = contract_address
            balanced_contract_group_grouped[contract_address] = 'Balanced'



all_df =[]
for dat in tqdm(csv_files):
    df = pd.read_csv(dat, low_memory=False)
    df['to_balanced_only'] = df['to'].map(balanced_contract_group_grouped)
    df['from_balanced_only'] = df['from'].map(balanced_contract_group_grouped)
    
    if 'to_label_group' in df.columns:
        df['to_label_group_new'] = np.where( (df['to_label_group'] != 'Balanced') & (df['to_balanced_only'] == 'Balanced' ), 'Balanced', df['to_label_group'])
        df['from_label_group_new'] = np.where( (df['from_label_group'] != 'Balanced') & (df['from_balanced_only'] == 'Balanced' ), 'Balanced', df['from_label_group'])
        df_subset = df[(df['to_label_group_new'] == 'Balanced') | (df['from_label_group_new'] == 'Balanced')].reset_index(drop=True)
        
    else:
        df_subset = df
    
    data_date = dat.stem.split('_')[-1]
    df_subset['date'] = data_date
    
    conditions = [df_subset['p2p'], df_subset['p2c'], df_subset['c2p'], df_subset['c2c']]
    choices = ['hx -> hx', 'hx -> cx', 'cx -> hx', 'cx -> cx']
    
    df_subset.loc[:, 'tx_mode'] = np.select(conditions, choices, default=np.nan)
    
    df_subset['tx_mode'] = np.where(df_subset['intEvtCount']==1, 'IntEvent', df_subset['tx_mode'])
    
    df_final = df_subset[['date', 'tx_mode']]
    all_df.append(df_final)


df = pd.concat(all_df).reset_index(drop=True)


df['tx_mode'] = np.where(df['tx_mode'] == 'hx -> hx', 'hx -> cx', df['tx_mode'])

# =============================================================================
# Plot
# =============================================================================
start_date = '2025-8-20'
end_date = '2025-11-20'

df['date'] = pd.to_datetime(df['date'])

df_main = df[df['tx_mode'] != 'IntEvent']
df_intevent = df[df['tx_mode'] == 'IntEvent']

df_main_grouped = df_main.groupby(['date', 'tx_mode']).size().unstack(fill_value=0)
df_intevent_grouped = df_intevent.groupby('date').size()

mask = (df_main_grouped.index >= start_date) & (df_main_grouped.index <= end_date)
df_main_grouped = df_main_grouped.loc[mask]
df_intevent_grouped = df_intevent_grouped.loc[start_date:end_date]

df_main_grouped = df_main_grouped.sort_index()
df_intevent_grouped = df_intevent_grouped.sort_index()

df_intevent_grouped = df_intevent_grouped.reindex(df_main_grouped.index, fill_value=0)

fig, ax = plt.subplots(figsize=(12, 6))

bottom = None
for tx_mode in df_main_grouped.columns:
    ax.bar(df_main_grouped.index, df_main_grouped[tx_mode],
           bottom=bottom, label=tx_mode)
    if bottom is None:
        bottom = df_main_grouped[tx_mode].values
    else:
        bottom += df_main_grouped[tx_mode].values

ax.bar(df_intevent_grouped.index, df_intevent_grouped,
       bottom=bottom, color='grey', alpha=0.3, label='IntEvent')

avg_tx_per_day = df_main_grouped.sum(axis=1).mean()
avg_tx_per_day_with_intevent = (df_main_grouped.sum(axis=1) + df_intevent_grouped).mean()

ax.set_xlabel('Date')
ax.set_ylabel('Transaction Count')
ax.set_title('Balanced Transaction Count')
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Transaction Mode")
avg_text = f"Avg transactions per day: {int(avg_tx_per_day)} ({int(avg_tx_per_day_with_intevent)} incl. IntEvent)"
ax.text(0.02, 0.95, avg_text, transform=ax.transAxes, fontsize=12, verticalalignment='top')
ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=10))
plt.tight_layout()  
plt.show()
