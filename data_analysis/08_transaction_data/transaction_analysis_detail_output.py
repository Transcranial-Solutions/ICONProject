#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 13:55:02 2024

@author: tono
"""

from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import ticker
import matplotlib.lines as mlines
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet
from matplotlib.ticker import FuncFormatter
import networkx as nx
from matplotlib import cm


# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')
walletPath = Path(projectPath).joinpath("wallet")
tokentransferPath = Path(dailyPath).joinpath("10_token_transfer/results/")

walletPath.mkdir(parents=True, exist_ok=True)

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail_with_group*.csv')])


# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 0 #0
date_prev = "2024-07-20"

day_1 = "2024-04-20" #07
day_2 = "2024-07-22"

def yesterday(doi = "2021-08-20"):
    yesterday = datetime.fromisoformat(doi) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20"):
    tomorrow = datetime.fromisoformat(doi) + timedelta(1)
    return tomorrow.strftime('%Y-%m-%d')

# today's date
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")


if use_specific_prev_date == 1:
    date_of_interest = [date_prev]
elif use_specific_prev_date == 0:
    date_of_interest = [yesterday(date_today)]
elif use_specific_prev_date == 2:
    # for loop between dates
    # day_1 = "2024-07-14"; day_2 = "2024-07-20"
    date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()
else:
    date_of_interest=[]
    print('No date selected.')

print(date_of_interest)

matching_paths = [path for path in tx_detail_paths if any(date in path.name for date in date_of_interest)]


def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

def hex_to_int(val: str):
    try:
        return int(val, 0)
    except ValueError:
        print(f"failed to convert {val} to int")
        return float("NAN")

def parse_icx(val: str):
    try:
        return loop_to_icx(int(val, 0))
    except ZeroDivisionError:
        return float("NAN")
    except ValueError:
        return float("NAN")


# get iiss info
def get_iiss_info(walletPath):
    tester_wallet = walletPath.joinpath("test_keystore_1")

    if tester_wallet.exists():
        wallet = KeyWallet.load(str(tester_wallet), "abcd1234*")
    else:
        wallet = KeyWallet.create()
        wallet.get_address()
        wallet.get_private_key()
        wallet.store(str(tester_wallet), "abcd1234*")

    # Get the tester address
    tester_address = wallet.get_address()

    # Define the system address
    SYSTEM_ADDRESS = "cx0000000000000000000000000000000000000000"
    
    icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

    
    call = CallBuilder().from_(tester_address) \
        .to(SYSTEM_ADDRESS) \
        .method("getIISSInfo") \
        .build()
    result = icon_service.call(call)['variable']

    df = {'Icps': hex_to_int(result['Icps']), 
          'Iglobal': parse_icx(result['Iglobal']),
          'Iprep': hex_to_int(result['Iprep']),
          'Irelay': hex_to_int(result['Irelay']), 
          'Iwage': hex_to_int(result['Iwage'])}

    return df



def get_tx_group_with_fees(df, in_group): 
    df_subset = df[df['p2p'] | df['p2c'] | df['c2p'] | df['c2c']].reset_index(drop=True)
    
    conditions = [df_subset['p2p'], df_subset['p2c'], df_subset['c2p'], df_subset['c2c']]
    choices = ['hx -> hx', 'hx -> cx', 'cx -> hx', 'cx -> cx']
    
    df_subset.loc[:, 'tx_mode'] = np.select(conditions, choices, default=np.nan)
    
    tx_count = df_subset[in_group].value_counts().rename('count')
    tx_fees = df_subset[df_subset['tx_type'] == 'main'].groupby(in_group)['tx_fees'].sum().rename('fees')
    
    tx_with_fees = pd.concat([tx_fees, tx_count], axis=1).sort_values(by=['fees', 'count'], ascending=False)
    tx_with_fees['fees'] = np.where(tx_with_fees['fees'].isna(), 0, tx_with_fees['fees'])
    tx_with_fees = tx_with_fees.reset_index()
    tx_with_fees = tx_with_fees.rename(columns={'index': 'group'})    

    # tx_count_from = df_subset['from_label_group'].value_counts().rename('count_from')
    # tx_count_to = df_subset['to_label_group'].value_counts().rename('count_to')
    # tx_count = pd.concat([tx_count_from, tx_count_to], axis=1)
    
    # fees_of_tx_count_from = df_subset[df_subset['tx_type'] == 'main'].groupby('from_label_group')['tx_fees'].sum().rename('fees_from')
    # fees_of_tx_count_to = df_subset[df_subset['tx_type'] == 'main'].groupby('to_label_group')['tx_fees'].sum().rename('fees_to')
    # fees_of_tx_count = pd.concat([fees_of_tx_count_from, fees_of_tx_count_to], axis=1)

    # tx_with_fees = pd.concat([fees_of_tx_count, tx_count], axis=1).sort_values(by=['fees_from', 'fees_to', 'count_to'], ascending=False)
    # tx_with_fees['fees_from'] = np.where(tx_with_fees['fees_from'].isna(), 0, tx_with_fees['fees_from'])
    # tx_with_fees['fees_to'] = np.where(tx_with_fees['fees_to'].isna(), 0, tx_with_fees['fees_to'])
    # tx_with_fees = tx_with_fees.reset_index()
    # tx_with_fees = tx_with_fees.rename(columns={'index': 'group'})    
    return tx_with_fees


def get_agg_df_for_count_fees_and_value(df):
    # Filter the subset of data based on the boolean columns
    df_subset = df[df['p2p'] | df['p2c'] | df['c2p'] | df['c2c']].reset_index(drop=True)
    df_subset['TxCount'] = df_subset['regTxCount'] + df_subset['intTxCount']
    
    df_subset.loc[df_subset['tx_type'] != 'main', 'tx_fees'] = 0
    df_subset.loc[df_subset['tx_type'] != 'main', 'value'] = 0
    
    # Aggregate transaction counts and fees by from_label_group and to_label_group
    df_agg = df_subset.groupby(['from_label_group', 'to_label_group']).agg({
        'TxCount': 'sum',
        'tx_fees': 'sum',
        'value': 'sum'
    }).reset_index()
    return df_agg


daily_issuance = get_iiss_info(walletPath)['Iglobal']*12/365

for tx_path in tqdm(matching_paths):
    tx_path_date = tx_path.stem.split('_')[-1]
    df = pd.read_csv(tx_path, low_memory=False)
    df.drop("Unnamed: 0", axis=1, inplace=True)
    
    df_agg = get_agg_df_for_count_fees_and_value(df)
    
    tx_path_date_underscore = tx_path_date.replace("-", "_")
    tokentransfer_date_Path = tokentransferPath.joinpath(tx_path_date_underscore, f'IRC_token_transfer_{tx_path_date_underscore}.csv')
    token_transfer_df = pd.read_csv(tokentransfer_date_Path, low_memory=False)
    
    try:
        icx_price = token_transfer_df[token_transfer_df['IRC Token'] == 'ICX']['Price in USD'].iloc[0]
        icx_transfer_value = df_agg['value'].sum() * icx_price



# df_temp = df[df['tx_type'] == 'main']
# df_temp['p2p'].sum() + df_temp['p2c'].sum() + df_temp['c2p'].sum() + df_temp['c2c'].sum()
# df_temp['regTxCount'].sum() + df_temp['intTxCount'].sum()





# tx_count_with_fees_by_mode = get_tx_group_with_fees(df, in_group=['to_label_group', 'tx_mode'])
# tx_count_with_fees_by_mode_from = get_tx_group_with_fees(df, in_group=['from_label_group', 'tx_mode'])


tx_count_with_fees_to = get_tx_group_with_fees(df, 'to_label_group')
tx_count_with_fees_from = get_tx_group_with_fees(df, 'from_label_group')



def visualise_tx_group_with_fees(df, tx_path_date, in_group='to_label_group', log_scale=False):
    sns.set(style="dark")
    plt.style.use("dark_background")

    tx_count_with_fees = get_tx_group_with_fees(df, in_group)
    
    contract_address_label = 'Destination' if in_group == 'to_label_group' else 'Source'
    
    total_activity_count = df['regTxCount'].sum() + df['intTxCount'].sum() + df['systemTickCount'].sum() + df['intEvtCount'].sum()
    total_activity_count_txt = f"Regular Tx: {df['regTxCount'].sum():,}; Internal Tx: {df['intTxCount'].sum():,}\nInternal Events: {df['intEvtCount'].sum():,}; System Ticks: {df['systemTickCount'].sum():,}"

    total_tx_count = tx_count_with_fees['count'].sum()
    total_fees = tx_count_with_fees['fees'].sum().round(2)
    total_tx_count_txt = f"Transactions (Fee-incurring): {int(total_tx_count):,}\nTotal Activity: {int(total_activity_count):,}"


    fig, ax1 = plt.subplots(figsize=(14, 8))

    # Bar plot for transaction counts
    tx_count_with_fees_sorted = tx_count_with_fees.sort_values(by=['fees','count'], ascending=False)
    tx_count_with_fees_sorted.plot(kind='bar', x='group', y='count', stacked=True, ax=ax1, color='palegoldenrod', legend=False)


    plt.title(f'Daily Transactions ({tx_path_date})', fontsize=14, weight='bold', pad=10, loc='left')
    ax1.set_xlabel(f'{contract_address_label} Contracts/Addresses')
    ax1.set_ylabel('Transactions', labelpad=10)
    ax1.set_xticklabels(tx_count_with_fees_sorted['group'], rotation=90, ha="center")

    # Line plot for transaction fees
    ax2 = ax1.twinx()
    ax2.plot(tx_count_with_fees_sorted['group'], tx_count_with_fees_sorted['fees'], marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black', markersize=8)
    ax2.set_ylabel('Fees burned (ICX)', labelpad=10)
    
    daily_burned_percentage = f"{tx_count_with_fees['fees'].sum()/daily_issuance:.2%}"
    fees_burned_label = f'Total Fees Burned ({total_fees} ICX / {daily_burned_percentage} of daily inflation)'

    # Adjust axis formatting
    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()

    if ymax >= 3_000:
        ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
    if ymax >= 1_000_000:
        ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))

    color = 'white'
    m_line = mlines.Line2D([], [], color=color, label=f'{fees_burned_label}', linewidth=1, marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black')
    leg = plt.legend(handles=[m_line], loc='upper right', fontsize='medium', bbox_to_anchor=(0.98, 0.999), frameon=False)
    for text in leg.get_texts():
        plt.setp(text, color='cyan')

    plt.tight_layout(rect=[0,0,1,1])

    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()
    
    ymax_scale_factor = 0.22 if log_scale else 0.82
    ax1.text(xmax*0.97, ymax*ymax_scale_factor, total_tx_count_txt,
             horizontalalignment='right',
             verticalalignment='center',
             linespacing=1.5,
             fontsize=12,
             weight='bold')

    handles, labels = ax1.get_legend_handles_labels()
    labels = [i.replace('count', 'Tx count') if 'count' in i else i for i in labels]
    ax1.legend(handles, labels, loc='upper right', bbox_to_anchor=(1, 1.08), frameon=False, fancybox=True, shadow=True, ncol=3)

    ax1.text(xmax*1.07, ymax*-0.12, total_activity_count_txt,
             horizontalalignment='right',
             verticalalignment='center',
             rotation=90,
             linespacing=1.5,
             fontsize=8)
    
    ax2.spines['right'].set_color('cyan')
    ax2.yaxis.label.set_color('cyan')
    ax2.tick_params(axis='y', colors="cyan")

    if log_scale:
        ax1.set_yscale('log')

    plt.show()


visualise_tx_group_with_fees(df, tx_path_date, in_group='to_label_group')
visualise_tx_group_with_fees(df, tx_path_date, in_group='from_label_group')



tx_count_with_fees_to = get_tx_group_with_fees(df, 'to_label_group')
tx_count_with_fees_from = get_tx_group_with_fees(df, 'from_label_group')







# # Filter the subset of data based on the boolean columns
# df_subset = df[df['p2p'] | df['p2c'] | df['c2p'] | df['c2c']].reset_index(drop=True)
# df_subset['TxCount'] = df_subset['regTxCount'] + df_subset['intTxCount']

# df_subset.loc[df_subset['tx_type'] != 'main', 'tx_fees'] = 0
# df_subset.loc[df_subset['tx_type'] != 'main', 'value'] = 0

# # Aggregate transaction counts and fees by from_label_group and to_label_group
# df_agg = df_subset.groupby(['from_label_group', 'to_label_group']).agg({
#     'TxCount': 'sum',
#     'tx_fees': 'sum',
#     'value': 'sum'
# }).reset_index()

# Create a directed graph
G = nx.DiGraph()

# Add edges to the graph
for index, row in df_agg.iterrows():
    G.add_edge(row['from_label_group'], row['to_label_group'], weight=row['TxCount'], fees=row['tx_fees'])

# Compute node sizes based on total fees
node_fees = df_agg.groupby('to_label_group')['tx_fees'].sum().to_dict()
max_fee = max(node_fees.values())

# Normalizing node sizes and applying a cap
# max_size_fees = 100  # Maximum node size
# node_sizes_fees = [min((node_fees.get(node, 1) / max_fee) * max_size_fees, max_size_fees) for node in G.nodes()]





node_values = df_agg.groupby('to_label_group')['value'].sum().to_dict()
max_value = max(node_values.values())

# Normalizing node sizes and applying a cap
max_size_values = 100_000  # Maximum node size
node_sizes_values = [min((node_values.get(node, 1) / max_value) * max_size_values, max_size_values) for node in G.nodes()]






# Compute edge colors based on transaction counts
edge_weights = np.array([d['weight'] for (u, v, d) in G.edges(data=True)])
cmap = cm.get_cmap('viridis')
edge_colors = [cmap(weight / max(edge_weights)) for weight in edge_weights]

# Use the Kamada-Kawai layout for better node distribution
pos = nx.circular_layout(G)

sns.set(style="dark")
plt.style.use("dark_background")
plt.figure(figsize=(20, 20))

# Draw nodes with sizes based on fees, semi-transparent and with border
nx.draw_networkx_nodes(G, pos, node_size=node_sizes_values, node_color='lightblue', alpha=0.2, edgecolors='black', linewidths=1.5)
# nx.draw_networkx_nodes(G, pos, node_size=max_size_fees, node_color='orange', alpha=0.2, edgecolors='orange', linewidths=1.5)

# Draw edges with colors based on transaction count and curved
edges = nx.draw_networkx_edges(G, pos, arrowstyle='-|>', arrowsize=10, edge_color=edge_colors,
                               width=2, connectionstyle='arc3,rad=0.1', alpha=0.6)

# Draw node labels with fees and values in different colors
for node, (x, y) in pos.items():
    node_text = node
    value_text = f"\n\n{int(node_values.get(node, 0)):,} ICX"
    fees_text = f"\n\n{int(node_fees.get(node, 0)):,} ICX"
    
    plt.text(x, y, node_text, fontsize=9, fontfamily='sans-serif', color='cyan', ha='center', va='center')
    plt.annotate(f"{value_text}", (x, y), fontsize=8, fontfamily='sans-serif', color='yellow', ha='center', va='center')
    plt.annotate(f"\n\n{fees_text}", (x, y), fontsize=8, fontfamily='sans-serif', color='orange', ha='center', va='center')

# Create a ScalarMappable for the colorbar
sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=min(edge_weights), vmax=max(edge_weights)))
sm.set_array([])
plt.colorbar(sm, label='Transaction Count')

plt.title('Transaction Network Graph', fontsize=14)
plt.text(0.62, 1.013, '(', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
plt.text(0.70, 1.013, 'Value Transferred ', color='yellow', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
plt.text(0.78, 1.013, '&', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
plt.text(0.84, 1.013, 'Fees Burned', color='orange', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
plt.text(0.90, 1.013, ')', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)

plt.show()
