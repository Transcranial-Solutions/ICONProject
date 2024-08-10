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
import ast


# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')
walletPath = Path(projectPath).joinpath("wallet")
tokentransferPath = Path(dailyPath).joinpath("10_token_transfer/results/")
tokenaddressPath = Path(dailyPath).joinpath("wallet_addresses")

walletPath.mkdir(parents=True, exist_ok=True)

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail_with_group*.csv')])
tx_block_paths = sorted([i for i in dataPath.glob('transaction_blocks*.csv')])

# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 0 #0
date_prev = "2024-08-07"

day_1 = "2023-07-01" #07
day_2 = "2024-08-08"

def yesterday(doi = "2021-08-20", delta=1):
    yesterday = datetime.fromisoformat(doi) - timedelta(delta)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20", delta=1):
    tomorrow = datetime.fromisoformat(doi) + timedelta(delta)
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

def int_to_hex(val: int) -> str:
    """
    Converts an integer to a hexadecimal string.
    
    Parameters:
        val (int): The integer value to be converted to hex.

    Returns:
        str: The hexadecimal string representation of the integer.
    """
    try:
        return hex(val).lower()
    except TypeError:
        print(f"failed to convert {val} to hex")
        return "NAN"

def parse_icx(val: str):
    try:
        return loop_to_icx(int(val, 0))
    except ZeroDivisionError:
        return float("NAN")
    except ValueError:
        return float("NAN")

def load_from_json(filename):
    """
    Load dictionary data from a JSON file.
    """
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
    return data

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


def get_balanced_burned(block_height):
    # ctz_solidwallet = 'https://ctz.solidwallet.io/api/v3'
    local_endpoint = "http://127.0.0.1:9000/api/v3"
    
    icon_service = IconService(HTTPProvider(local_endpoint))
    contract_address = "cxdc30a0d3a1f131565c071272a20bc0b06fd4c17b"
    
    # block_height = "0x50d14d2"  
    
    call = CallBuilder()\
        .from_("hx0000000000000000000000000000000000000000")\
        .to(contract_address)\
        .method("getBurnedAmount")\
        .height(block_height)\
        .build()
    try:
        result = icon_service.call(call)
        print(f"Burned Amount: {result}")
    except:
        result = '0x0'
    
    return loop_to_icx(hex_to_int(result))

def get_balanced_burned_amount(date_of_interest, tx_block_paths):
    
    def calculate_differences(data_dict):
        sorted_dates = sorted(data_dict.keys(), key=lambda date: datetime.strptime(date, '%Y-%m-%d'))
        
        differences = {}
        
        previous_value = data_dict[sorted_dates[0]]
        for i, date in enumerate(sorted_dates):
            if i == 0:
        
                differences[date] = 0
            else:
                current_value = data_dict[date]
                differences[date] = current_value - previous_value
                previous_value = current_value
    
        if sorted_dates:
            differences.pop(sorted_dates[0])
    
        return differences

    date_of_interest = [date_of_interest] if not isinstance(date_of_interest, list) else date_of_interest
    date_of_interest_with_previous = [yesterday(date_of_interest[0])] + date_of_interest    
    matching_paths = [path for path in tx_block_paths if any(date in str(path) for date in date_of_interest_with_previous)]
    
    balanced_burned_collector = {}
    for matching_path in matching_paths:
        date_to_check = matching_path.stem.split('_')[-1]
        balanced_burned_collector[date_to_check] = {}
        
        df = pd.read_csv(matching_path)
        block_height = df['blockHeight'].iloc[0]
        
        block_height_hex = int_to_hex(block_height)
        balanced_burned_icx = get_balanced_burned(block_height_hex)
        balanced_burned_collector[date_to_check] = balanced_burned_icx
    
    return calculate_differences(balanced_burned_collector)
    

def find_token_transfer_path(base_path, date_str):
    # Convert the date format to underscore
    date_str_underscore = date_str.replace("-", "_")
    
    # Create a search pattern
    search_pattern = f'IRC_token_transfer_{date_str_underscore}.csv'
    
    # Search through possible directory depths
    for depth in range(1, 3):  # Searching up to 2 levels deep
        pattern = f"{'*/' * depth}{search_pattern}"
        for sub_path in base_path.glob(pattern):
            # Check if the file exists
            if sub_path.is_file():
                return sub_path

    return None


def compute_tx_group(df, from_label='from_label_group', to_label='to_label_group'):
    """
    Computes the 'tx_group' column based on the conditions specified, prioritizing non-NaN values.

    Parameters:
        df (pd.DataFrame): The DataFrame containing transaction data with 'from_label_group' and 'to_label_group' columns.
        from_label (str): The name of the column representing the from label group.
        to_label (str): The name of the column representing the to label group.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'tx_group' column.
    """

    # Initialize the 'tx_group' column with NaN
    df['tx_group'] = np.nan

    # Condition 1: Prefer non-NaN 'from_label' over 'to_label' if one of them is unknown
    mask_from_known = df[from_label].notna() & (df[to_label].isna() | df[to_label].str.startswith('unknown'))
    df.loc[mask_from_known, 'tx_group'] = df[from_label]

    mask_to_known = df[to_label].notna() & (df[from_label].isna() | df[from_label].str.startswith('unknown'))
    df.loc[mask_to_known, 'tx_group'] = df[to_label]

    # Condition 2: If both are unknown, prioritize 'unknown_cx' over 'unknown_hx'
    mask_both_unknown = df[from_label].str.startswith('unknown') & df[to_label].str.startswith('unknown')
    df.loc[mask_both_unknown & (df[from_label] == 'unknown_cx'), 'tx_group'] = 'unknown_cx'
    df.loc[mask_both_unknown & (df[to_label] == 'unknown_cx'), 'tx_group'] = 'unknown_cx'
    df.loc[mask_both_unknown & (df[from_label] == 'unknown_hx'), 'tx_group'] = 'unknown_hx'
    df.loc[mask_both_unknown & (df[to_label] == 'unknown_hx'), 'tx_group'] = 'unknown_hx'

    # Condition 3: If one of them is NaN, use the other one
    df.loc[df['tx_group'].isna() & df[from_label].notna(), 'tx_group'] = df[from_label]
    df.loc[df['tx_group'].isna() & df[to_label].notna(), 'tx_group'] = df[to_label]

    return df

def get_tx_group_with_fees(df, in_group): 
    if not isinstance(in_group, list):
        in_group = [in_group]
    
    if len(in_group) == 1:
        in_group = in_group[0]
        
    df_subset = df[df['p2p'] | df['p2c'] | df['c2p'] | df['c2c']].reset_index(drop=True)
    
    conditions = [df_subset['p2p'], df_subset['p2c'], df_subset['c2p'], df_subset['c2c']]
    choices = ['hx -> hx', 'hx -> cx', 'cx -> hx', 'cx -> cx']
    
    df_subset.loc[:, 'tx_mode'] = np.select(conditions, choices, default=np.nan)
    
    df_subset = compute_tx_group(df_subset)
    
    tx_count = df_subset[in_group].value_counts().rename('count')
    tx_fees = df_subset[df_subset['tx_type'] == 'main'].groupby(in_group)['tx_fees'].sum().rename('fees')
    
    tx_with_fees = pd.concat([tx_fees, tx_count], axis=1).sort_values(by=['fees', 'count'], ascending=False)
    tx_with_fees['fees'] = np.where(tx_with_fees['fees'].isna(), 0, tx_with_fees['fees'])
    tx_with_fees = tx_with_fees.reset_index()
    if  not isinstance(in_group, list):
        tx_with_fees = tx_with_fees.rename(columns={'index': 'group'})    
    else:
        group_col = [col for col in tx_with_fees if 'group' in col][0]
        tx_with_fees = tx_with_fees.rename(columns={group_col: 'group'})    

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
    
    # Aggregate transaction counts and fees by from_label_group and to_label_group
    df_agg = df_subset.groupby(['from_label_group', 'to_label_group']).agg({
        'TxCount': 'sum',
        'tx_fees': 'sum',
        'Value in USD': 'sum'
    }).reset_index()
    return df_agg


def get_active_user_wallet_count(df):
    df_temp = df.copy()
    df_temp['active_user_wallet'] = np.where(df_temp['p2p'] | df_temp['p2c'] | df_temp['c2p'], True, False)
    df_subset = df_temp[df_temp['active_user_wallet']].reset_index(drop=True)
    active_user_wallets = pd.concat([df_subset['from'], df_subset['to']])
    active_user_wallet_count = len(active_user_wallets[active_user_wallets.str.startswith('hx', na=False)].unique())
    return active_user_wallet_count, df_subset



def visualise_tx_group_with_fees_by_tx_mode(df, tx_path_date, total_transfer_value_text, balanced_burn, in_group=['to_label_group', 'tx_mode'], to_or_from='to', save_path='', log_scale=False):
    sns.set(style="dark")
    plt.style.use("dark_background")

    active_user_wallet_count, _ = get_active_user_wallet_count(df)

    tx_count_with_fees = get_tx_group_with_fees(df, in_group)
    L1_fees = tx_count_with_fees['fees'].sum()
    dex_row = {'group': 'Balanced', 'tx_mode': 'DEX', 'fees': balanced_burn, 'count': 0}
    tx_count_with_fees = tx_count_with_fees.append(dex_row, ignore_index=True)
    
    contract_address_label = 'Destination' if 'to_label_group' in in_group else 'Source'
    
    total_activity_count = df['regTxCount'].sum() + df['intTxCount'].sum() + df['systemTickCount'].sum() + df['intEvtCount'].sum()
    total_activity_count_txt = f"Regular Tx: {df['regTxCount'].sum():,}; Internal Tx: {df['intTxCount'].sum():,}\nInternal Events: {df['intEvtCount'].sum():,}; System Ticks: {df['systemTickCount'].sum():,}"

    total_tx_count = tx_count_with_fees['count'].sum()
    total_fees = f"{round(tx_count_with_fees['fees'].sum()):,}"
    total_tx_count_txt = f"Transactions (Fee-incurring): {int(total_tx_count):,}\nTotal Activity: {int(total_activity_count):,}\n{total_transfer_value_text}\nActive Wallets: {int(active_user_wallet_count):,}"

    fig, ax1 = plt.subplots(figsize=(14, 8))

    # Bar plot for transaction counts
    tx_count_with_fees_sorted = tx_count_with_fees.sort_values(by=['fees', 'count'], ascending=False)
    
    # Summing fees per group and sorting by total fees
    total_fees_per_group = tx_count_with_fees_sorted.groupby('group')['fees'].sum().sort_values(ascending=False)
    
    # Setting the group order for plotting
    tx_count_with_fees_sorted['group'] = pd.Categorical(tx_count_with_fees_sorted['group'], categories=total_fees_per_group.index, ordered=True)
    tx_count_with_fees_sorted = tx_count_with_fees_sorted.sort_values('group')

    tx_count_with_fees_pivoted = tx_count_with_fees_sorted.pivot(index='group', columns='tx_mode', values='count')

    # Ensure columns are in the specified order and add missing columns with NaN values
    required_order = ["hx -> hx", "hx -> cx", "cx -> hx", "cx -> cx"]
    for col in required_order:
        if col not in tx_count_with_fees_pivoted.columns:
            tx_count_with_fees_pivoted[col] = np.nan
    tx_count_with_fees_pivoted = tx_count_with_fees_pivoted[required_order]

    tx_count_with_fees_pivoted.plot(kind='bar', stacked=True, ax=ax1, legend=False)

    plt.title(f'Daily Transactions ({tx_path_date})', fontsize=14, weight='bold', pad=10, loc='left')
    ax1.set_xlabel(f'{contract_address_label} Contracts/Addresses')
    ax1.set_ylabel('Transactions', labelpad=10)
    ax1.set_xticklabels(tx_count_with_fees_pivoted.index, rotation=90, ha="center")

    # Ensure the main order for line plot is maintained
    tx_count_with_fees_main_order = tx_count_with_fees_sorted[['group', 'fees']].groupby('group').sum().sort_values(by='fees', ascending=False)
    tx_count_with_fees_main_order = tx_count_with_fees_main_order.reindex(tx_count_with_fees_pivoted.index)

    # Line plot for transaction fees
    ax2 = ax1.twinx()
    ax2.plot(tx_count_with_fees_main_order.index, tx_count_with_fees_main_order['fees'], marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black', markersize=8)
    ax2.set_ylabel('Fees burned (ICX)', labelpad=10)
    
    daily_burned_percentage = f"{tx_count_with_fees_main_order['fees'].sum()/daily_issuance:.2%}"
    fees_burned_label = f'Fees Burned: {total_fees} ICX (L1: {round(L1_fees):,}; DEX: {round(balanced_burn):,}) / {daily_burned_percentage} of daily inflation'

    # Adjust axis formatting
    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()

    # Increase ymax by 5%
    ax1.set_ylim(ymin, ymax * 1.05)

    if ymax >= 3_000:
        ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
    if ymax >= 1_000_000:
        ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))

    color = 'white'
    m_line = mlines.Line2D([], [], color=color, label=f'{fees_burned_label}', linewidth=1, marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black')
    leg = plt.legend(handles=[m_line], loc='upper right', fontsize='medium', bbox_to_anchor=(0.98, 0.999), frameon=False)
    for text in leg.get_texts():
        plt.setp(text, color='cyan')

    plt.tight_layout(rect=[0, 0, 1, 1])

    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()
    
    ymax_scale_factor = 0.22 if log_scale else 0.82
    ax1.text(xmax * 0.97, ymax * ymax_scale_factor, total_tx_count_txt,
             horizontalalignment='right',
             verticalalignment='center',
             linespacing=1.5,
             fontsize=11,
             weight='bold')

    handles, labels = ax1.get_legend_handles_labels()
    labels = [i.replace('count', 'Tx count') if 'count' in i else i for i in labels]
    ax1.legend(handles, labels, loc='upper right', bbox_to_anchor=(1, 1.08), frameon=False, fancybox=True, shadow=True, ncol=4)

    ax1.text(xmax * 1.07, ymax * -0.12, total_activity_count_txt,
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
    plt.savefig(save_path.joinpath(f'tx_summary_{to_or_from}_{tx_path_date}.png'))
    plt.close()



def plot_transaction_network(df, balanced_burn=0, from_column='from_label_group', to_column='to_label_group', 
                             tx_count_column='TxCount', fees_column='tx_fees', value_column='Value in USD', 
                             main_node_group='to_label_group', max_size_values=100_000,
                             save_path='', tx_path_date='', dpi=300):
    """
    Plots a transaction network graph based on the provided DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame containing transaction data.
        from_column (str): Name of the column with the source node labels.
        to_column (str): Name of the column with the destination node labels.
        tx_count_column (str): Name of the column with transaction counts.
        fees_column (str): Name of the column with transaction fees.
        value_column (str): Name of the column with transaction values in USD.
        main_node_group (str): Group to compute node sizes based on total fees.
        max_size_values (int): Maximum size of the nodes.

    Returns:
        None: Displays a plot of the transaction network graph.
    """

    # Create a directed graph
    G = nx.DiGraph()

    # Add edges to the graph
    for index, row in df.iterrows():
        G.add_edge(row[from_column], row[to_column], weight=row[tx_count_column], fees=row[fees_column])

    # Compute node sizes based on total fees
    node_fees = df.groupby(main_node_group)[fees_column].sum().to_dict()
    node_fees['Balanced'] += balanced_burn
    max_fee = max(node_fees.values())
    total_fees = df[fees_column].sum() + balanced_burn

    # Compute node sizes based on total value transferred
    node_values_in_usd = df.groupby(main_node_group)[value_column].sum()
    node_values_in_usd = node_values_in_usd.to_dict()
    max_value = max(node_values_in_usd.values())
    total_values_in_usd = df[value_column].sum()

    # Normalizing node sizes and applying a cap
    node_sizes_values = [min((node_values_in_usd.get(node, 1) / max_value) * max_size_values, max_size_values) for node in G.nodes()]

    # Compute edge colors based on transaction counts
    edge_weights = np.array([d['weight'] for (u, v, d) in G.edges(data=True)])
    cmap = cm.get_cmap('viridis')
    edge_colors = [cmap(weight / max(edge_weights)) for weight in edge_weights]

    # Use the Kamada-Kawai layout for better node distribution
    pos = nx.circular_layout(G)

    sns.set(style="dark")
    plt.style.use("dark_background")
    plt.figure(figsize=(14, 11))

    # Draw nodes with sizes based on fees, semi-transparent and with border
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes_values, node_color='lightblue', alpha=0.2, edgecolors='black', linewidths=1.5)

    # Draw edges with colors based on transaction count and curved
    nx.draw_networkx_edges(G, pos, arrowstyle='-|>', arrowsize=10, edge_color=edge_colors,
                           width=2, connectionstyle='arc3,rad=0.1', alpha=0.6)

    # Draw node labels with fees and values in different colors
    for node, (x, y) in pos.items():
        node_text = node
        value_text = f"\n\n${int(node_values_in_usd.get(node, 0)):,}"
        fees_text = f"\n\n{int(node_fees.get(node, 0)):,} ICX"
        
        plt.text(x, y, node_text, fontsize=9, fontfamily='sans-serif', color='cyan', ha='center', va='center')
        plt.annotate(f"{value_text}", (x, y), fontsize=8, fontfamily='sans-serif', color='yellow', ha='center', va='center')
        plt.annotate(f"\n\n{fees_text}", (x, y), fontsize=8, fontfamily='sans-serif', color='orange', ha='center', va='center')

    # Create a ScalarMappable for the colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=min(edge_weights), vmax=max(edge_weights)))
    sm.set_array([])
    plt.colorbar(sm, label='Transaction Count')

    plt.title(f'Transaction Network Graph ({tx_path_date})', fontsize=14, loc='left')    
    # plt.text(0.68, 1.013, '(', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
    # plt.text(0.78, 1.013, 'Value Transferred ', color='yellow', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
    # plt.text(0.87, 1.013, '&', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
    # plt.text(0.94, 1.013, 'Fees Burned', color='orange', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
    # plt.text(0.99, 1.013, ')', color='white', fontsize=14, ha='center', va='center', transform=plt.gca().transAxes)
    
    plt.text(1.00, 1.035, f'Value Transferred (~{total_values_in_usd:,.0f} USD)', color='yellow', fontsize=14, ha='right', va='center', transform=plt.gca().transAxes)
    plt.text(1.00, 1.015, f'Fees Burned ({total_fees:,.2f} ICX)', color='orange', fontsize=14, ha='right', va='center', transform=plt.gca().transAxes)
    
    plt.tight_layout()
    plt.show()

    save_full_path = Path(save_path) / f'tx_network_graph_{tx_path_date}.png'
    plt.savefig(save_full_path)#, dpi=dpi)
    plt.close()



def get_unique_active_wallet_interactions(df, save_path, tx_path_date, log_scale=True):
    """
    Computes unique active wallet interactions and plots a bar chart.

    Parameters:
        df (pd.DataFrame): The DataFrame containing transaction data with 'from' and 'to' columns.
        log_scale (bool): Whether to use a logarithmic scale for the y-axis.

    Returns:
        None
    """

    # Get active user wallet count and the subset of the DataFrame
    active_user_wallet_count, df_subset = get_active_user_wallet_count(df)

    # Sort the addresses in 'from' and 'to' columns and store them in new columns
    sorted_from_to = pd.DataFrame({
        'sorted_from': np.minimum(df_subset['from'], df_subset['to']),
        'sorted_to': np.maximum(df_subset['from'], df_subset['to'])
    })

    # Combine the sorted columns into a single column for unique identification
    df_subset['pair_id'] = sorted_from_to['sorted_from'] + '_' + sorted_from_to['sorted_to']

    # Drop duplicates based on the 'pair_id' column
    unique_pairs_df = df_subset.drop_duplicates(subset='pair_id').reset_index(drop=True)

    # Drop the 'pair_id' column if it is no longer needed
    unique_pairs_df = unique_pairs_df.drop(columns='pair_id')

    # Compute the 'tx_group' column using the previously defined function
    unique_pairs_df = compute_tx_group(unique_pairs_df)

    # Count unique wallet interactions by group
    wallet_interaction_by_group_counts = (
        unique_pairs_df['tx_group']
        .value_counts()
        .reset_index()
        .rename(columns={'index': 'group', 'tx_group': 'count'})
    )

    # Plot the results using seaborn
    fig, ax1 = plt.subplots(figsize=(14, 8))
    sns.barplot(data=wallet_interaction_by_group_counts, x='group', y='count', palette='viridis', ax=ax1)

    # Add title and labels
    plt.title('Unique Active Wallet Interactions', fontsize=16)
    plt.xlabel('Interaction Group', fontsize=14)
    plt.ylabel('Number of Wallets', fontsize=14)
    plt.xticks(rotation=90)

    # Add counts above each bar
    for i, row in wallet_interaction_by_group_counts.iterrows():
        ax1.text(i, row['count'], f'{row["count"]}', ha='center', va='bottom', fontsize=8)

    # Apply logarithmic scale if specified
    if log_scale:
        ax1.set_yscale('log')
        plt.ylabel('Number of Wallets (Log Scale)', fontsize=14)

    ax1.text(
        0.98, 0.96,  # X and Y position in axes coordinates
        f"Unique active wallets: {active_user_wallet_count:,.0f}",
        transform=ax1.transAxes,  # Specify that the coordinates are relative to the axes
        fontsize=12,
        ha='right',  # Horizontal alignment
        va='top',  # Vertical alignment
        bbox=dict(facecolor='cyan', alpha=0.5, edgecolor='none')  # Optional: Add a semi-transparent background box
    )

    plt.tight_layout()
    plt.show()
    
    save_full_path = Path(save_path) / f'active_wallet_count_{tx_path_date}.png'
    plt.savefig(save_full_path)#, dpi=dpi)
    plt.close()
    
    
# =============================================================================
# RUN    
# =============================================================================




balanced_dex_icx_burned = get_balanced_burned_amount(date_of_interest, tx_block_paths)
daily_issuance = get_iiss_info(walletPath)['Iglobal']*12/365

for tx_path in tqdm(matching_paths):
    tx_path_date = tx_path.stem.split('_')[-1]
    this_year = tx_path_date[0:4]
    
    resultsPath_year = resultsPath.joinpath(this_year)
    resultsPath_year.mkdir(parents=True, exist_ok=True)
    
    
    df = pd.read_csv(tx_path, low_memory=False)
    if "Unnamed: 0" in df.columns:
        df.drop("Unnamed: 0", axis=1, inplace=True)

    balanced_burn = balanced_dex_icx_burned.get(tx_path_date)  

    df_addresses = pd.concat([df[['from','from_label_group']].rename(columns={'from':'address', 'from_label_group':'group'}),
                             df[['to','to_label_group']].rename(columns={'to':'address', 'to_label_group':'group'})]).drop_duplicates()
    
    df_addresses = df_addresses.dropna()
    df_addresses = df_addresses[df_addresses['address'].str.startswith(('cx', 'hx')) | (df_addresses['address'] == '0x0')]
    

    tokentransfer_date_Path = find_token_transfer_path(tokentransferPath, tx_path_date)    
    token_transfer_summary_df = pd.read_csv(tokentransfer_date_Path, low_memory=False)
    
    if 'address' in token_transfer_summary_df.columns:
        token_transfer_summary_df = pd.merge(token_transfer_summary_df, df_addresses, on='address', how='left')
    else:
        token_addresses = load_from_json(tokenaddressPath.joinpath('token_addresses.json'))
        symbol_to_address = {v['token_symbols']: k for k, v in token_addresses.items()}
        token_transfer_summary_df['address'] = token_transfer_summary_df['IRC Token'].map(symbol_to_address)
        token_transfer_summary_df = pd.merge(token_transfer_summary_df, df_addresses, on='address', how='left')
    
    ## Manual addition
    token_transfer_summary_df['group'] = np.where(token_transfer_summary_df['IRC Token'] == 'FIN', 'Optimus', token_transfer_summary_df['IRC Token'])
    token_transfer_summary_df['group'] = np.where(token_transfer_summary_df['IRC Token'] == 'BTCB', 'Bitcoin', token_transfer_summary_df['IRC Token'])
    token_transfer_summary_df['group'] = np.where(token_transfer_summary_df['IRC Token'] == 'BNB', 'BNB', token_transfer_summary_df['IRC Token'])
    token_transfer_summary_df['group'] = np.where(token_transfer_summary_df['IRC Token'] == 'HVH', 'Havah', token_transfer_summary_df['IRC Token'])
    token_transfer_summary_df['group'] = np.where(token_transfer_summary_df['IRC Token'].str.contains('USDC', na=False) & ~token_transfer_summary_df['IRC Token'].str.contains('IUSDC', na=False), 'USDC', token_transfer_summary_df['IRC Token'])

    df = pd.merge(df, token_transfer_summary_df.rename(columns={'IRC Token':'symbol'})[['symbol', 'Price in USD', 'group']], on='symbol', how='left')
    df['group'] = np.where((df['symbol'] == 'ICX') & df['group'].isna(), 'ICX', df['group'])
    df['value'] = df['value'].astype('float64').fillna(0)
    df['Price in USD'] = df['Price in USD'].astype('float64').fillna(0)
    df['Value in USD'] = df['value'] * df['Price in USD']
    
    ## outlier removal    
    upper_cap = 50_000_000
    outliers = (df['Value in USD'] > upper_cap)
    df.loc[df['Value in USD'] > upper_cap, 'Value in USD'] = 0
    
    df_agg = get_agg_df_for_count_fees_and_value(df)
    
    # total_transfer_value = df['Value in USD'].sum()
    # total_transfer_value_by_group = df.groupby('group')['Value in USD'].sum()
    # total_transfer_value_text = 'Total Value Transferred: ~' + '{:,}'.format(int(total_transfer_value)) + ' USD'
    
    total_transfer_value = df_agg['Value in USD'].sum()
    # total_transfer_value_by_group = df_agg.groupby('group')['Value in USD'].sum()
    total_transfer_value_text = f'Value Transferred: ~{total_transfer_value:,.0f} USD'

    # visualise_tx_group_with_fees_by_tx_mode(df, tx_path_date, total_transfer_value_text, balanced_burn, in_group=['from_label_group', 'tx_mode'], to_or_from='from', save_path=resultsPath_year)
    # visualise_tx_group_with_fees_by_tx_mode(df, tx_path_date, total_transfer_value_text, balanced_burn, in_group=['to_label_group', 'tx_mode'], to_or_from='to', save_path=resultsPath_year)
    
    # transaction count and fees
    visualise_tx_group_with_fees_by_tx_mode(df, tx_path_date, total_transfer_value_text, balanced_burn, in_group=['tx_group', 'tx_mode'], to_or_from='mixed', save_path=resultsPath_year)

    # network / value transferred and fees
    plot_transaction_network(df_agg, balanced_burn, save_path=resultsPath_year, tx_path_date=tx_path_date)

    # unique active wallet counts
    get_unique_active_wallet_interactions(df, save_path=resultsPath_year, tx_path_date=tx_path_date, log_scale=True)








