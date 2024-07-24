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

# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')
walletPath = Path(projectPath).joinpath("wallet")
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




daily_issuance = get_iiss_info(walletPath)['Iglobal']*12/365

for tx_path in tqdm(matching_paths):
    df = pd.read_csv(tx_path, low_memory=False)
    df.drop("Unnamed: 0", axis=1, inplace=True)



# df_temp = df[df['tx_type'] == 'main']
# df_temp['p2p'].sum() + df_temp['p2c'].sum() + df_temp['c2p'].sum() + df_temp['c2c'].sum()
# df_temp['regTxCount'].sum() + df_temp['intTxCount'].sum()



total_activity_count = df['regTxCount'].sum() + df['intTxCount'].sum() + df['systemTickCount'].sum() + df['intEvtCount'].sum()

tx_count_with_fees = get_tx_group_with_fees(df, in_group='to_label_group')
total_tx_count = tx_count_with_fees['to'].sum()
total_fees = tx_count_with_fees['fees'].sum().round(2)


# tx_count_with_fees_by_mode = get_tx_group_with_fees(df, in_group=['to_label_group', 'tx_mode'])
# tx_count_with_fees_by_mode_from = get_tx_group_with_fees(df, in_group=['from_label_group', 'tx_mode'])






total_tx = f"Transactions (Fee-incurring): {int(total_tx_count):,}\nTotal Activity: {int(total_activity_count):,}"



visualise_tx_group_with_fees(fee_burning_tx, date_prev, total_tx, system_tx)


def visualise_tx_group_with_fees(tx_count_with_fees, date_prev, all_tx, system_tx, log_scale=False):
    sns.set(style="dark")
    plt.style.use("dark_background")

    fig, ax1 = plt.subplots(figsize=(14, 8))

    # Bar plot for transaction counts
    tx_count_with_fees_sorted = tx_count_with_fees.sort_values(by='fees', ascending=False)
    tx_count_with_fees_sorted.plot(kind='bar', x='group', y='count', stacked=True, ax=ax1, color='palegoldenrod', legend=False)

    plt.title(f'Daily Transactions ({date_prev})', fontsize=14, weight='bold', pad=10, loc='left')
    ax1.set_xlabel('Destination Contracts/Addresses')
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
    m_line = mlines.Line2D([], [], color=color, label=f'({fees_burned_label})', linewidth=1, marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black')
    leg = plt.legend(handles=[m_line], loc='upper right', fontsize='medium', bbox_to_anchor=(0.98, 0.999), frameon=False)
    for text in leg.get_texts():
        plt.setp(text, color='cyan')

    plt.tight_layout(rect=[0,0,1,1])

    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()
    
    ymax_scale_factor = 0.22 if log_scale else 0.82
    ax1.text(xmax*0.97, ymax*ymax_scale_factor, all_tx,
             horizontalalignment='right',
             verticalalignment='center',
             linespacing=1.5,
             fontsize=12,
             weight='bold')

    handles, labels = ax1.get_legend_handles_labels()
    ax1.legend(handles, labels, loc='upper right', bbox_to_anchor=(1, 1.08), frameon=False, fancybox=True, shadow=True, ncol=3)

    ax1.text(xmax*1.07, ymax*-0.12, system_tx,
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



