#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 19:21:41 2024

@author: tono
"""

from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import requests

# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail*.csv')])



ATTACH_WALLET_INFO = True

# Constants
POSSIBLE_NANS = ['', ' ', np.nan]


if ATTACH_WALLET_INFO:
    # reading address info
    walletaddressPath = Path(dailyPath, "wallet_addresses")
    with open(Path(walletaddressPath, 'contract_addresses.json')) as f:
              contract_addresses = json.load(f)
    with open(Path(walletaddressPath, 'exchange_addresses.json')) as f:
              exchange_addresses = json.load(f)
    with open(Path(walletaddressPath, 'other_addresses.json')) as f:
              other_addresses = json.load(f)
    
    all_known_addresses = {**contract_addresses, **exchange_addresses, **other_addresses}



def replace_dict_if_unknown(key, d, value):
    if ("-" in d.values()) or (d.values() == None):
        d.update({key: value})
         
def add_dict_if_noexist(key, d, value):
    if key not in d:
        d[key] = value
        
def merge_dicts(dict1, dict2):
    merged_dict = dict1.copy()
    
    for key, value in dict2.items():
        if key not in merged_dict:
            merged_dict[key] = value
    
    return merged_dict


def wallet_grouping(df, in_col, in_name, else_name):
    group_col = f'{in_col}_group'
    df[group_col] = else_name
    mask = df[in_col].notna() & df[in_col].str.contains(in_name, case=False, regex=True)
    df.loc[mask, group_col] = in_name
    df[group_col] = np.where(df['to'] == 'System', 'System', df[group_col])
    return df


def group_wallet(df, in_col='to_def'):
    group_col = f'{in_col}_group'
    these_incols = [
        'bithumb', 'upbit', 'velic', 'bitvavo', 'unkEx_c', 'unkEx_d', 'kraken',
        'circle_arb', 'ICONbet', 'Relay', 'MyID', 'Balance', 'Nebula', 'peek', 
        'Craft', 'SEED', 'iAM '
    ]

    df = wallet_grouping(df, in_col, in_name='binance', else_name=df[in_col])

    for i in these_incols:
        df = wallet_grouping(df, in_col, i, df[group_col])
    
    # Unified protocol grouping
    unified_protocol = (df[in_col].str[1].str.isupper()) & (df[in_col].str[0] == 'u')
    df[group_col] = np.where(unified_protocol, 'UP', df[group_col])

    return df

def manual_grouping(df, in_col):
    group_col = f'{in_col}_group'
    
    # Balanced
    balanced_conditions = [
        df[group_col].str.contains('balanced_', case=False, na=False),
        df[group_col].str.contains('baln', case=False, na=False),
        df[group_col].str.contains('circle_arb', case=False, na=False),
        df[group_col] == 'Balance'
    ]
    df[group_col] = np.select(balanced_conditions, ['Balanced'] * len(balanced_conditions), default=df[group_col])
    
    # Omm
    df[group_col] = np.where(df[group_col].str.contains('omm', case=False, na=False), 'Omm', df[group_col])

    # Optimus
    optimus_conditions = [
        df[group_col].str.contains('optimus', case=False, na=False),
        df[in_col].str.contains('optimus', case=False, na=False),
        df[group_col].str.contains('finance token', case=False, na=False)
    ]
    df[group_col] = np.select(optimus_conditions, ['Optimus'] * len(optimus_conditions), default=df[group_col])

    # Craft
    craft_addresses = [
        'cx9c4698411c6d9a780f605685153431dcda04609f',
        'cx82c8c091b41413423579445032281bca5ac14fc0',
        'cx7ecb16e4c143b95e01d05933c17cb986cfe618e6',
        'cx5ce7d060eef6ebaf23fa8a8717d3a5c8f0a3fda9',
        'cx2d86ce51600803e187ce769129d1f6442bcefb5b'
    ]
    df[group_col] = np.where(df['to'].isin(craft_addresses), 'Craft', df[group_col])

    # iAM
    df[group_col] = np.where(df['to'] == 'cx210ded1e8e109a93c89e9e5a5d0dcbc48ef90394', 'iAM ', df[group_col])

    # Bridge
    bridge_addresses = [
        'cxa82aa03dae9ca03e3537a8a1e2f045bcae86fd3f',
        'cx0eb215b6303142e37c0c9123abd1377feb423f0e'
    ]
    df[group_col] = np.where(df['to'].isin(bridge_addresses), 'Bridge', df[group_col])

    # ICONbet
    iconbet_conditions = [
        df[group_col].str.contains('SicBo', case=False, na=False),
        df[group_col].str.contains('Jungle Jackpot', case=False, na=False),
        df[group_col] == 'TapToken'
    ]
    df[group_col] = np.select(iconbet_conditions, ['ICONbet'] * len(iconbet_conditions), default=df[group_col])

    # GangstaBet
    gangstabet_conditions = [
        df[group_col].str.contains('gangstabet', case=False, na=False),
        df[group_col].str.contains('crown', case=False, na=False),
        df[group_col].str.lower().str.startswith('gang', na=False)
    ]
    gangstabet_addresses = ['cx8683d50b9f53275081e13b64fba9d6a56b7c575d']
    df[group_col] = np.where(df['to'].isin(gangstabet_addresses), 'GangstaBet', df[group_col])
    df[group_col] = np.select(gangstabet_conditions, ['GangstaBet'] * len(gangstabet_conditions), default=df[group_col])

    # EPX
    epx_conditions = [
        df[group_col] == 'FutureICX',
        df[group_col].str.contains('epx', case=False, na=False)
    ]
    df[group_col] = np.select(epx_conditions, ['EPX'] * len(epx_conditions), default=df[group_col])

    # UP
    df[group_col] = np.where(df['to'] == 'cxc432c12e6c91f8a685ee6ff50a653c8a056875e4', 'UP', df[group_col])

    # Inanis
    df[group_col] = np.where(df[group_col].str.contains('Inanis', case=False, na=False), 'Inanis', df[group_col])

    # FRAMD
    framd_conditions = [
        df[in_col].str.contains('Yetis', case=False, na=False),
        df[group_col].str.contains('FRAMD', case=False, na=False)
    ]
    df[group_col] = np.select(framd_conditions, ['FRAMD'] * len(framd_conditions), default=df[group_col])

    # BTP
    df[group_col] = np.where(df[in_col].str.lower().str.startswith('btp', na=False), 'BTP', df[group_col])

    # Blobble
    blobble_addresses = [
        'cx32ec70628489e36852e3ef248e8379f28ea47aa5',
        'cxf2ca3b655a782f39227934d37ff3b77f9b1ebcf9',
        'cx7b4472ca8408eca00a8b483c7151b11ae735988b',
        'hxec052dfc0db0ae26086ca78e36a054a8424d3707',
        'hx891d1d00f371272c0d2f735d58acd435ffef9e62',
        'cxf14dea1cff7ceb29a46e6d04533a08f84441e41d'
    ]
    df[group_col] = np.where(df['to'].isin(blobble_addresses), 'Blobble', df[group_col])
    df[group_col] = np.where(df[group_col].str.contains('blobble', case=False, na=False), 'Blobble', df[group_col])

    return df

def grouping_wrapper(df, in_col):
    df = group_wallet(df, in_col)
    df = manual_grouping(df, in_col)
    return df


# Function to get transactions and the page count needed for webscraping
def get_tx_via_icon_community_tracker(skip_pages):
    """Extract JSON information from ICON community site"""
    url = f'https://tracker.icon.community/api/v1/addresses/contracts?search=&skip={skip_pages}&limit=100'
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        req = requests.get(url, headers=headers)
        req.raise_for_status()  # Raise an exception for HTTP errors
        jtracker = req.json()  # Directly use json() method to parse response
        jtracker_df = pd.DataFrame(jtracker)
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching or parsing data: {e}")
        jtracker_df = pd.DataFrame()
    return jtracker_df

def token_tx_using_community_tracker(total_pages=100):
    """Get contract transactions data using ICON community tracker"""
    skip = 100
    last_page = total_pages * skip - skip
    page_count = range(0, last_page, skip)

    tx_all = [get_tx_via_icon_community_tracker(k) for k in tqdm(page_count)]
    df_contract = pd.concat(tx_all, ignore_index=True)
    return df_contract

def get_contract_info():
    jknown_address = {}
    df_contract = token_tx_using_community_tracker()

    # Clean 'name' column in the dataframe
    df_contract['name'] = np.where(df_contract['name'].isin(POSSIBLE_NANS), '-', df_contract['name'])

    # Create a dictionary from the dataframe
    contract_d = dict(zip(df_contract['address'], df_contract['name']))

    # Update known addresses with contract addresses
    jknown_address.update(contract_d)

    # Update contract info
    for k, v in contract_addresses.items():
        replace_dict_if_unknown(k, jknown_address, v)

    # Replace '-' with 'unknown_cx'
    jknown_address = {k: ("unknown_cx" if v == "-" else v) for k, v in jknown_address.items()}

    return jknown_address



def convert_to_native(obj):
    if isinstance(obj, np.int64): 
        return int(obj)
    if isinstance(obj, np.float64): 
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: convert_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_native(i) for i in obj]
    return obj








def process_transaction_file(tx_path, merged_addresses):
    df = pd.read_csv(tx_path, low_memory=False)
    if df.empty:
        return None
    
    if ATTACH_WALLET_INFO:

        ## known addresses and groupings
        df['eventlogs_cx_label'] = df['eventlogs_cx'].map(merged_addresses)
        df['to_label'] = df['to'].map(merged_addresses)
        df['from_label'] = df['from'].map(merged_addresses)
        
        df['eventlogs_cx_label'] = np.where(df['txIndex'] == 0, 'System', df['eventlogs_cx_label'])
        df['to_label'] = np.where(df['txIndex'] == 0, 'System', df['to_label'])
        df['from_label'] = np.where(df['txIndex'] == 0, 'System', df['from_label'])
        
        df['to_label'] = np.where(df['to'].str.startswith('hx', na=False) & df['to_label'].isna(), 'unknown_hx', df['to_label'])
        df['to_label'] = np.where(df['to'].str.startswith('cx', na=False) & df['to_label'].isna(), 'unknown_cx', df['to_label'])
        
        df['from_label'] = np.where(df['from'].str.startswith('hx', na=False) & df['from_label'].isna(), 'unknown_hx', df['from_label'])
        df['from_label'] = np.where(df['from'].str.startswith('cx', na=False) & df['from_label'].isna(), 'unknown_cx', df['from_label'])
    
        df = grouping_wrapper(df, 'eventlogs_cx_label')
        df = grouping_wrapper(df, 'to_label')
        df = grouping_wrapper(df, 'from_label')
        

    tx_date = df['tx_date'].mode()[0]
    summary = {}

    # tx count
    summary['tx_count'] = df['regTxCount'].sum() + df['intTxCount'].sum()
    summary['other_msg_count'] = df['intEvtCount'].sum()
    summary['system_count'] = df['systemTickCount'].sum()

    # interaction count
    summary['p2p_count'] = df['p2p'].sum()
    summary['p2c_count'] = df['p2c'].sum()
    summary['c2c_count'] = df['c2c'].sum()
    summary['c2p_count'] = df['c2p'].sum()

    # unique hx wallets
    hx_addresses = np.unique(np.concatenate([
        df['to'][df['to'].str.startswith('hx', na=False)].unique(),
        df['from'][df['from'].str.startswith('hx', na=False)].unique()
    ]))
    summary['hx_address_count'] = len(hx_addresses)

    # unique cx wallets
    cx_addresses = np.unique(np.concatenate([
        df['to'][df['to'].str.startswith('cx', na=False)].unique(),
        df['from'][df['from'].str.startswith('cx', na=False)].unique()
    ]))
    summary['cx_address_count'] = len(cx_addresses)

    # tx fees (fees burned)
    summary['tx_fees'] = df.loc[df['tx_type'] == 'main', 'tx_fees'].sum()

    return tx_date, summary, df


def main():
    
    if ATTACH_WALLET_INFO:
        jknown_address = {}
        for address_dict in [exchange_addresses, other_addresses]:
            for k, v in address_dict.items():
                add_dict_if_noexist(k, jknown_address, v)
        
        jknown_address = get_contract_info()
        merged_addresses = {**jknown_address, **all_known_addresses}
        
    summary_counts = {}
    # df_combined = []
    for tx_path in tqdm(tx_detail_paths):
        result = process_transaction_file(tx_path, merged_addresses)
        if result:
            tx_date, summary, df = result
            summary_counts[tx_date] = summary
            df.to_csv(f'tx_detail_with_group_info_{tx_date}.csv')
    
    # df_all = pd.concat(df_combined)

    summary_counts_native = convert_to_native(summary_counts)

    print(json.dumps(summary_counts_native, indent=4))

    df_final = pd.DataFrame(summary_counts).T
    df_final = df_final[~df_final.index.astype(str).str.startswith('2024-05')]
    df_final.to_csv(resultsPath.joinpath('tx_detail_summary.csv'))
    # df_all.to_csv(resultsPath.joinpath('tx_detail_combined.csv'))

if __name__ == "__main__":
    main()



# def get_tx_group_with_fees(df, in_group): 
#     tx_count = df[in_group].value_counts().rename('tx_count')
#     fees_of_tx_count = df[df['tx_type'] == 'main'].groupby([in_group])['tx_fees'].sum()
#     tx_with_fees = pd.concat([fees_of_tx_count, tx_count], axis=1).sort_values(by = ['tx_fees', 'tx_count'], ascending=False)
#     return tx_with_fees

# get_tx_group_with_fees(df, in_group='to_label_group')




