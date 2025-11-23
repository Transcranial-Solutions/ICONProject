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
from datetime import datetime, timedelta
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet

# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail*.csv')])
tx_detail_paths = [i for i in tx_detail_paths if '_with_group' not in i.as_posix()]
tx_block_paths = sorted([i for i in dataPath.glob('transaction_blocks*.csv')])


ATTACH_WALLET_INFO = True
ONLY_SAVE_SPECIFIED_DATE = True ## if False, it will do all the data we have
SAVE_SUMMARY = True
REFRESH_SAVED_SUMMARY = False

# Constants
POSSIBLE_NANS = ['', ' ', np.nan]

# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 0 #0
date_prev = "2025-11-05"

day_1 = "2025-10-09" #07
day_2 = "2025-11-05"

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
    address_col = in_col.split('_label')[0]
    
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
    df[group_col] = np.where(df[address_col].isin(craft_addresses), 'Craft', df[group_col])

    # iAM
    df[group_col] = np.where(df[address_col] == 'cx210ded1e8e109a93c89e9e5a5d0dcbc48ef90394', 'iAM ', df[group_col])

    # Bridge
    bridge_addresses = [
        'cxa82aa03dae9ca03e3537a8a1e2f045bcae86fd3f',
        'cx0eb215b6303142e37c0c9123abd1377feb423f0e'
    ]
    df[group_col] = np.where(df[address_col].isin(bridge_addresses), 'Bridge', df[group_col])

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
    df[group_col] = np.where(df[address_col].isin(gangstabet_addresses), 'GangstaBet', df[group_col])
    df[group_col] = np.select(gangstabet_conditions, ['GangstaBet'] * len(gangstabet_conditions), default=df[group_col])

    # EPX
    epx_conditions = [
        df[group_col] == 'FutureICX',
        df[group_col].str.contains('epx', case=False, na=False)
    ]
    df[group_col] = np.select(epx_conditions, ['EPX'] * len(epx_conditions), default=df[group_col])

    # UP
    df[group_col] = np.where(df[address_col] == 'cxc432c12e6c91f8a685ee6ff50a653c8a056875e4', 'UP', df[group_col])

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
    
    # code metal
    df[group_col] = np.where(df[group_col].str.contains('code|metal', case=False, na=False), 'CodeMetal', df[group_col])
    
    # Blobble
    blobble_addresses = [
        'cx32ec70628489e36852e3ef248e8379f28ea47aa5',
        'cxf2ca3b655a782f39227934d37ff3b77f9b1ebcf9',
        'cx7b4472ca8408eca00a8b483c7151b11ae735988b',
        'hxec052dfc0db0ae26086ca78e36a054a8424d3707',
        'hx891d1d00f371272c0d2f735d58acd435ffef9e62',
        'cxf14dea1cff7ceb29a46e6d04533a08f84441e41d'
    ]
    df[group_col] = np.where(df[address_col].isin(blobble_addresses), 'Blobble', df[group_col])
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
        # print(f"Error fetching or parsing data: {e}")
        jtracker_df = pd.DataFrame()
    return jtracker_df

def token_tx_using_community_tracker(total_pages=100):
    """Get contract transactions data using ICON community tracker"""
    skip = 100
    last_page = total_pages * skip - skip
    page_count = range(0, last_page, skip)

    tx_all = [get_tx_via_icon_community_tracker(k) for k in tqdm(page_count, desc="Extracting tracker info")]
    df_contract = pd.concat(tx_all, ignore_index=True)
    return df_contract


def get_all_known_addresses(dailyPath):
    # reading address info
    walletaddressPath = Path(dailyPath, "wallet_addresses")
    with open(Path(walletaddressPath, 'contract_addresses.json')) as f:
              contract_addresses = json.load(f)
    with open(Path(walletaddressPath, 'exchange_addresses.json')) as f:
              exchange_addresses = json.load(f)
    with open(Path(walletaddressPath, 'other_addresses.json')) as f:
              other_addresses = json.load(f)
    
    all_known_addresses = {**contract_addresses, **exchange_addresses, **other_addresses}
    
    jknown_address = {}
    for address_dict in [exchange_addresses, other_addresses]:
        for k, v in address_dict.items():
            add_dict_if_noexist(k, jknown_address, v)
    
    jknown_address = get_contract_info(contract_addresses)
    merged_addresses = {**jknown_address, **all_known_addresses}
    return merged_addresses

def get_contract_info(contract_addresses):
    
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


def process_transaction_file(tx_path):
    df = pd.read_csv(tx_path, low_memory=False)
    if df.empty:
        return None

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
    summary['tx_fees_L1'] = df.loc[df['tx_type'] == 'main', 'tx_fees'].sum()

    return tx_date, summary

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

    if not isinstance(date_of_interest, list):
        date_of_interest = [date_of_interest]
        
    if not isinstance(tx_block_paths, list):
        tx_block_paths = [tx_block_paths]
        
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

def get_balanced_burned(block_height):
    # ctz_solidwallet = 'https://ctz.solidwallet.io/api/v3'
    local_endpoint = "http://127.0.0.1:9000/api/v3"
    
    icon_service = IconService(HTTPProvider(local_endpoint))
    contract_address = "cxdc30a0d3a1f131565c071272a20bc0b06fd4c17b"
    
    # block_height = "0x50d14d2"  
    try:
        call = CallBuilder()\
            .from_("hx0000000000000000000000000000000000000000")\
            .to(contract_address)\
            .method("getBurnedAmount")\
            .height(block_height)\
            .build()
        try:
            result = icon_service.call(call)
            print(f"Burned Amount: {result}")
        except Exception as e:
            result = '0x0'
            print(f"An error occurred: {e}")
            
    except:
        result = '0x0'
        print("Maybe contract does not exist.")

    output = loop_to_icx(hex_to_int(result))
    return output

def attach_wallet_info_to_tx_data(tx_path, merged_addresses):
    df = pd.read_csv(tx_path, low_memory=False)
    if df.empty:
        return None

    ## known addresses and groupings
    df['eventlogs_cx_label'] = df['eventlogs_cx'].map(merged_addresses)
    df['to_label'] = df['to'].map(merged_addresses)
    df['from_label'] = df['from'].map(merged_addresses)
    
    df['eventlogs_cx_label'] = np.where(df['systemTickCount'] == 1, 'System', df['eventlogs_cx_label'])
    df['to_label'] = np.where(df['systemTickCount'] == 1, 'System', df['to_label'])
    df['from_label'] = np.where(df['systemTickCount'] == 1, 'System', df['from_label'])

    df['to_label'] = np.where(df['to'].str.startswith('hx', na=False) & df['to_label'].isna(), 'unknown_hx', df['to_label'])
    df['to_label'] = np.where(df['to'].str.startswith('cx', na=False) & df['to_label'].isna(), 'unknown_cx', df['to_label'])
    
    df['from_label'] = np.where(df['from'].str.startswith('hx', na=False) & df['from_label'].isna(), 'unknown_hx', df['from_label'])
    df['from_label'] = np.where(df['from'].str.startswith('cx', na=False) & df['from_label'].isna(), 'unknown_cx', df['from_label'])

    df = grouping_wrapper(df, 'eventlogs_cx_label')
    df = grouping_wrapper(df, 'to_label')
    df = grouping_wrapper(df, 'from_label')
        
    return df


def main():
 
    if SAVE_SUMMARY:
        
        if not REFRESH_SAVED_SUMMARY:
            df_tx_detail_summary_loaded = pd.read_csv(resultsPath.joinpath('tx_detail_summary.csv'), low_memory=False)
            
            summary_loaded_date = list(df_tx_detail_summary_loaded['date'])
            tx_detail_paths_date = [i.stem.split('_')[-1] for i in tx_detail_paths]
            
            diff_dates = list(set(tx_detail_paths_date).difference(set(summary_loaded_date)))
            if len(diff_dates) > 0:
                tx_detail_paths_date_truncated = [path for path in tx_detail_paths if any(elem in path.stem.split('_') for elem in diff_dates)]
            else:
                tx_detail_paths_date_truncated = []
        else:
            tx_detail_paths_date_truncated = tx_detail_paths.copy()
        
        if len(tx_detail_paths_date_truncated) > 0:
        
            summary_counts = {}
            for tx_path in tqdm(tx_detail_paths_date_truncated, desc="Summarising tx details"):
                tqdm.write(f"Working on: {tx_path.stem}")
                result_of_tx = process_transaction_file(tx_path)
        
                if result_of_tx:
                    tx_date, summary = result_of_tx
                    balanced_dex_icx_burned = get_balanced_burned_amount(tx_date, tx_block_paths)
                    summary['tx_fees_DEX'] = balanced_dex_icx_burned.get(tx_date)
                    summary_counts[tx_date] = summary
        
            # summary_counts_native = convert_to_native(summary_counts)
            # print(json.dumps(summary_counts_native, indent=4))
        
            df_tx_detail_summary = pd.DataFrame(summary_counts).T.rename_axis('date').reset_index()
           
            if not REFRESH_SAVED_SUMMARY:
                df_tx_detail_summary_final = pd.concat([df_tx_detail_summary_loaded, df_tx_detail_summary], axis=0).reset_index(drop=True)
            else:
                df_tx_detail_summary_final = df_tx_detail_summary
            
            df_tx_detail_summary_final.to_csv(resultsPath.joinpath('tx_detail_summary.csv'), index=False)
    
    # this is for detailed tx analysis
    if ATTACH_WALLET_INFO:
        merged_addresses = get_all_known_addresses(dailyPath)
        overlapping_paths = [path for path in matching_paths if path in tx_detail_paths]
        loop_paths = overlapping_paths if ONLY_SAVE_SPECIFIED_DATE else tx_detail_paths
        
        for tx_path in tqdm(loop_paths, desc="tx details with group info"):
            tqdm.write(f"Working on: {tx_path.stem}")
            tx_date = tx_path.stem.split('_')[-1]
            df = attach_wallet_info_to_tx_data(tx_path, merged_addresses)            
            # check = df[df['txHash'] == '0x9baac9fafb9271ae9a47037e22065ad089b32a182b7e091994c31be779c87394']
            df.to_csv(dataPath.joinpath(f'tx_detail_with_group_info_{tx_date}.csv'), index=False)
        

if __name__ == "__main__":
    main()


