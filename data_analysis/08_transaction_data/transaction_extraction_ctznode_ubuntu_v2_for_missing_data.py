#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 22 08:18:41 2025

@author: tono
"""

#########################################################################
## Project: Transaction per day                                        ##
## Date: July 2021                                                     ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Extract

# import json library
# import urllib
# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# from pandas.core.groupby.generic import DataFrameGroupBy
# import pylab as pl
import os
from functools import reduce
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from iconsdk.builder.call_builder import CallBuilder
# from typing import Union
from time import time, sleep
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random
import codecs
import dns.resolver
import json
import requests
from decimal import Decimal
from pathlib import Path

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

workers = 1

dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'
tokentransferPath = Path(dailyPath).joinpath("10_token_transfer/results/")
tokenaddressPath = Path(dailyPath).joinpath("wallet_addresses")

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(projectPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)


def yesterday(doi = "2021-08-20"):
    yesterday = datetime.fromisoformat(doi) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20"):
    tomorrow = datetime.fromisoformat(doi) + timedelta(1)
    return tomorrow.strftime('%Y-%m-%d')



# today's date
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")


# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 2 #0 #0
date_prev = "2025-11-06"

day_1 = "2025-10-09" #07
day_2 = "2025-11-05"

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


# remote = "https://ctz.solidwallet.io/api/v3"
# remote = "http://52.79.77.39:9000/api/v3" # 
# remote = "http://52.196.159.184:9000/api/v3"
remote = "http://127.0.0.1:9000/api/v3"

data = {'jsonrpc':'2.0', 'method': 'icx_getLastBlock','id': 1223}


def get_height(endpoint: str) -> int:
        payload = json.dumps(data)
        payload = payload.encode()
        req = requests.post(endpoint, data=payload)
        response_data = req.json()
        return response_data["result"]["height"]


def insert_str(string="YYYY-WW", index=4, timeinterval=' week') -> str:
    """
    Inserting strings between strings
    """
    return string[:index] + timeinterval + string[index:]

## value extraction function
def extract_values(obj, key):
    # Pull all values of specified key from nested JSON
    arr = []
    def extract(obj, arr, key):
        # Recursively search for values of key in JSON tree
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    if not results:
        # results=''
        results=np.nan
    return results

## value extraction function
def extract_values_no_params(obj, key):
    # Pull all values of specified key from nested JSON
    arr = []
    def extract(obj, arr, key):
        # Recursively search for values of key in JSON tree
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k != "params" and v != 'txHash':
                    if isinstance(v, (dict, list)):
                        extract(v, arr, key)
                    elif k == key:
                        arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    if not results:
        # results=''
        results=np.nan
    return results

def deep_get_imps(data, key: str):
    split_keys = re.split("[\\[\\]]", key)
    out_data = data
    for split_key in split_keys:
        if split_key == "":
            return out_data
        elif isinstance(out_data, dict):
            out_data = out_data.get(split_key)
        elif isinstance(out_data, list):
            try:
                sub = int(split_key)
            except ValueError:
                return None
            else:
                length = len(out_data)
                out_data = out_data[sub] if -length <= sub < length else None
        else:
            return None
    if not out_data:
        # out_data=''
        out_data=np.nan
    return out_data

def deep_get(dictionary, keys):
    return reduce(deep_get_imps, keys.split("."), dictionary)

def extract_date(df, date):
    df['date'] = pd.to_datetime(df['block_createDate']).dt.strftime("%Y-%m-%d")
    return df[df['date'] == date]


def load_from_json(filename):
    """
    Load dictionary data from a JSON file.
    """
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
    return data

remote_height = get_height(remote)

# resolve
# this_ip = dns.resolver.resolve('mercator.transcranial-solutions.dev')[0].to_text() # steve node down
this_ip = '159.196.221.33' # tono node
main_ip = "http://" + this_ip + ":9000/api/v3"

local_ip = "http://127.0.0.1:9000/api/v3"
# local_ip = "http://192.168.1.105:9000/api/v3"

for date_prev in date_of_interest:
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ solidwallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # using solidwallet
    # icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
    # icon_service = IconService(HTTPProvider("http://localhost:9000", 3))
    # icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io", 3, request_kwargs={"timeout": 60}))
    # icon_service = IconService(HTTPProvider("http://localhost:9000/api/v3", request_kwargs={"timeout": 60}))
    # block = icon_service.get_block(1002) 


    icon_service = None
    connection_refused_count = 0
    while icon_service is None:
        try:

            # localhost
            try:
                icon_service = IconService(HTTPProvider(local_ip, request_kwargs={"timeout": 60}))
                block = icon_service.get_block(remote_height)
                print('### Using the local node ###')
            except:
                icon_service = IconService(HTTPProvider(main_ip, request_kwargs={"timeout": 60}))
                block = icon_service.get_block(remote_height)
                workers = 1
                print('### Using the main node ###')
        
            # icon_service.get_block(42001335)
        
            ## Creating Wallet if does not exist (only done for the first time)
            tester_wallet = os.path.join(walletPath, "test_keystore_1")
        
            if os.path.exists(tester_wallet):
                wallet = KeyWallet.load(tester_wallet, "abcd1234*")
            else:
                wallet = KeyWallet.create()
                wallet.get_address()
                wallet.get_private_key()
                wallet.store(tester_wallet, "abcd1234*")
        
            tester_address = wallet.get_address()

        except:
            connection_refused_count += 1
            print(f'Connection refused {connection_refused_count} time(s) (get icon service). Retrying...')
            sleep(10)
        
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ block Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # windows_path = "/mnt/e/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
    # if not os.path.exists(windows_path):
    #     os.makedirs(windows_path)

    try:
        combined_df = pd.read_csv(os.path.join(dataPath, 'transaction_blocks_' + date_prev + '.csv'))
    except:
        combined_df_before = pd.read_csv(os.path.join(dataPath, 'transaction_blocks_' + yesterday(date_prev) + '.csv'))
        combined_df_before = extract_date(combined_df_before, yesterday(date_prev))

        combined_df_after = pd.read_csv(os.path.join(dataPath, 'transaction_blocks_' + tomorrow(date_prev) + '.csv'))
        combined_df_after = extract_date(combined_df_after, tomorrow(date_prev))

        start_block = combined_df_before['blockHeight'].iloc[-1] + 1
        end_block = combined_df_after['blockHeight'].iloc[0] - 1

        combined_df = pd.DataFrame(data=range(start_block, end_block), columns=['blockHeight'])
        combined_df['block_createDate'] = date_prev

        combined_df.to_csv(os.path.join(dataPath, 'transaction_blocks_' + date_prev + '.csv'), index=False)


    try:
        combined_df['date'] = pd.to_datetime(combined_df['block_createDate']).dt.strftime("%Y-%m-%d")
        print('Running ' + date_prev + ' data...')

        combined_df = extract_date(combined_df, date_prev)

        block_of_interest = combined_df['blockHeight']
        # block_of_interest = block_of_interest.iloc[0:100]

        def get_block_df(blockInfo):
            # print(blockInfo)
            this_blockinfo = None
            connection_refused_count = 0
            while this_blockinfo is None:
                try:
                    this_blockinfo = icon_service.get_block(blockInfo)
                except:
                    connection_refused_count += 1
                    print(f'Connection refused {connection_refused_count} time(s) (block extraction). Retrying...')
                    sleep(10)
            block_blockheight = deep_get(this_blockinfo, "height")
            # blockTS = deep_get(blockInfo, "time_stamp")
            txHashes = extract_values_no_params(this_blockinfo, "txHash")
            txTS = extract_values(this_blockinfo, "timestamp")
        
            block_count = pd.Series(block_blockheight).count()
            txHashes_count = pd.Series(txHashes).count()
            txTS_count = pd.Series(txTS).count()
        
            if block_count < txTS_count:
                if block_count == 1 and txTS_count ==  2:
                    txTS = [x for x in txTS if isinstance(x, int)][0]
                else:
                    txTS = [x for x in txTS if isinstance(x, int)]
        
            if block_count != txHashes_count:
                l=[]
                l.extend([block_blockheight] * txHashes_count)
                block_blockheight = l
        
            combined_block = {'blockHeight': block_blockheight,
                # 'block_timestamp': blockTS,
                'txHash': txHashes,
                'timestamp': txTS}
        
            block_df = pd.DataFrame(data=combined_block)
            return block_df
        
        
        def run_block():
            start = time()
            connection_refused_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                myblock = []
                for result in tqdm(executor.map(get_block_df, block_of_interest), total=len(block_of_interest)):
                    if result is None:
                        connection_refused_count += 1
                        print(f'Connection refused {connection_refused_count} time(s) (block extraction). Retrying...')
                        myblock.append(get_block_df(block_of_interest))
                    else:
                        myblock.append(result)
                        
            block_df = pd.concat(myblock).reset_index(drop=True)
            print(f'Time taken: {time() - start}')
        
            return block_df
        
        
        
        block_df = run_block()

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # saving block data balance
        # windows_path = "/mnt/e/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
        # if not os.path.exists(windows_path):
        #     os.makedirs(windows_path)
        # block_df.to_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'), index=False)
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # random_sleep_except = random.uniform(600,900)
        # print("I'll pause for"+str(random_sleep_except/60) + " minutes before doing another... \n")
        # sleep(random_sleep_except) #sleep the script for x seconds and....#
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # load block info
        # windows_path = "/mnt/e/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
        # block_df = pd.read_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'))

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # getting hash only
        txHashes = block_df['txHash']
        # txHashes = txHashes.iloc[0:200]
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # collecting transaction info using multithreading
        def get_tx_dfs(txHash):
            tx = None
            connection_refused_count = 0
            while tx is None:
                try:
                    tx = icon_service.get_transaction(txHash)
                except:
                    connection_refused_count += 1
                    print(f'Connection refused {connection_refused_count} time(s) (tx extraction A). Retrying...')
                    sleep(10)
            
            # removing some data here
            entries_to_remove = ('version','data','signature','blockHeight','blockHash','nid','nonce')
            # entries_to_remove = ('version','data','signature','blockHash','nid','nonce')

            for k in entries_to_remove:
                tx.pop(k, None)
            combined_tx = pd.json_normalize(tx)

            tx_results = None
            connection_refused_tx_count = 0
            while tx_results is None:
                try:
                    tx_results = icon_service.get_transaction_result(txHash)
                except:
                    connection_refused_count += 1
                    print(f'Connection refused {connection_refused_tx_count} time(s) (tx extraction B). Retrying...')
                    sleep(10)

            # removing some data here
            entries_to_remove = ('logsBloom','blockHeight','blockHash','to', 'scoreAddress')
            
            for k in entries_to_remove:
                tx_results.pop(k, None)
            combined_tx_results = pd.json_normalize(tx_results)

            tx_dfs = pd.merge(combined_tx, combined_tx_results, on=["txHash","txIndex"], how="left")

            return tx_dfs


        def run_tx():
                    start = time()
                    connection_refused_count = 0
                    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                        mytx = []
                        for result in tqdm(executor.map(get_tx_dfs, txHashes), total=len(txHashes)):
                            if result is None:
                                connection_refused_count += 1
                                print(f'Connection refused {connection_refused_count} time(s) (block extraction). Retrying...')
                                mytx.append(get_tx_dfs(txHashes))
                            else:
                                mytx.append(result)
                                
                    tx_df = pd.concat(mytx).reset_index(drop=True)
                    print(f'Time taken: {time() - start}')
                
                    return tx_df

        # def run_tx():
        #     start = time()

        #     with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        #         mytx = list(tqdm(executor.map(get_tx_dfs, txHashes), total=len(txHashes)))

        #     tx_df = pd.concat(mytx).reset_index(drop=True)
        #     print(f'Time taken: {time() - start}')

        #     return tx_df

        tx_df = run_tx()


        # =============================================================================
        # Token info
        # =============================================================================
        try:
            date_prev_underscore = date_prev.replace("-", "_")
            token_xfer_df = pd.read_csv(tokentransferPath.joinpath(f'{date_prev_underscore}/token_transfer_detail_{date_prev_underscore}.csv'), low_memory=False)
            token_xfer_df.rename(columns={'token_contract_address':'eventlogs_cx',
                                          'from_address': 'from',
                                          'dest_address': 'to',
                                          'transaction_hash': 'txHash',
                                          }, inplace=True)
            token_xfer_df = token_xfer_df.drop_duplicates()
            token_xfer_df_length = len(token_xfer_df[token_xfer_df['datetime'] == date_prev])
            
            token_xfer_df = token_xfer_df[token_xfer_df['datetime'] == date_prev]
            token_xfer_df = token_xfer_df[['eventlogs_cx','from','to','txHash','log_index','amount','symbol']]
        
        except FileNotFoundError:
            print("Token Transfer file not found")
            token_xfer_df = pd.DataFrame(columns=['eventlogs_cx', 'from', 'to', 'txHash', 'log_index', 'amount', 'symbol'])
            token_xfer_df_length = 0
        
        except Exception as e:
            print(f"An error occurred: {e}")
            token_xfer_df = pd.DataFrame(columns=['eventlogs_cx', 'from', 'to', 'txHash', 'log_index', 'amount', 'symbol'])
            token_xfer_df_length = 0
        
        # =============================================================================
        # Token address
        # =============================================================================
        try:
            token_addresses = load_from_json(tokenaddressPath.joinpath('token_addresses.json'))
        except FileNotFoundError:
            print("Token address file not found")
            token_addresses = None

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        def loop_to_icx(loop):
            return loop / 1000000000000000000
        
        # def loop_to_icx(loop):
        #     return Decimal(loop) / Decimal('1000000000000000000')
        
        def int_to_value(val):
            return Decimal(val) / Decimal('1000000')
        
        def is_hex(val):
            """
            Check if a given string is a valid hexadecimal number.
            """
            if isinstance(val, str) and re.fullmatch(r'0x[0-9a-fA-F]+', val):
                return True
            return False
        
        def parse_icx(val):
            if pd.isna(val) or not is_hex(val):
                return np.nan
            try:
                return loop_to_icx(int(val, 0))
            except (ZeroDivisionError, ValueError):
                return np.nan
        
        def hex_to_int(val):
            if pd.isna(val) or not is_hex(val):
                return np.nan
            try:
                return int(val, 0)
            except ValueError:
                print(f"Failed to convert {val} to int")
                return np.nan
            

        # convert timestamp to datetime
        def timestamp_to_date(df, timestamp, dateformat):
            return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)
        
        def df_merge_all(block_df, tx_df):
            # tx_all = pd.merge(block_df, tx_df, on=["txHash","timestamp"], how="left")
            tx_all = pd.merge(block_df, tx_df, on=["txHash"], how="left")
            return(tx_all)

        def tx_data_cleaning_1(tx_all):
            tx_all = tx_all.rename(columns={'cumulativeStepUsed':'stepUsedCumulative'})
            tx_all['stepPrice'] = pd.to_numeric(tx_all['stepPrice'], errors='coerce').astype(int).map(loop_to_icx).fillna(0)
            tx_all['stepUsed'] = pd.to_numeric(tx_all['stepUsed'], errors='coerce').astype(int).fillna(0)
            tx_all['tx_fees'] = tx_all['stepUsed'] * tx_all['stepPrice']

            tx_all['tx_date'] = timestamp_to_date(tx_all, 'timestamp', '%Y-%m-%d')
            tx_all['tx_time'] = timestamp_to_date(tx_all, 'timestamp', '%H:%M:%S')

            tx_all['value'] = loop_to_icx(tx_all['value'])
            # tx_all['tx_time_all'] = pd.to_datetime(tx_all['timestamp'] / 1000000, unit='s')
            return tx_all

        def get_list(df, strings):
            cols = list(df.columns.values)
            delete_list = [cols.index(i) for i in cols if strings in i]
            return df.columns[delete_list].tolist()

        def remove_list(cols, strings):
            delete_list = [cols.index(i) for i in cols if strings in i]
            for index in sorted(delete_list, reverse=True):
                del cols[index]

        def tx_data_cleaning_2(tx_all):
            # exploding event logs
            temp_tx = tx_all.explode('eventLogs').reset_index(drop=True).sort_index(axis=1)

            # giving empty {} to be able to json_normalize
            temp_tx['eventLogs'] = np.where(temp_tx['eventLogs'].isnull(), {}, temp_tx['eventLogs'])

            eventLogs_df = pd.json_normalize(temp_tx['eventLogs']).rename(columns={"scoreAddress":"eventLogs.scoreAddress","indexed":"eventLogs.indexed","data":"eventLogs.data"})
            df = temp_tx.join(eventLogs_df)

            # standardising columns
            df['eventLogs_indexed'] = df['eventLogs.indexed'].str[0]
            df['eventLogs_data'] = df['eventLogs.indexed'].str[1:] + df['eventLogs.data']

            # cleaning up
            df = df.drop(columns=['eventLogs.indexed', 'eventLogs.data'])
            df = df.rename(columns={"eventLogs_indexed":"eventLogs.indexed","eventLogs_data":"eventLogs.data"})
            df = df.drop(columns=["eventLogs","data"], axis=1, errors='ignore')

            # shifting data. and eventLogs. to the end
            cols = list(df.columns.values)  # Make a list of all of the columns in the df
            # data_list = get_list(df=df, strings="data.")
            eventlog_list = get_list(df=df, strings="eventLogs.")

            # remove_list(cols, strings="data.")
            remove_list(cols, strings="eventLogs.")

            # df = df[cols + eventlog_list + data_list]  # Create new dataframe with columns in the order you want
            df = df[cols + eventlog_list]  # Create new dataframe with columns in the order you want
                       
            def split_list_to_columns(lst, prefix='event'):
                max_len = max(lst.apply(len).max(), 1)
                return pd.DataFrame(lst.tolist(), index=lst.index).rename(
                    columns={i: f'{prefix}_{i+1}' for i in range(max_len)}
                )
            
            def add_token_info(df, column_name, token_addresses, prefix):
                mapped_data = df[column_name].map(token_addresses)
                mapped_df = pd.json_normalize(mapped_data).set_index(mapped_data.index)
                mapped_df = mapped_df.rename(columns={
                    'token_decimals': f'{prefix}_decimals',
                    'token_symbols': f'{prefix}_symbols'
                })
                return pd.concat([df, mapped_df], axis=1)
            
            def unique_ordered(original_list):
                seen = set()
                unique_list = []
                for item in original_list:
                    if item not in seen:
                        seen.add(item)
                        unique_list.append(item)
                return unique_list
            
            def safe_division(event_value, token_decimals):
                try:
                    event_value = int(event_value)
                    token_decimals = int(token_decimals)
                    return Decimal(event_value) / Decimal(10**token_decimals)
                except Exception as e:
                    print(f"Error processing entry: {e}")
                    return np.nan
            
            def find_pattern_index_vectorized(event_log_indexed, patterns):
                compiled_patterns = [re.compile(pattern.replace(',', r'\s*,\s*')) for pattern in patterns]
                match_starts = []
                matched_patterns = []
            
                for log in event_log_indexed:
                    if pd.isna(log):
                        match_starts.append(None)
                        matched_patterns.append(None)
                        continue
            
                    for pattern, compiled_pattern in zip(patterns, compiled_patterns):
                        match = compiled_pattern.search(log)
                        if match:
                            match_starts.append(match.start())
                            matched_patterns.append(pattern)
                            break
                    else:
                        match_starts.append(None)
                        matched_patterns.append(None)
            
                return match_starts, matched_patterns
            
            def extract_data_vectorized(df, patterns):
                # Ensure the eventLogs.data column contains lists
                if isinstance(df['eventLogs.data'].iloc[0], str):
                    df['eventLogs.data'] = df['eventLogs.data'].apply(eval)
            
                event_log_indexed = df['eventLogs.indexed']
                event_log_data = df['eventLogs.data']
            
                # Find pattern indices
                match_starts, matched_patterns = find_pattern_index_vectorized(event_log_indexed, patterns)
            
                # Create new columns
                df['match_start'] = match_starts
                df['matched_pattern'] = matched_patterns
            
                # Precompute extracted data positions
                def compute_positions(row):
                    match_start = row['match_start']
                    pattern = row['matched_pattern']
                    if match_start is not None and pattern is not None:
                        num_commas = len(re.findall(',', row['eventLogs.indexed'][:int(match_start)]))
                        pattern_elements = pattern.split(',')
                        return num_commas, pattern_elements
                    return None, None
            
                df['position_info'] = df.apply(compute_positions, axis=1)
            
                # Vectorized extraction
                def extract_elements(row):
                    pos_info = row['position_info']
                    if pos_info is not None:
                        pos, pattern_elements = pos_info
                        if pos is not None and isinstance(row['eventLogs.data'], list):
                            extracted_data = row['eventLogs.data'][pos:pos + len(pattern_elements)]
                            return extracted_data
                    return []
            
                df['extracted_data'] = df.apply(extract_elements, axis=1)
            
                # Assign values based on patterns
                def assign_values(row):
                    extracted_data = row['extracted_data']
                    if not extracted_data:
                        return row
            
                    pattern_elements = row['position_info'][1]
            
                    if len(extracted_data) >= 2:
                        if pattern_elements[:2] in (['Address', 'int'], ['str', 'int']):
                            row['event_to'] = extracted_data[0]
                            row['event_value_to'] = extracted_data[1]
                        elif pattern_elements[:2] in (['Address', 'bytes'], ['Address', 'str']):
                            row['event_to'] = extracted_data[0]
            
                    if len(extracted_data) >= 3:
                        if row['eventLogs.indexed'] in ['IScoreClaimedV2(Address,int,int)', 'FeeDistributed(Address,int,int)', 'Stake(Address,int,int)']:
                            row['event_to'] = extracted_data[0]
                            row['event_value_to'] = extracted_data[2]
                        elif row['eventLogs.indexed'] == 'LiquidityPurchased(int,int,int)':
                            row['event_to'] = extracted_data[0]
                            row['event_value_from'] = extracted_data[2]
                            row['event_value_to'] = extracted_data[1]
                        elif pattern_elements[:3] == ['Address', 'Address', 'bool']:
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[1]
                        elif pattern_elements[:3] == ['Address', 'int', 'int']:
                            row['event_to'] = extracted_data[0]
                            row['event_value_to'] = extracted_data[1]
                        else:
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[1]
                            row['event_value_from'] = extracted_data[2]
                            row['event_value_to'] = extracted_data[2]
            
                    if len(extracted_data) >= 4:
                        if row['eventLogs.indexed'] == 'ICXIssued(int,int,int,int)':
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[1]
                            row['event_value_from'] = extracted_data[2]
                            row['event_value_to'] = extracted_data[2]
                        elif row['eventLogs.indexed'] == 'FeeReceived(Address,int,int,Address)':
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[3]
                            row['event_value_from'] = extracted_data[2]
                            row['event_value_to'] = extracted_data[2]
                        elif row['eventLogs.indexed'] == 'WorkingBalanceUpdated(Address,Address,int,int)':
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[1]
                            row['event_value_from'] = '0x0'#'0x12'
                            row['event_value_to'] = '0x0'#'0x12'
                            
                        elif pattern_elements[:4] == ['Address', 'int', 'Address', 'int']:
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[2]
                            row['event_value_from'] = extracted_data[1]
                            row['event_value_to'] = extracted_data[3]
            
                    if len(extracted_data) >= 5:
                        if row['eventLogs.indexed'] == 'TransferSingle(Address,Address,Address,int,int)':
                            extracted_data = unique_ordered(extracted_data)
                            if len(extracted_data) >= 4:
                                row['event_from'] = extracted_data[0]
                                row['event_to'] = extracted_data[1]
                                row['event_value_from'] = extracted_data[3]
                                row['event_value_to'] = extracted_data[3]
                                
                        elif row['eventLogs.indexed'] == 'ReserveUpdated(Address,int,int,int,int)':
                            row['event_from'] = extracted_data[0]
                            row['event_to'] = extracted_data[1]
                            row['event_value_from'] = '0x0'#'0x12'
                            row['event_value_to'] = '0x0'#'0x12'                                
            
                        elif pattern_elements == ['Address', 'int', 'int', 'int', 'int']:
                            row['event_to'] = extracted_data[0]
                            row['event_value_to'] = extracted_data[1]
            
                    return row
            
                df = df.apply(assign_values, axis=1)
            
                return df
            
            patterns = [
                'Address,int,int,int,int',
                'Address,Address,Address,int,int',
                'Address,int,Address,int',
                'Address,int,int,Address',
                'Address,Address,int,int',
                'int,int,int,int',
                'Address,Address,bool',
                'Address,Address,int',
                'str,str,int',
                'Address,str,int',
                'str,Address,int',
                'Address,int,int',
                'int,int,int',
                'Address,int',
                'str,int',
                'Address,bytes',
                'Address,str',
            ]
            
            # Prepare DataFrame
            df_combined = df.copy()
            df_combined['event_from'] = np.nan
            df_combined['event_to'] = np.nan
            df_combined['event_value_from'] = np.nan
            df_combined['event_value_to'] = np.nan
            
            # Extract data using the optimized function
            df_combined = extract_data_vectorized(df_combined, patterns)
            df_combined.drop(columns=['match_start', 'matched_pattern', 'position_info', 'extracted_data'], inplace=True)

            # =============================================================================
            # 
            # =============================================================================

            # check1 = df[df['txHash'] == '0x65fe2188648cd7ba5a3c1a99c55335e8b76490690dc13dbfa487747ce8ac8a6e']
            # check1 = df_combined[df_combined['txHash'] == '0xdfe6f66fde2d2edf0710292b3b1e645dd3403662e0f9609b409119560b79a103']
            # check2 = token_xfer_df[token_xfer_df['transaction_hash'] == '0xdfe6f66fde2d2edf0710292b3b1e645dd3403662e0f9609b409119560b79a103']
            # check = df_combined[df_combined['txHash'] == '0xa5026bc4bb4a6a0d486ccda7999e9c3b956b5f9b9b8932ddc8a4572bd69d8c13']
            
            if token_addresses:
                
                # Map token information for 'eventLogs.scoreAddress'
                df_combined = add_token_info(df_combined, 'eventLogs.scoreAddress', token_addresses, 'scoreAddress')
                
                # Map token information for 'event_from'
                df_combined = add_token_info(df_combined, 'event_from', token_addresses, 'event_from')
                
                # Map token information for 'event_to'
                df_combined = add_token_info(df_combined, 'event_to', token_addresses, 'event_to')
                
                # df_copy = df_combined.copy()
                # df_combined = df_copy.copy()

                
                df_combined['token_decimals'] = (df_combined['scoreAddress_decimals'].fillna(df_combined['event_from_decimals']).fillna(df_combined['event_to_decimals']))
                df_combined['token_symbols'] = (df_combined['scoreAddress_symbols'].fillna(df_combined['event_from_symbols']).fillna(df_combined['event_to_symbols']))
                
                df_combined['event_value_from'] = (df_combined['event_value_from'].fillna(df_combined['event_value_to']))
                df_combined['event_value_to'] = (df_combined['event_value_to'].fillna(df_combined['event_value_from']))

                                
                df_combined['symbols_source'] = np.select(
                    [
                        df_combined['scoreAddress_symbols'].notna(),
                        df_combined['event_from_symbols'].notna(),
                        df_combined['event_to_symbols'].notna()
                    ],
                    [
                        'scoreAddress',  # Score Address is used
                        'event_from',    # Event From is used
                        'event_to'       # Event To is used
                    ],
                    default=np.nan
                    )
                                
                # Create 'event_value' based on the source of 'event_decimals' and 'event_symbols'
                df_combined['event_value'] = np.where(
                    df_combined['symbols_source'] == 'event_from', df_combined['event_value_from'],
                    np.where(
                        df_combined['symbols_source'] == 'event_to', df_combined['event_value_to'], 
                        df_combined['event_value_from']
                    )
                )

                # check = df_combined[df_combined['txHash'] == '0xdb3f211ef09aa4797c77a05fe3519e48771425301a9e6dca69ac5ef12dc2bdab']
                # check = df_combined[df_combined['txHash'] == '0x9baac9fafb9271ae9a47037e22065ad089b32a182b7e091994c31be779c87394']

                
                
                # df_combined['event_value'] = (df_combined['event_value_from'].fillna(df_combined['event_value_to']))
                # making nan 0 here            
                # df_combined['event_value_from'] = df_combined['event_value_from'].apply(hex_to_int).fillna(0)
                # df_combined['event_value_to'] = df_combined['event_value_to'].apply(hex_to_int).fillna(0)
                


                df_combined.drop(columns=['scoreAddress_decimals','scoreAddress_symbols', 
                                          'event_from_decimals', 'event_from_symbols', 
                                          'event_to_decimals', 'event_to_symbols',
                                          'event_value_from', 'event_value_to',
                                          'symbols_source'
                                          ], inplace=True)

                condition_icx = (df_combined['eventLogs.indexed'].str.contains('icx', case=False, regex=False) & df_combined['eventLogs.indexed'].notna())
                df_combined['token_decimals'] = np.where(condition_icx, 18, df_combined['token_decimals'])
                df_combined['token_symbols'] = np.where(condition_icx, 'ICX', df_combined['token_symbols'])

                condition_unknown = ((df_combined['token_symbols'] == '$unknown') & df_combined['token_symbols'].notna())
                df_combined['token_decimals'] = np.where(condition_unknown, np.nan, df_combined['token_decimals'])
                
                df_combined['event_value'] = df_combined['event_value'].apply(hex_to_int)#.fillna(0)
                # df_combined['token_decimals'].fillna(18, inplace=True)
                # df_combined['token_symbols'].fillna('ICX', inplace=True)


            df_temp = df_combined[['event_value', 'token_decimals']]
            data_dict = df_temp.to_dict('records')
            # data_dict = [safe_division(entry) for entry in data_dict]
            data_dict = [safe_division(entry['event_value'], entry['token_decimals']) for entry in data_dict]
            df_combined['event_value_decimals'] = pd.DataFrame(data_dict)
            df_combined['eventlogs'] = df_combined['eventLogs.indexed'].str.split('(', expand=True)[0]
                        
            
            keep_cols = ['timestamp', 'dataType', 'txIndex', 'status', 'failure.code', 'failure.message', 'txHash', 'from', 'to', 
                         'tx_date', 'tx_fees', 'tx_time', 'value', 'eventLogs.scoreAddress', 'eventLogs.indexed', 'event_from', 'event_to',
                         'event_value_decimals', 'token_symbols'
                         # 'event_value', 'token_decimals',
                         ]
            
            df_combined = df_combined[keep_cols]
            
            df_combined_main = df_combined[['timestamp', 'dataType', 'txIndex', 'status', 'failure.code', 'failure.message', 
                                            'txHash', 'from', 'to', 'tx_date', 'tx_fees', 'tx_time', 'value']].drop_duplicates()
            
            df_combined_main['tx_type'] = 'main'
            # np.where(df_combined_main['value'] != np.nan, 'ICX', np.nan)
            
            # check = df_combined_main[df_combined_main['txHash'] == '0xdb3f211ef09aa4797c77a05fe3519e48771425301a9e6dca69ac5ef12dc2bdab']

            
            df_combined_event = df_combined[['timestamp', 'dataType', 'txIndex', 'status', 'failure.code', 'failure.message', 
                                             'txHash', 'tx_date', 'tx_fees', 'tx_time', 
                                             'eventLogs.scoreAddress', 'eventLogs.indexed', 'event_from', 'event_to', 'token_symbols', 
                                             # 'event_value', 'token_decimals', 
                                             'event_value_decimals'
                                             ]]\
                .rename(columns={'event_from': 'from', 'event_to': 'to', 'event_value_decimals': 'value'})
            
            df_combined_event['tx_type'] = 'event'
            
            # check = df_combined_event[df_combined_event['txHash'] == '0xdb3f211ef09aa4797c77a05fe3519e48771425301a9e6dca69ac5ef12dc2bdab']
            # check = df_combined_event[df_combined_event['txHash'] == '0x9baac9fafb9271ae9a47037e22065ad089b32a182b7e091994c31be779c87394']

            columns_to_check = ['from', 'to']
            df_combined_main = df_combined_main.dropna(subset=columns_to_check)
            
            df_combined = pd.concat([df_combined_main, df_combined_event])
            
            # check = df_combined[df_combined['txHash'] == '0xdb3f211ef09aa4797c77a05fe3519e48771425301a9e6dca69ac5ef12dc2bdab']


            df_combined['p2p'] = df_combined['from'].str.startswith('hx') & df_combined['to'].str.startswith('hx')
            df_combined['p2c'] = df_combined['from'].str.startswith('hx') & df_combined['to'].str.startswith('cx')
            df_combined['c2p'] = df_combined['from'].str.startswith('cx') & df_combined['to'].str.startswith('hx')
            df_combined['c2c'] = df_combined['from'].str.startswith('cx') & df_combined['to'].str.startswith('cx')
            
            df_combined.loc[:,'regTxCount'] = np.where(df_combined['tx_type'] == 'main', 1, 0)
            df_combined.loc[:,'intTxCount'] = np.where((df_combined['tx_type'] == 'event') & df_combined[['p2p','p2c','c2p','c2c']].any(axis=1), 1, 0)
            df_combined.loc[:,'systemTickCount'] = np.where((df_combined['txIndex'] == 0) & (df_combined['regTxCount'] !=1) & (df_combined['intTxCount'] != 1), 1, 0)
            df_combined.loc[:,'intEvtCount'] = np.where((df_combined['regTxCount'] !=1) & (df_combined['systemTickCount'] != 1) & (df_combined['intTxCount'] != 1), 1, 0)

            df_combined['eventlogs'] = df_combined['eventLogs.indexed'].str.split('(', expand=True)[0]
            df_combined.drop(columns=['eventLogs.indexed'], inplace=True)
            df_combined.rename(columns={'eventLogs.scoreAddress':'eventlogs_cx'}, inplace=True)
            
        
            # condition = (df_combined['regTxCount'] != 1) & (df_combined['systemTickCount'] != 1)# & (df_combined['value'].notna())
            condition = (df_combined['intTxCount'] == 1) | (df_combined['intEvtCount'] == 1)# & (df_combined['value'].notna())
            df_combined.loc[condition, 'log_index'] = df_combined[condition].groupby('txHash').cumcount()
                        
            if token_xfer_df_length != 0:
                df_output = pd.merge(df_combined, token_xfer_df, on=['eventlogs_cx', 'from', 'to', 'txHash', 'log_index'], how='left')
                df_output['symbol'] = np.where((df_output['tx_type'] == 'main') & df_output['value'].notna(), 'ICX', df_output['symbol'])
                df_output['value'] = np.where((df_output['tx_type'] == 'event') & df_output['value'].isna(), df_output['amount'], df_output['value'])
                df_output.drop(columns='amount', inplace=True)
            else:
                df_output = df_combined
                df_output['symbol'] = np.where((df_output['tx_type'] == 'main') & df_output['value'].notna(), 'ICX', np.nan)
                
            # check = df_output[df_output['txHash'] == '0xdb3f211ef09aa4797c77a05fe3519e48771425301a9e6dca69ac5ef12dc2bdab']
            # check = df_output[df_output['txHash'] == '0x9baac9fafb9271ae9a47037e22065ad089b32a182b7e091994c31be779c87394']

            df_output['symbol'] = np.where(df_output['symbol'].isna(), df_output['token_symbols'], df_output['symbol'])
            df_output.drop(columns=['token_symbols'], inplace=True)
            df_output['value'] = df_output['value'].astype('float64')

            int_tx_event = df_output.groupby('txHash').agg('sum')[['regTxCount', 'intTxCount', 'intEvtCount', 'systemTickCount']].reset_index()

            tx_all = pd.merge(tx_all, int_tx_event, on='txHash', how='left')

            return tx_all, df_output

        def tx_data_cleaning_3(df):
            cols = list(df.columns.values)  # Make a list of all of the columns in the df
            remove_list(cols, strings="step")
            df = df[cols]
            df = df.drop(columns=['eventLogs'])
            return df

        def final_output():
            tx_all = tx_data_cleaning_1(tx_all=tx_df.copy())
            tx_all, df_combined = tx_data_cleaning_2(tx_all=tx_all)
            # tx_all = df_merge_all(block_df=block_df, tx_df=tx_all)
            tx_all = df_merge_all(block_df=tx_all, tx_df=block_df.drop(columns='timestamp'))

            final_tx_df = tx_data_cleaning_3(df=tx_all)
            final_tx_df['dataType'] = final_tx_df['dataType'].fillna('unknown')
            
            # in case there are some nans, we'll just fill with 0
            final_tx_df = final_tx_df.fillna(0) 

            return final_tx_df, df_combined

        final_tx_df, df_combined = final_output()

        # TODO: Save final_tx_df when completed
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # saving
        # windows_path = "/mnt/e/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
        # final_tx_df.to_csv(os.path.join(dataPath, 'tx_final_' + date_prev + '.csv'), index=False)
        df_combined.to_csv(os.path.join(dataPath, 'tx_detail_' + date_prev + '.csv'), index=False)
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        # bytearray.fromhex(dat[2:]).decode()
        # import binascii 
        # string = '{"msgType":"JejuVisitLog","time":1626533340,"period":60,"count":17}'
        # binascii.hexlify(string.encode())

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        print()
        print(date_prev + ' is done!\n')

    except:
        print('\nNo data found for ' + date_prev + '!!!\n')


def run_all():
    if __name__ == "__main__":
        run_all()
