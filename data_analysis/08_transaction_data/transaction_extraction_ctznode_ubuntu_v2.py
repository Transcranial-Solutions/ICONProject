#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 20:56:03 2024

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


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

workers = 1


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

def yesterday(doi = "2021-08-20"):
    yesterday = datetime.fromisoformat(doi) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20"):
    tomorrow = datetime.fromisoformat(doi) + timedelta(1)
    return tomorrow.strftime('%Y-%m-%d')

def extract_date(df, date):
    df['date'] = pd.to_datetime(df['block_createDate']).dt.strftime("%Y-%m-%d")
    return df[df['date'] == date]



# today's date
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")


# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 0
date_prev = "2022-07-12"

if use_specific_prev_date == 1:
    date_of_interest = [date_prev]
elif use_specific_prev_date == 0:
    date_of_interest = [yesterday(date_today)]
elif use_specific_prev_date == 2:
    # for loop between dates
    day_1 = "2022-07-13"; day_2 = "2022-07-16"
    date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()
else:
    date_of_interest=[]
    print('No date selected.')

print(date_of_interest)


# remote = "https://ctz.solidwallet.io/api/v3"
# remote = "http://52.79.77.39:9000/api/v3" # 
remote = "http://52.196.159.184:9000/api/v3"

data = {'jsonrpc':'2.0', 'method': 'icx_getLastBlock','id': 1223}
def get_height(endpoint: str) -> int:
        payload = json.dumps(data)
        payload = payload.encode()
        req = requests.post(endpoint, data=payload)
        response_data = req.json()
        return response_data["result"]["height"]
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

        # combined_df.to_csv(os.path.join(dataPath, 'transaction_blocks_' + date_prev + '.csv'), index=False)


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

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        def loop_to_icx(loop):
            icx = loop / 1000000000000000000
            return(icx)

        # convert timestamp to datetime
        def timestamp_to_date(df, timestamp, dateformat):
            return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)
        
        def df_merge_all(block_df, tx_df):
            # tx_all = pd.merge(block_df, tx_df, on=["txHash","timestamp"], how="left")
            tx_all = pd.merge(block_df, tx_df, on=["txHash"], how="left")
            return(tx_all)

        def tx_data_cleaning_1(tx_all):
            tx_all = tx_all.rename(columns={'cumulativeStepUsed':'stepUsedCumulative'})
            tx_all['stepPrice'] = pd.to_numeric(tx_all['stepPrice'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)
            tx_all['stepUsed'] = pd.to_numeric(tx_all['stepUsed'], errors='coerce').astype('Int64').fillna(0)
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
            
            # p2p / p2c / c2p / c2c?

            # def has_pair_starting_with_hx_cx(lst):
            #     if not isinstance(lst, list):
            #         return False
                
            #     hx_count = 0
            #     cx_count = 0
                
            #     for item in lst:
            #         if isinstance(item, str) and not pd.isnull(item):
            #             if item.startswith('hx'):
            #                 hx_count += 1
            #             elif item.startswith('cx'):
            #                 cx_count += 1
                            
            #     return (hx_count >= 1) or (cx_count >= 2) or (hx_count >= 1 and cx_count >= 1)


            
            # def any_starts_with_hx(lst):
            #     return any(isinstance(item, str) and item.startswith('hx') for item in lst if not pd.isnull(item))
            
            # def extract_hx_element(lst):
            #     if isinstance(lst, list):
            #         for item in lst:
            #             if isinstance(item, str) and item.startswith('hx'):
            #                 return item
            #     return None
            
            def split_list_to_columns(lst, prefix='event'):
                max_len = max(lst.apply(len).max(), 1)  # Find the maximum list length
                return pd.DataFrame(lst.tolist(), index=lst.index).rename(
                    columns={i: f'{prefix}_{i+1}' for i in range(max_len)}
                )
            
            
            # def label_columns(value):
            #     if isinstance(value, str):
            #         if value.startswith('hx'):
            #             return 'hx'
            #         elif value.startswith('cx'):
            #             return 'cx'
            #         else:
            #             return 'other'
            #     return np.nan
            
            

            
            df_expanded = split_list_to_columns(df['eventLogs.data'].dropna())[['event_1','event_2']]
            df_expanded.rename(columns={'event_1':'event_from', 'event_2': 'event_to'}, inplace=True)
            df_combined = df.join(df_expanded, rsuffix='_original')
            
            df_combined['p2p_main'] = df_combined['from'].str.startswith('hx') & df_combined['to'].str.startswith('hx')
            df_combined['p2c_main'] = df_combined['from'].str.startswith('hx') & df_combined['to'].str.startswith('cx')
            df_combined['c2p_main'] = df_combined['from'].str.startswith('cx') & df_combined['to'].str.startswith('hx')
            df_combined['c2c_main'] = df_combined['from'].str.startswith('cx') & df_combined['to'].str.startswith('cx')

            df_combined['p2p_event'] = df_combined['event_from'].str.startswith('hx') & df_combined['event_to'].str.startswith('hx')
            df_combined['p2c_event'] = df_combined['event_from'].str.startswith('hx') & df_combined['event_to'].str.startswith('cx')
            df_combined['c2p_event'] = df_combined['event_from'].str.startswith('cx') & df_combined['event_to'].str.startswith('hx')
            df_combined['c2c_event'] = df_combined['event_from'].str.startswith('cx') & df_combined['event_to'].str.startswith('cx')
            

            
            
            # test = df['eventLogs.data'].apply(has_pair_starting_with_hx_cx)
            
            # df.loc[:, 'intTxCount'] = np.where(df['eventLogs.data'].apply(lambda x: any_starts_with_hx(x) if isinstance(x, list) else False), 1, 0)
            
            # df.loc[:, 'intTx_hx'] = df['eventLogs.data'].apply(extract_hx_element)

            # counting internal transactions / total events
            # df.loc[:, 'intTxCount'] = np.where(df['eventLogs.indexed'].str.contains('Address', na=False), 1, 0)
            # df.loc[:,'intEvtCount'] = np.where(df['eventLogs.data'].notnull(), 1, 0)

            int_tx_event = df.groupby('txHash').agg('sum')[['intTxCount', 'intEvtCount']].reset_index()
            # print(int_tx_event)

            tx_all = pd.merge(tx_all, int_tx_event, on='txHash', how='left')

            return tx_all

        def tx_data_cleaning_3(df):
            cols = list(df.columns.values)  # Make a list of all of the columns in the df
            remove_list(cols, strings="step")
            df = df[cols]
            # df = df.drop(columns=['eventLogs'])
            return df

        def final_output():
            tx_all = tx_data_cleaning_1(tx_all=tx_df.copy())
            tx_all, df_int_tx = tx_data_cleaning_2(tx_all=tx_all)
            # tx_all = df_merge_all(block_df=block_df, tx_df=tx_all)
            tx_all = df_merge_all(block_df=tx_all, tx_df=block_df.drop(columns='timestamp'))

            final_tx_df = tx_data_cleaning_3(df=tx_all)
            final_tx_df['dataType'] = final_tx_df['dataType'].fillna('unknown')
            
            # in case there are some nans, we'll just fill with 0
            final_tx_df = final_tx_df.fillna(0) 

            return final_tx_df

        final_tx_df = final_output()


        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # saving
        # windows_path = "/mnt/e/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
        # final_tx_df.to_csv(os.path.join(dataPath, 'tx_final_' + date_prev + '.csv'), index=False)
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        # bytearray.fromhex(dat[2:]).decode()
        # import binascii 
        # string = '{"msgType":"JejuVisitLog","time":1626533340,"period":60,"count":17}'
        # binascii.hexlify(string.encode())

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        print(date_prev + ' is done!')

    except:
        print('No data found for ' + date_prev + '!!!')


def run_all():
    if __name__ == "__main__":
        run_all()
