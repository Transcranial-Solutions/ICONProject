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
# from iconsdk.builder.call_builder import CallBuilder
# from typing import Union
from time import time
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random
import codecs

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

workers = 8

currPath = os.getcwd()
if not "08_transaction_data" in currPath:
    projectPath = os.path.join(currPath, "08_transaction_data")
    if not os.path.exists(projectPath):
        os.mkdir(projectPath)
else:
    projectPath = currPath
    
dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)



def insert_str(string="YYYY-WW", index=4, timeinterval=' week') -> str:
    """
    Inserting strings between strings
    """
    return string[:index] + timeinterval + string[index:]

# # get yesterday function
# def yesterday(string=False):
#     yesterday = datetime.utcnow() - timedelta(1)
#     if string:
#         return yesterday.strftime('%Y-%m-%d')
#     return yesterday

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
use_specific_prev_date = 1
date_prev = "2021-08-11"

if use_specific_prev_date == 1:
    date_of_interest = [date_prev]
elif use_specific_prev_date == 0:
    date_of_interest = [yesterday(date_today)]
elif use_specific_prev_date == 2:
    # for loop between dates
    day_1 = "2021-08-12"; day_2 = "2021-08-19"
    date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()
else:
    date_of_interest=[]
    print('No date selected.')

print(date_of_interest)


for date_prev in date_of_interest:

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ solidwallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # using solidwallet
    # icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
    # icon_service = IconService(HTTPProvider("http://localhost:9000", 3))
    # icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io", 3, request_kwargs={"timeout": 60}))
    # icon_service = IconService(HTTPProvider("http://localhost:9000/api/v3", request_kwargs={"timeout": 60}))

    # localhost
    icon_service = IconService(HTTPProvider("http://127.0.0.1:9000/api/v3", request_kwargs={"timeout": 60}))
    # block = icon_service.get_block(38000000)


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

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ block Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # reading from the data extracted previously
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
        combined_df = pd.read_csv(os.path.join(dataPath, 'transaction_blocks_' + date_prev + '.csv'))
        print('Running ' + date_prev + ' data...')

        combined_df = extract_date(combined_df, date_prev)
        # combined_df['date'] = pd.to_datetime(combined_df['block_createDate']).dt.strftime("%Y-%m-%d")
        # df_of_interest = combined_df[combined_df['date'] == date_prev]

        block_of_interest = combined_df['blockHeight']
        # block_of_interest = block_of_interest.iloc[0:100]

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Other way ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        def get_block_df(blockInfo):
            blockInfo = icon_service.get_block(blockInfo)
            block_blockheight = deep_get(blockInfo, "height")
            # blockTS = deep_get(blockInfo, "time_stamp")
            txHashes = extract_values_no_params(blockInfo, "txHash")
            txTS = extract_values(blockInfo, "timestamp")

            combined_block = {'blockHeight': block_blockheight,
                # 'block_timestamp': blockTS,
                'txHash': txHashes,
                'timestamp': txTS}

            block_df = pd.DataFrame(data=combined_block)
            return block_df

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # collecting contact info using multithreading

        def run_block():
            start = time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                myblock = list(tqdm(executor.map(get_block_df, block_of_interest), total=len(block_of_interest)))

            block_df = pd.concat(myblock).reset_index(drop=True)
            print(f'Time taken: {time() - start}')

            return block_df

        block_df = run_block()

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # # save block info
        # block_df.to_csv(os.path.join(dataPath, 'transactions_each_block_' + date_prev + '.csv'), index=False)
        # #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # # load block info
        # block_df = pd.read_csv(os.path.join(dataPath, 'transactions_each_block_' + date_prev + '.csv'))
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        # getting hash only
        txHashes = block_df['txHash']
        # txHashes = txHashes.iloc[0:200]

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # collecting transaction info using multithreading
        def get_tx_dfs(txHash):
            tx = icon_service.get_transaction(txHash)

            # removing some data here
            entries_to_remove = ('version','data','signature','blockHeight','blockHash','nid','nonce')
            for k in entries_to_remove:
                tx.pop(k, None)
            combined_tx = pd.json_normalize(tx)



            tx_results = icon_service.get_transaction_result(txHash)

            # removing some data here
            entries_to_remove = ('logsBloom','blockHeight','blockHash','to', 'scoreAddress')
            for k in entries_to_remove:
                tx_results.pop(k, None)
            combined_tx_results = pd.json_normalize(tx_results)

            tx_dfs = pd.merge(combined_tx, combined_tx_results, on=["txHash","txIndex"], how="left")

            return tx_dfs

        def run_tx():
            start = time()

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                mytx = list(tqdm(executor.map(get_tx_dfs, txHashes), total=len(txHashes)))

            tx_df = pd.concat(mytx).reset_index(drop=True)
            print(f'Time taken: {time() - start}')

            return tx_df

        tx_df = run_tx()

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
        # loop to icx converter
        def loop_to_icx(loop):
            icx = loop / 1000000000000000000
            return(icx)

        # convert timestamp to datetime
        def timestamp_to_date(df, timestamp, dateformat):
            return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)

        def df_merge_all(block_df, tx_df):
            tx_all = pd.merge(block_df, tx_df, on=["txHash","timestamp"], how="left")
            return(tx_all)

        def tx_data_cleaning_1(tx_all):
            tx_all = tx_all.rename(columns={'cumulativeStepUsed':'stepUsedCumulative'})
            tx_all['stepPrice'] = pd.to_numeric(tx_all['stepPrice'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)
            tx_all['stepUsed'] = pd.to_numeric(tx_all['stepUsed'], errors='coerce').astype('Int64').fillna(0)
            tx_all['tx_fees'] = tx_all['stepUsed'] * tx_all['stepPrice']

            tx_all['tx_date'] = timestamp_to_date(tx_all, 'timestamp', '%Y-%m-%d')
            tx_all['tx_time'] = timestamp_to_date(tx_all, 'timestamp', '%H:%M:%S')

            tx_all['value'] = loop_to_icx(tx_all['value'])
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


            # counting internal transactions / total events
            df['intTxCount'] = np.where(df['eventLogs.indexed'].str.contains('Address', na=False), 1, 0)
            df['intEvtCount'] = np.where(df['eventLogs.data'].notnull(), 1, 0)

            int_tx_event = df.groupby('txHash').agg('sum')[['intTxCount', 'intEvtCount']].reset_index()
            # print(int_tx_event)

            tx_all = pd.merge(tx_all, int_tx_event, on='txHash', how='left')

            return tx_all

        def tx_data_cleaning_3(df):
            cols = list(df.columns.values)  # Make a list of all of the columns in the df
            remove_list(cols, strings="step")
            df = df[cols]
            df = df.drop(columns=['eventLogs'])

            # shifting columns (from - to) together
            # cols = list(df.columns.values)
            # to_loc = df.columns.get_loc("to")
            # from_loc = df.columns.get_loc("from")
            # df = df[cols[0:from_loc + 1] + [cols[to_loc]] + cols[from_loc + 1:to_loc] + cols[to_loc + 1:]]
            return df


        def final_output():
            tx_all = tx_data_cleaning_1(tx_all=tx_df)
            tx_all = tx_data_cleaning_2(tx_all=tx_all)
            tx_all = df_merge_all(block_df=block_df, tx_df=tx_all)
            final_tx_df = tx_data_cleaning_3(df=tx_all)

            return final_tx_df

        final_tx_df = final_output()


        final_tx_df.to_csv(os.path.join(dataPath, 'tx_final_' + date_prev + '.csv'), index=False)

        # check = pd.read_csv(os.path.join(dataPath, 'tx_final_' + date_prev + '.csv'), low_memory=False)

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