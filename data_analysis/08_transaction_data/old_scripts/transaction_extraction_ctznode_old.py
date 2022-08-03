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

# currPath = os.getcwd()
currPath = "/home/tonoplast/IconProject/"
projectPath = os.path.join(currPath, "transaction_data")
if not os.path.exists(projectPath):
    os.mkdir(projectPath)
    
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

# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(1)
    if string:
        return yesterday.strftime('%Y-%m-%d')
    return yesterday

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


# today's date
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")

# to use specific date, otherwise use yesterday
use_specific_prev_date = 1
date_prev = "2021-07-28"

if use_specific_prev_date == 1:
    date_prev = date_prev
else:
    date_prev = yesterday(today)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ solidwallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# using solidwallet
# icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
# icon_service = IconService(HTTPProvider("http://localhost:9000", 3))
# icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io", 3, request_kwargs={"timeout": 60}))

icon_service = IconService(HTTPProvider("http://127.0.0.1:9000", 3, request_kwargs={"timeout": 60}))
# block = icon_service.get_block(1000) 


## Creating Wallet (only done for the first time)
# wallet = KeyWallet.create()
# wallet.get_address()
# wallet.get_private_key()
tester_wallet = os.path.join(walletPath, "test_keystore_1")
# wallet.store(tester_wallet, "abcd1234*")

wallet = KeyWallet.load(tester_wallet, "abcd1234*")
tester_address = wallet.get_address()



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ block Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

# if not os.path.exists(windows_path):
#     os.makedirs(windows_path)

combined_df = pd.read_csv(os.path.join(windows_path, 'transaction_blocks_' + date_prev + '.csv'))

combined_df['date'] = pd.to_datetime(combined_df['block_createDate']).dt.strftime("%Y-%m-%d")
df_of_interest = combined_df[combined_df['date'] == date_prev]

df_of_interest = df_of_interest[df_of_interest['block_fee'] != '0']
block_of_interest = df_of_interest['blockHeight']

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Other way ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

def get_block_df(blockInfo):
    blockInfo = icon_service.get_block(blockInfo)
    block_blockheight = deep_get(blockInfo, "height")
    blockTS = deep_get(blockInfo, "time_stamp")
    txHashes = extract_values_no_params(blockInfo, "txHash")
    txTS = extract_values(blockInfo, "timestamp")
    
    combined_block = {'blockHeight': block_blockheight,
        'block_timestamp': blockTS,
        'txHash': txHashes,
        'tx_timestamp': txTS}

    block_df = pd.DataFrame(data=combined_block)
    return block_df


def get_tx_df(txHash):
    tx = icon_service.get_transaction(txHash)
    tx_blockHeight = deep_get(tx, 'blockHeight')
    tx_txHash = deep_get(tx, "txHash")
    tx_timestamp = deep_get(tx, 'timestamp')
    tx_from = deep_get(tx, 'from')
    tx_to = deep_get(tx, 'to')
    tx_value = extract_values(tx, 'value')
    tx_data = deep_get(tx, "data")
    tx_dataType = deep_get(tx, "dataType")

    combined_tx = {'blockHeight': tx_blockHeight,
        'txHash': tx_txHash,
        'tx_timestamp': tx_timestamp,
        'tx_from': tx_from,
        'tx_to': tx_to,
        'tx_value': [tx_value],
        'tx_data': [tx_data],
        'tx_dataType': tx_dataType}
    
    tx_df = pd.DataFrame(data=combined_tx)

    return tx_df   


# probably not use this because this might not be correct
# def get_eventlogs_tx_data(tx_results):
#     eventlog_output=[]
#     for i in tx_results['eventLogs']:
#         eventlog = deep_get(i, "indexed")
#         if "transfer" in eventlog[0].lower() and eventlog[1].startswith(("hx","cx")) and eventlog[2].startswith(("hx","cx")):
#             eventlog_output.append(eventlog)
#         else:
#             pass
#         eventlog_output.append(eventlog)
#     if not eventlog_output:
#         # eventlog_output = ''
#         eventlog_output = np.nan
#     return eventlog_output



def get_tx_results_df(txHash):
    tx_results = icon_service.get_transaction_result(txHash)
    txResults_txHash = deep_get(tx_results, 'txHash')
    txResults_status = deep_get(tx_results, 'status')
    txResults_txIndex = deep_get(tx_results, 'txIndex')
    txResults_stepUsed = deep_get(tx_results, 'stepUsed')
    txResults_stepPrice = deep_get(tx_results, 'stepPrice')
    # txResults_eventlogs = dict(enumerate(get_eventlogs_tx_data(tx_results)))
    txResults_eventlogs = deep_get(tx_results, 'eventLogs')

    combined_txResults = {'txHash': txResults_txHash,
        'status': txResults_status,
        'txIndex': txResults_txIndex,
        'stepUsed': txResults_stepUsed,
        'stepPrice': txResults_stepPrice,
        'eventlogs': [txResults_eventlogs],}

    tx_results_df = pd.DataFrame(data=combined_txResults)

    return tx_results_df


# block_of_interest = block_of_interest.iloc[0:50]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting contact info using multithreading

# def run_block():
start = time()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    myblock = list(tqdm(executor.map(get_block_df, block_of_interest), total=len(block_of_interest)))

block_df = pd.concat(myblock).reset_index(drop=True)
print(f'Time taken: {time() - start}')

# if __name__ == "__main__":
#     run_block()





# def not_done():
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

# saving block data balance
block_df.to_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'), index=False)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# random_sleep_except = random.uniform(600,900)
# print("I'll pause for"+str(random_sleep_except/60) + " minutes before doing another... \n")
# time.sleep(random_sleep_except) #sleep the script for x seconds and....#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"
block_df = pd.read_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'))

txHashes = block_df['txHash']

# txHashes = txHashes.iloc[0:1000]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting contact info using multithreading

start = time()

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    mytx = list(tqdm(executor.map(get_tx_df, txHashes), total=len(txHashes)))
    mytx_results = list(tqdm(executor.map(get_tx_results_df, txHashes), total=len(txHashes)))


tx_df = pd.concat(mytx).reset_index(drop=True)
tx_results_df = pd.concat(mytx_results).reset_index(drop=True)

print(f'Time taken: {time() - start}')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)


# convert timestamp to datetime
def timestamp_to_date(df, timestamp, dateformat):
    return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)

def df_merge_all(tx_df=tx_df, tx_results_df=tx_results_df, block_df=block_df):
    tx_all = pd.merge(tx_df, tx_results_df, on="txHash", how="left")
    tx_all = pd.merge(block_df, tx_all, on=["txHash","tx_timestamp","blockHeight"], how="left")
    return(tx_all)

tx_all = df_merge_all()



# def explode_eventlogs(tx_all=tx_all):
tx_all = tx_all.explode("eventlogs").reset_index(drop=True)
new_column_list = ['int_tx_details','int_tx_from_address','int_tx_to_address','int_tx_amount']
tx_all_int_no = tx_all[tx_all['eventlogs'].isnull()]
tx_all_int_yes = tx_all[tx_all['eventlogs'].notnull()]
tx_all_int_yes[new_column_list] = pd.DataFrame(tx_all_int_yes.eventlogs.tolist(), index= tx_all_int_yes.index)

37543688

tx_all_int_yes['int_tx_amount'] = pd.to_numeric(tx_all_int_yes['int_tx_amount'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)

tx_all_int_yes.iloc[25]['txHash']


check = tx_all_int_yes[tx_all_int_yes['int_tx_amount'] == 'cxf000000000000000000000000000000000000000']

check1 = tx_all_int_yes[tx_all_int_yes['txHash'] == '0xe5b1ba8d7efe8f07df45eb18badd0c35bf6dd3d3802ef783d25f436481a6b2c4']

# txHash = '0xe5b1ba8d7efe8f07df45eb18badd0c35bf6dd3d3802ef783d25f436481a6b2c4'

check2 = tx_all[tx_all['txHash'] == '0xe5b1ba8d7efe8f07df45eb18badd0c35bf6dd3d3802ef783d25f436481a6b2c4']




pd.json_normalize(tx_results['eventLogs'])



tx_all = tx_all_int_yes.append(tx_all_int_no).sort_values(by='tx_timestamp').reset_index(drop=True)
# return(tx_all)

tx_all = explode_eventlogs()


tx_all['stepPrice'] = pd.to_numeric(tx_all['stepPrice'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)
tx_all['stepUsed'] = pd.to_numeric(tx_all['stepUsed'], errors='coerce').astype('Int64').fillna(0)
tx_all['tx_fees'] = tx_all['stepUsed'] * tx_all['stepPrice']

# tx_all['int_tx_amount'] = pd.to_numeric(tx_all['int_tx_amount'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)

# tx_all['block_date'] = timestamp_to_date(tx_all, 'block_timestamp', '%Y-%m-%d')
tx_all['block_time'] = timestamp_to_date(tx_all, 'block_timestamp', '%H:%M:%S')
tx_all['tx_date'] = timestamp_to_date(tx_all, 'tx_timestamp', '%Y-%m-%d')
tx_all['tx_time'] = timestamp_to_date(tx_all, 'tx_timestamp', '%H:%M:%S')


tx_all.to_csv(os.path.join(windows_path, 'tx_final_' + date_prev + '.csv'), index=False)




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# bytearray.fromhex(dat[2:]).decode()
# import binascii 
# string = '{"msgType":"JejuVisitLog","time":1626533340,"period":60,"count":17}'
# binascii.hexlify(string.encode())

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# tx_df.to_csv(os.path.join(windows_path, 'tx_' + date_prev + '.csv'), index=False)
# tx_results_df.to_csv(os.path.join(windows_path, 'tx_results_' + date_prev + '.csv'), index=False)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# def run_block():
#     if __name__ == "__main__":
#         run_block()