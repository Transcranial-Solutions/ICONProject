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
# import matplotlib.pyplot as plt
# from pandas.core.groupby.generic import DataFrameGroupBy
# import pylab as pl
import os
from functools import reduce
# import numpy as np
# from concurrent.futures import ThreadPoolExecutor, as_completed
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
        results=''
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
        results=''
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
        out_data = ''
    return out_data

def deep_get(dictionary, keys):
    return reduce(deep_get_imps, keys.split("."), dictionary)


# today's date
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")

# to use specific date, otherwise use yesterday
use_specific_prev_date = 0
date_prev = "2021-02-08"

if use_specific_prev_date == 1:
    date_prev = date_prev
else:
    date_prev = yesterday(today)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ solidwallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# using solidwallet
# icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
icon_service = IconService(HTTPProvider("http://localhost:9000", 3))

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
    # return combined_block

def get_tx_df(txHash):
    tx = icon_service.get_transaction(txHash)
    tx_blockHeight = deep_get(tx, 'blockHeight')
    tx_txHash = deep_get(tx, "txHash")
    tx_timestamp = deep_get(tx, 'timestamp')
    tx_from = deep_get(tx, 'from')
    tx_to = deep_get(tx, 'to')
    tx_value = deep_get(tx, 'value')
    tx_data = deep_get(tx, "data")
    tx_dataType = deep_get(tx, "dataType")

    combined_tx = {'blockHeight': tx_blockHeight,
        'txHash': tx_txHash,
        'tx_timestamp': tx_timestamp,
        'tx_from': tx_from,
        'tx_to': tx_to,
        'tx_value': tx_value,
        'tx_data': [tx_data],
        'tx_dataType': tx_dataType}
    
    # tx_df = pd.DataFrame(data=combined_tx, index=[0])
    tx_df = pd.DataFrame(data=combined_tx)

    return tx_df   

def get_eventlogs_tx_data(tx_results):
    eventlog_output=[]
    for i in tx_results['eventLogs']:
        eventlog = deep_get(i, "indexed")
        if "transfer" in eventlog[0].lower() and eventlog[1].startswith(("hx","cx")) and eventlog[2].startswith(("hx","cx")):
            eventlog_output.append(eventlog)
        else:
            pass
    if not eventlog_output:
        eventlog_output = ''
    return eventlog_output

def get_tx_results_df(txHash):
    tx_results = icon_service.get_transaction_result(txHash)
    txResults_txHash = deep_get(tx_results, 'txHash')
    txResults_status = deep_get(tx_results, 'status')
    txResults_stepUsed = deep_get(tx_results, 'stepUsed')
    txResults_stepPrice = deep_get(tx_results, 'stepPrice')
    # txResults_eventlogs = dict(enumerate(get_eventlogs_tx_data(tx_results)))
    txResults_eventlogs = get_eventlogs_tx_data(tx_results)

    combined_txResults = {'txHash': txResults_txHash,
        'status': txResults_status,
        'stepUsed': txResults_stepUsed,
        'stepPrice': txResults_stepPrice,
        'eventlogs': [txResults_eventlogs],}

    tx_results_df = pd.DataFrame(data=combined_txResults)

    return tx_results_df   

# startRange = 37090345
# endRange = 37090346

# startRange = block_of_interest.iloc[0]
# endRange = block_of_interest.iloc[-1]

# blockRange = range(startRange,endRange+1)

# block_of_interest = block_of_interest[block_of_interest > 37211616]
# block_of_interest = block_of_interest[0:5000]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

pbar = tqdm(block_of_interest)
pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
myblock = []
for i in pbar:
    try:
        this_block = get_block_df(i)
        myblock.append(this_block)
    except:
        random_sleep_except = random.uniform(240,360)
        print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
        time.sleep(random_sleep_except) #sleep the script for x seconds and....#
        continue



# tmp_block_df = pd.concat(myblock)
block_df = pd.concat(myblock)

windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

# saving block data balance
block_df.to_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'), index=False)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
random_sleep_except = random.uniform(600,900)
print("I'll pause for"+str(random_sleep_except/60) + " minutes before doing another... \n")
time.sleep(random_sleep_except) #sleep the script for x seconds and....#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
block_df = pd.read_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'))


txHashes = block_df['txHash']

pbar = tqdm(txHashes)
pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
mytx = []
mytx_results = []
for txHash in pbar:
    try:
        this_tx = get_tx_df(txHash)
        mytx.append(this_tx)

        this_tx_results = get_tx_results_df(txHash)
        mytx_results.append(this_tx_results)

    except:
        random_sleep_except = random.uniform(240,360)
        print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
        time.sleep(random_sleep_except) #sleep the script for x seconds and....#
        continue


tx_df = pd.concat(mytx)
tx_results_df = pd.concat(mytx_results)

tx_df.to_csv(os.path.join(windows_path, 'tx_' + date_prev + '.csv'), index=False)
tx_results_df.to_csv(os.path.join(windows_path, 'tx_results_' + date_prev + '.csv'), index=False)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# pd.to_datetime(1560932465382639 / 1000000, unit='s').strftime('%Y-%m-%d')

# bytearray.fromhex(dat[2:]).decode()
# import binascii 
# string = '{"msgType":"JejuVisitLog","time":1626533340,"period":60,"count":17}'
# binascii.hexlify(string.encode())
