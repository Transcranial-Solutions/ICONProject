
#########################################################################
## Project: Transaction per day                                        ##
## Date: August 2021                                                   ##
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
from ast import literal_eval

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
use_specific_prev_date = 1
date_prev = "2021-07-28"

if use_specific_prev_date == 1:
    date_prev = date_prev
else:
    date_prev = yesterday(today)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# data loading
windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

block_df = pd.read_csv(os.path.join(windows_path, 'transactions_each_block_' + date_prev + '.csv'))
tx_df = pd.read_csv(os.path.join(windows_path, 'tx_' + date_prev + '.csv'))
tx_results_df = pd.read_csv(os.path.join(windows_path, 'tx_results_' + date_prev + '.csv'))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# processing

# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)


# convert timestamp to datetime
def timestamp_to_date(df, timestamp, dateformat):
    return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)

tx_all = pd.merge(tx_df, tx_results_df, on="txHash", how="left")
# tx_all = pd.merge(block_df, tx_all, on=["txHash","tx_timestamp","blockHeight"], how="left")


# tx_all['eventlogs'] = tx_all['eventlogs'].apply(literal_eval)

# check = tx_all.iloc[0:100000][['txHash','eventlogs']]
check = tx_all[['txHash','eventlogs']]

# check = check[check['eventlogs'].notnull()]
check = check[check['eventlogs'] != 'NaN']
check['eventlogs'].apply(literal_eval)
# check = check.iloc[0:5]



tx_all['stepPrice'] = pd.to_numeric(tx_all['stepPrice'], errors='coerce').astype('Int64').map(loop_to_icx).fillna(0)
tx_all['stepUsed'] = pd.to_numeric(tx_all['stepUsed'], errors='coerce').astype('Int64').fillna(0)
tx_all['tx_fees'] = tx_all['stepUsed'] * tx_all['stepPrice']

# tx_all['block_date'] = timestamp_to_date(tx_all, 'block_timestamp', '%Y-%m-%d')
tx_all['block_time'] = timestamp_to_date(tx_all, 'block_timestamp', '%H:%M:%S')
tx_all['tx_date'] = timestamp_to_date(tx_all, 'tx_timestamp', '%Y-%m-%d')
tx_all['tx_time'] = timestamp_to_date(tx_all, 'tx_timestamp', '%H:%M:%S')


check = tx_all.iloc[0:500][['txHash','tx_timestamp','eventlogs']]
check = check[check['eventlogs'].notnull()]



check.explode("eventlogs")
check['eventlogs'].explode()




tx_all.eventlogs = tx_all.eventlogs.fillna({i: [] for i in tx_all.index})  # replace NaN with []


test = tx_all[['eventlogs']]
test = test.loc[110584]

# test['eventlogs'] = test['eventlogs'].apply(literal_eval)
# test['eventlogs'].explode()


tx_all = tx_all.explode("eventlogs").reset_index(drop=True)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# pd.to_datetime(1560932465382639 / 1000000, unit='s').strftime('%Y-%m-%d')

# bytearray.fromhex(dat[2:]).decode()
# import binascii 
# string = '{"msgType":"JejuVisitLog","time":1626533340,"period":60,"count":17}'
# binascii.hexlify(string.encode())
