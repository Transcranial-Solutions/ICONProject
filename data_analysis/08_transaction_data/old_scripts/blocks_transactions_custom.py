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

# Webscraping
# This file extracts information from Blockmove's Iconwatch website (https://iconwat.ch/address/).
# Extract

# import json library
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.groupby.generic import DataFrameGroupBy
import pylab as pl
import os
from functools import reduce
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from iconsdk.builder.call_builder import CallBuilder
from typing import Union
from time import time
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# currPath = os.getcwd()
currPath = "/home/tonoplast/IconProject/"
projectPath = os.path.join(currPath, "wallet_ranking")
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
        return yesterday.strftime('%Y_%m_%d')
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
# today = date.today()
today = datetime.utcnow()
date_today_sav = today.strftime("%Y_%m_%d")

date_today = today.strftime("%Y-%m-%d")
date_today_text = date_today.replace("_","/")

week_today = today.strftime("%Y-%U")
week_today_text = insert_str(week_today, 4, ' week')

month_today = today.strftime("%Y-%m")
month_today_text = today.strftime("%Y %B")

year_today = today.strftime("%Y")
year_today_text = "Year " + year_today


# to use specific date, otherwise use yesterday
use_specific_prev_date = 0
day_prev = "2021_02_08"

if use_specific_prev_date == 1:
    day_prev = day_prev
else:
    day_prev = yesterday(today)
# day_prev_text = day_prev.replace("_","/")
day_prev_text = day_prev.replace("_","-")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ solidwallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# using solidwallet
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

## Creating Wallet (only done for the first time)
# wallet = KeyWallet.create()
# wallet.get_address()
# wallet.get_private_key()
tester_wallet = os.path.join(walletPath, "test_keystore_1")
# wallet.store(tester_wallet, "abcd1234*")

wallet = KeyWallet.load(tester_wallet, "abcd1234*")
tester_address = wallet.get_address()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX Address Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
known_address_url = Request('https://iconwat.ch/data/thes', headers={'User-Agent': 'Mozilla/5.0'})
jknown_address = json.load(urlopen(known_address_url))

# add any known addresses here manually
def add_dict_if_noexist(key, d, value):
    if key not in d:
        d[key] = value

# add any known addresses here manually (if not exist)
add_dict_if_noexist('hx02dd8846baddc171302fb88b896f79899c926a5a', jknown_address, 'ICON_Vote_Monitor')
add_dict_if_noexist('hxa527f96d8b988a31083167f368105fc0f2ab1143', jknown_address, 'binance_us')

add_dict_if_noexist('hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310', jknown_address, 'upbit_hot1')
add_dict_if_noexist('hxfdb57e23c32f9273639d6dda45068d85ee43fe08', jknown_address, 'upbit_hot2')
add_dict_if_noexist('hx4a01996877ac535a63e0107c926fb60f1d33c532', jknown_address, 'upbit_hot3')
add_dict_if_noexist('hx8d28bc4d785d331eb4e577135701eb388e9a469d', jknown_address, 'upbit_hot4')
add_dict_if_noexist('hxf2b4e7eab4f14f49e5dce378c2a0389c379ac628', jknown_address, 'upbit_hot5')

add_dict_if_noexist('hx6eb81220f547087b82e5a3de175a5dc0d854a3cd', jknown_address, 'bithumb_1')
add_dict_if_noexist('hx0cdf40498ef03e6a48329836c604aa4cea48c454', jknown_address, 'bithumb_2')
add_dict_if_noexist('hx6d14b2b77a9e73c5d5804d43c7e3c3416648ae3d', jknown_address, 'bithumb_3')

add_dict_if_noexist('hxa390d24fdcba68515b492ffc553192802706a121', jknown_address, 'bitvavo_hot')
add_dict_if_noexist('hxa390d24fdcba68515b492ffc553192802706a121', jknown_address, 'bitvavo_cold')

add_dict_if_noexist('hx85532472e789802a943bd34a8aeb86668bc23265', jknown_address, 'unkEx_c1')
add_dict_if_noexist('hx94a7cd360a40cbf39e92ac91195c2ee3c81940a6', jknown_address, 'unkEx_c2')

add_dict_if_noexist('hxe5327aade005b19cb18bc993513c5cfcacd159e9', jknown_address, 'unkEx_d1')

# add_dict_if_noexist('hxddec6fb21f9618b537e930eaefd7eda5682d9dc8', jknown_address, 'unkEx_e1')
# add_dict_if_noexist('hx294c5d0699615fc8d92abfe464a2601612d11bf7', jknown_address, 'unkEx_e2')


# binance sweepers
add_dict_if_noexist('hx8a50805989ceddee4341016722290f13e471281e', jknown_address, 'binance_sweeper_01')
add_dict_if_noexist('hx58b2592941f61f97c7a8bed9f84c543f12099239', jknown_address, 'binance_sweeper_02')
add_dict_if_noexist('hx49c5c7eead084999342dd6b0656bc98fa103b185', jknown_address, 'binance_sweeper_03')
add_dict_if_noexist('hx56ef2fa4ebd736c5565967197194da14d3af88ca', jknown_address, 'binance_sweeper_04')
add_dict_if_noexist('hxe295a8dc5d6c29109bc402e59394d94cf600562e', jknown_address, 'binance_sweeper_05')
add_dict_if_noexist('hxa479f2cb6c201f7a63031076601bbb75ddf78670', jknown_address, 'binance_sweeper_06')
add_dict_if_noexist('hx538de7e0fc0d312aa82549aa9e4daecc7fabcce9', jknown_address, 'binance_sweeper_07')
add_dict_if_noexist('hxc20e598dbc78c2bfe149d3deddabe77a72412c92', jknown_address, 'binance_sweeper_08')
add_dict_if_noexist('hx5fd21034a8b54679d636f3bbff328df888f0fe28', jknown_address, 'binance_sweeper_09')
add_dict_if_noexist('hxa94e9aba8830f7ee2bace391afd464417284c430', jknown_address, 'binance_sweeper_10')
add_dict_if_noexist('hxa3453ab17ec5444754cdc5d928be8a49ecf65b22', jknown_address, 'binance_sweeper_11')
add_dict_if_noexist('hx8c0a2f8ca8df29f9ce172a63bf5fd8106c610f42', jknown_address, 'binance_sweeper_12')
add_dict_if_noexist('hx663ff2514ece0de8c3ecd76f003a9682fdc1fb00', jknown_address, 'binance_sweeper_13')
add_dict_if_noexist('hx0232d71f68846848b00b4008771be7b0527fbb39', jknown_address, 'binance_sweeper_14')
add_dict_if_noexist('hx561485c5ee93cf521332b520bb5d10d9389c8bab', jknown_address, 'binance_sweeper_15')
add_dict_if_noexist('hx0b75cf8b1cdb81d514e64cacd82ed14674513e6b', jknown_address, 'binance_sweeper_16')
add_dict_if_noexist('hx02ebb44247de11ab80ace2e3c25ebfbcffe4fa68', jknown_address, 'binance_sweeper_17')
add_dict_if_noexist('hxc000d92a7d9d316c6acf11a23a4a20030d414ef2', jknown_address, 'binance_sweeper_18')
add_dict_if_noexist('hx7135ddaeaf43d87ea73cbdd22ba202b13a2caf6a', jknown_address, 'binance_sweeper_19')
add_dict_if_noexist('hxb2d0da403832f9f94617f5037808fe655434e5b7', jknown_address, 'binance_sweeper_20')
add_dict_if_noexist('hx387f3016ee2e5fb95f2feb5ba36b0578d5a4b8cf', jknown_address, 'binance_sweeper_21')
add_dict_if_noexist('hx69221e58dfa8e3688fa8e2ad368d78bfa0fad104', jknown_address, 'binance_sweeper_22')


# add_dict_if_noexist('hx294c5d0699615fc8d92abfe464a2601612d11bf7', jknown_address, 'funnel_a1')
# add_dict_if_noexist('hxc8377a960d4eb484a3b8a733012995583dda0813', jknown_address, 'easy_crypto')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Contract Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting contract information from icon transaction page
known_contract_url = Request('https://tracker.icon.foundation/v3/contract/list?page=1&count=1', headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of contract from website
# (this will need to change because icon page will have 500k max)
jknown_contract = json.load(urlopen(known_contract_url))

contract_count = 100
listSize = extract_values(jknown_contract, 'listSize')

# get page count to loop over
page_count = round((listSize[0] / contract_count) + 0.5)

known_contract_all = []
i = []
for i in range(0, page_count):

    known_contract_url = Request('https://tracker.icon.foundation/v3/contract/list?page=' + str(i+1) + '&count=' +
                                str(contract_count), headers={'User-Agent': 'Mozilla/5.0'})

    jknown_contract = json.load(urlopen(known_contract_url))

    # json format
    jknown_contract = json.load(urlopen(known_contract_url))
    known_contract_all.append(jknown_contract)

# extracting information by labels
contract_address = extract_values(known_contract_all, 'address')
contract_name = extract_values(known_contract_all, 'contractName')

# converting list into dictionary
def Convert(lst1, lst2):
    res_dct = {lst1[i]: lst2[i] for i in range(0, len(lst1), 1)}
    return res_dct

contract_d = Convert(contract_address, contract_name)

# updating known address with other contract addresses
jknown_address.update(contract_d)

# updating contact address
jknown_address['cxb0b6f777fba13d62961ad8ce11be7ef6c4b2bcc6'] = 'ICONbet DAOdice (new)'
jknown_address['cx38fd2687b202caf4bd1bda55223578f39dbb6561'] = 'ICONbet DAOlette (new)'
jknown_address['cxc6bb033f9d0b2d921887040b0674e7ceec1b769c'] = 'Lossless Lottery (Stakin)'

# making same table but with different column names
known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['dest_address', 'dest_def'])
known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from_address', 'from_def'])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting transaction information from icon transaction page
block_url = Request('https://tracker.icon.foundation/v3/block/list?page=1&count=1', headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of contract from website
# (this will need to change because icon page will have 500k max)
jblock = json.load(urlopen(block_url))

block_count = 100
listSize = extract_values(jblock, 'listSize')

# get page count to loop over
page_count = round((listSize[0] / block_count) + 0.5)


def get_block_data(page_count, block_count):
    block_url = Request('https://tracker.icon.foundation/v3/block/list?page=' + str(page_count) + '&count=' +
                            str(block_count), headers={'User-Agent': 'Mozilla/5.0'})
    jblock = json.load(urlopen(block_url))
    return jblock

def output_block_data(temp_df):
    tracker_height = extract_values(temp_df, 'height')
    tracker_createDate = extract_values(temp_df, 'createDate')
    tracker_txcount = extract_values(temp_df, 'txCount')
    tracker_amount = extract_values(temp_df, 'amount')
    tracker_fee = extract_values(temp_df, 'fee')
    tracker_hash = extract_values(temp_df, 'block_hash')


    combined = {'blockHeight': tracker_height,
        'createDate': tracker_createDate,
        'txCount': tracker_txcount,
        'amount': tracker_amount,
        'fee': tracker_fee,
        'hash': tracker_hash,}

    combined_df = pd.DataFrame(data=combined).sort_values(by="blockHeight", ascending=True)
    return combined_df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting block info using multithreading
start = time()

block_all = []
with ThreadPoolExecutor(max_workers=1) as executor:
    for k in range(0, page_count):
        block_all.append(executor.submit(get_block_data, page_count=k+1, block_count=block_count))

temp_df = []
for task in as_completed(block_all):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


combined_df = output_block_data(temp_df)
combined_df['date'] = pd.to_datetime(combined_df['createDate']).dt.strftime("%Y-%m-%d")

df_of_interest = combined_df[combined_df['date'] == day_prev_text]
df_of_interest = df_of_interest[df_of_interest['fee'] != '0']

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

pbar = tqdm(block_of_interest)
pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
myblock = []
for i in pbar:
    this_block = get_block_df(i)
    myblock.append(this_block)

# tmp_block_df = pd.concat(myblock)
block_df = pd.concat(myblock)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting contact info using multithreading
# start = time()

# myblock = []
# with ThreadPoolExecutor(max_workers=5) as executor:
#     for k in blockRange:
#         myblock.append(executor.submit(get_block_df, blockInfo=k))

# temp_df = []
# for task in as_completed(myblock):
#     temp_df.append(task.result())

# block_df = pd.concat(temp_df)
# print(f'Time taken: {time() - start}')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

txHashes = block_df['txHash']

pbar = tqdm(txHashes)
pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
mytx = []
mytx_results = []
for txHash in pbar:
    this_tx = get_tx_df(txHash)
    mytx.append(this_tx)

    this_tx_results = get_tx_results_df(txHash)
    mytx_results.append(this_tx_results)


tx_df = pd.concat(mytx)
tx_results_df = pd.concat(mytx_results)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting contact info using multithreading

# txHashes = block_df['txHash']
# start = time()

# mytx = []
# mytx_results = []
# with ThreadPoolExecutor(max_workers=5) as executor:
#     for k in txHashes:
#         mytx.append(executor.submit(get_tx_df, txHash=k))
#         mytx_results.append(executor.submit(get_tx_results_df, txHash=k))
        
# temp_df = []
# for task in as_completed(mytx):
#     temp_df.append(task.result())

# tx_df = pd.concat(temp_df)

# temp_df = []
# for task in as_completed(mytx_results):
#     temp_df.append(task.result())

# mytx_results = pd.concat(temp_df)


# print(f'Time taken: {time() - start}')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#




# pd.to_datetime(1560932465382639 / 1000000, unit='s').strftime('%Y-%m-%d')

# bytearray.fromhex(dat[2:]).decode()


