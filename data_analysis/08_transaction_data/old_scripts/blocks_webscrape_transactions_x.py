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
import pylab as pl
import numpy as np
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


# binance_cold3
# prep_address = 'hx9f0c84a113881f0617172df6fc61a8278eb540f5'

# binance_cold1
this_address = 'hx1729b35b690d51e9944b2e94075acff986ea0675'



def insert_str(string="YYYY-WW", index=4, timeinterval=' week') -> str:
    """
    Inserting strings between strings
    """
    return string[:index] + timeinterval + string[index:]


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
    return out_data


def deep_get(dictionary, keys):
    return reduce(deep_get_imps, keys.split("."), dictionary)


def get_eventlogs_tx_data(transaction_result):
    for i in transaction_result['eventLogs']:
        eventlog = deep_get(i, "indexed")
        if "transfer" in eventlog[0].lower():
            if eventlog[1].startswith(("hx","cx")) and eventlog[2].startswith(("hx","cx")):
                return eventlog
            else:
                return None

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

# block_all = []
# i = []
# for i in range(0, page_count):

#     block_url = Request('https://tracker.icon.foundation/v3/block/list?page=' + str(i+1) + '&count=' +
#                             str(block_count), headers={'User-Agent': 'Mozilla/5.0'})

#     # json format
#     jblock = json.load(urlopen(block_url))
#     block_all.append(jblock)


# tracker_txcount = sum(extract_values(block_all, 'txCount'))



def get_block_data(page_count, block_count):
    block_url = Request('https://tracker.icon.foundation/v3/block/list?page=' + str(page_count) + '&count=' +
                            str(block_count), headers={'User-Agent': 'Mozilla/5.0'})
    jblock = json.load(urlopen(block_url))
    return jblock

# collecting contact info using multithreading
start = time()

block_all = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(0, page_count):
        block_all.append(executor.submit(get_block_data, page_count=k+1, block_count=block_count))

temp_df = []
for task in as_completed(block_all):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')


tracker_height = extract_values(temp_df, 'height')
tracker_createDate = extract_values(temp_df, 'createDate')
tracker_txcount = extract_values(temp_df, 'txCount')
tracker_amount = extract_values(temp_df, 'amount')
tracker_fee = extract_values(temp_df, 'fee')
tracker_hash = extract_values(temp_df, 'hash')


combined = {'blockHeight': tracker_height,
    'createDate': tracker_createDate,
    'txCount': tracker_txcount,
    'amount': tracker_amount,
    'fee': tracker_fee,
    'hash': tracker_hash,}

combined_df = pd.DataFrame(data=combined).sort_values(by="blockHeight", ascending=True)

combined_df['date'] = pd.to_datetime(combined_df['createDate']).dt.strftime("%Y-%m-%d")

df_of_interest = combined_df[combined_df['date'] == '2021-07-17']

# df_of_interest = df_of_interest[df_of_interest['fee'] != '0']

block_of_interest = df_of_interest['blockHeight']

hash_of_interest = df_of_interest['hash']

# blockHeight = block_of_interest.iloc[1]


block_of_interest = block_of_interest.iloc[0:50]


def get_detailed_block_data(blockHeight):

    # # getting transaction information from icon transaction page
    # dblock_url = Request('https://tracker.icon.foundation/v3/block/txList?height='+ str(blockHeight) + '&page=1&count=10', headers={'User-Agent': 'Mozilla/5.0'})

    # # first getting total number of contract from website
    # # (this will need to change because icon page will have 500k max)
    # jdblock = json.load(urlopen(dblock_url))

    # block_count = 100
    # listSize = extract_values(jdblock, 'listSize')

    # # get page count to loop over
    # page_count = round((listSize[0] / block_count) + 0.5)

    page_count = 1
    block_count = 100
    # dblock_all = []
    dblock_url = Request('https://tracker.icon.foundation/v3/block/txList?height='+ str(blockHeight) +'&page=' + str(page_count) + '&count=' +
                                str(block_count), headers={'User-Agent': 'Mozilla/5.0'})

    jdblock = json.load(urlopen(dblock_url))
    return jdblock

# collecting contact info using multithreading
# start = time()

# dblock_all = []
# with ThreadPoolExecutor(max_workers=5) as executor:
#     for k in block_of_interest:
#         dblock_all.append(executor.submit(get_detailed_block_data,  blockHeight=k))

# temp_df = []
# for task in as_completed(dblock_all):
#     temp_df.append(task.result())

# print(f'Time taken: {time() - start}')

# dblock_all = []
# count=0
# for k in block_of_interest:
#     count += 1
#     dblock_all.append(get_detailed_block_data(blockHeight=k))
#     print(str(count) + "/" + str(len(block_of_interest)))

# tx_fromAddr = extract_values(dblock_all, 'fromAddr')
# tx_toAddr = extract_values(dblock_all, 'toAddr')
# tx_amount = extract_values(dblock_all, 'amount')
# tx_fee = extract_values(dblock_all, 'fee')
# tx_state = extract_values(dblock_all, 'state')
# tx_dataType = extract_values(dblock_all, 'dataType')
# tx_txType = extract_values(dblock_all, 'txType')
# tx_targetContractAddr = extract_values(dblock_all, 'targetContractAddr')

# tx_combined = {'fromAddr': tx_fromAddr,
#     'toAddr': tx_toAddr,
#     'amount': tx_amount,
#     'fee': tx_fee,
#     'state': tx_state,
#     'dataType': tx_dataType,
#     'txType': tx_txType,
#     'targetContractAddr': tx_targetContractAddr,}

# tx_df = pd.DataFrame(data=tx_combined)




# hash_of_interest[1]
# icon_service.get_block(hash_of_interest[1])

boi = block_of_interest[0:10]


# icon_service.get_block(37125973)


x = icon_service.get_block(37090346)

txHashes = extract_values(x, "txHash")



for txHash in txHashes:
    tx = icon_service.get_transaction(txHash)
    tx_results = icon_service.get_transaction_result(txHash)
    print(tx)
    # print(tx_results)
    # get_eventlogs_tx_data(tx_results)


txHash = "0xde1e500b8a8a4717dc3a0fe34747b1202eb1a811d4de61376bc95406afa89986"

txHash = "0xcc3436523c30404d3355af2db22dbbde7208835bcec2ecfa36c44c29c0896951"
tx = icon_service.get_transaction(txHash)
tx_results = icon_service.get_transaction_result(txHash)
print(tx)
print(tx_results)


deep_get(tx, "data")
deep_get(tx, "dataType")
deep_get(tx, 'from')
deep_get(tx, 'to')
deep_get(tx, 'timestamp')




bytearray.fromhex(dat[2:]).decode()





def get_eventlogs_tx_data(transaction_result):
    for i in transaction_result['eventLogs']:
        eventlog = deep_get(i, "indexed")
        if "transfer" in eventlog[0].lower():
            if eventlog[1].startswith(("hx","cx")) and eventlog[2].startswith(("hx","cx")):
                return eventlog
            else:
                return None














# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# name_this_address = known_address_details_from[known_address_details_from['from_address'] == this_address].pop('from_def').iloc[0]

# # getting transaction information from icon transaction page
# prep_tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address='
#                     + this_address, headers={'User-Agent': 'Mozilla/5.0'})


# # first getting total number of tx from website (this will need to change because icon page will have 500k max)
# jprep_prep_tx_url = json.load(urlopen(prep_tx_url))
# totalSize = extract_values(jprep_prep_tx_url, 'totalSize')

# # get page count to loop over
# tx_count = 100
# page_count = round((totalSize[0] / tx_count) + 0.5)

# prep_tx_all = []
# i = []
# for i in range(0, page_count):

#     # then apply total pages to extract correct amount of data
#     prep_tx_url = Request('https://tracker.icon.foundation/v3/address/txList?page=' + str(i+1) + '&count=' + str(tx_count)
#                         + '&address=' + this_address, headers={'User-Agent': 'Mozilla/5.0'})
#     # json format
#     jprep_list_url = json.load(urlopen(prep_tx_url))
#     prep_tx_all.append(jprep_list_url)

# # extracting p-rep information by labels
# tx_from_address = extract_values(prep_tx_all, 'fromAddr')
# tx_to_address = extract_values(prep_tx_all, 'toAddr')
# tx_date = extract_values(prep_tx_all, 'createDate')
# tx_amount = extract_values(prep_tx_all, 'amount')
# tx_fee = extract_values(prep_tx_all, 'fee')
# tx_state = extract_values(prep_tx_all, 'state')

# # combining lists
# combined = {'from_address': tx_from_address,
#     'dest_address': tx_to_address,
#     'datetime': tx_date,
#     'amount': tx_amount,
#     'fee': tx_fee,
#     'state': tx_state}

# # convert into dataframe
# combined_df = pd.DataFrame(data=combined)

# # removing failed transactions & drop 'state (state for transaction success)'
# combined_df = combined_df[combined_df.state != 0].drop(columns='state')

# # shorten date info
# combined_df['date'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m-%d")
# combined_df['week'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%U")
# combined_df['month'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m")
# combined_df['year'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y")


# prep_wallet_tx = pd.merge(combined_df, known_address_details_from, how='left', on='from_address')
# prep_wallet_tx = pd.merge(prep_wallet_tx, known_address_details_to, how='left', on='dest_address')
# prep_wallet_tx['amount'] = prep_wallet_tx['amount'].astype(float)

# # all historical data
# concat_df_all = prep_wallet_tx.copy()

# # selected time frame
# concat_df_date = prep_wallet_tx[prep_wallet_tx['date'] == date_today]
# concat_df_week = prep_wallet_tx[prep_wallet_tx['week'] == week_today]
# concat_df_month = prep_wallet_tx[prep_wallet_tx['month'] == month_today]
# concat_df_year = prep_wallet_tx[prep_wallet_tx['year'] == year_today]






















# def plot_and_save(df, edge_width_ratio=5, nodesize_ratio=10000, title="historical", figsize=(12,8), fname="historical", leftright=150, updown=0, vmax_val=0):
    
#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Concatenate data together  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     #~~~~~~~~~~ Assign unknown wallet a name (e.g. wallet_1, wallet_2) by appeared date ~~~~~~~~~#
#     # get unique address for both from and to transactions
#     address_1 = df.rename(columns={"from_address": "address"}).sort_values(by=['address']).\
#         groupby(['address']).first().reset_index()[['address']]
#     address_2 = df.rename(columns={"dest_address": "address"}).sort_values(by=['address']).\
#         groupby(['address']).first().reset_index()[['address']]

#     # concatenate & remove duplicates, attach wallet info from iconwatch
#     concat_address = pd.concat([address_1, address_2]).sort_values(by=['address']).\
#         groupby('address').first().reset_index()

#     wallet_info = []
#     wallet_info = pd.merge(concat_address, known_address_details_from, left_on='address', right_on='from_address', how='left').\
#         drop(columns='from_address').rename(columns={"from_def": "wallet_def"})

#     # re-ordering to get the wallet of interest to the top
#     wallet_info['this_order'] = np.where(wallet_info['address'] == this_address, 1, 2)
#     wallet_info = wallet_info.sort_values(by=['this_order']).drop(columns='this_order')


#     # if nan, then assign wallet_1, wallet_2 etc
#     wallet_info['wallet_count'] = 'wallet_' + wallet_info.groupby(['wallet_def']).cumcount().add(1).astype(str).apply(lambda x: x.zfill(3))
#     wallet_info['wallet_def'].fillna(wallet_info.wallet_count, inplace=True)
#     wallet_info = wallet_info.drop(columns=['wallet_count'])


#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     ## Getting balance from these wallets

#     # to series
#     wallet_address = wallet_info.address

#     # how many wallets
#     len_wallet_address = len(wallet_address)

#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     # using solidwallet
#     icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

#     ## Creating Wallet (only done for the first time)
#     # wallet = KeyWallet.create()
#     # wallet.get_address()
#     # wallet.get_private_key()
#     tester_wallet = os.path.join(walletPath, "test_keystore_1")
#     # wallet.store(tester_wallet, "abcd1234*")

#     wallet = KeyWallet.load(tester_wallet, "abcd1234*")
#     tester_address = wallet.get_address()

#     # loop to icx converter
#     def loop_to_icx(loop):
#         icx = loop / 1000000000000000000
#         return(icx)

#     def parse_icx(val: str) -> Union[float, int]:
#         """
#         Attempts to convert a string loop value into icx
#             Parameters:
#                 val (str): Loop value
#             Returns:
#                 (Union[float, int]): Will return the converted value as an int if successful, otherwise it will return NAN
#         """
#         try:
#             return loop_to_icx(int(val, 0))
#         except ZeroDivisionError:
#             return float("NAN")
#         except ValueError:
#             return float("NAN")

#     def hex_to_int(val: str) -> Union[int, float]:
#         """
#         Attempts to convert a string based hex into int
#             Parameters:
#                 val (str): Value in hex
#             Returns:
#                 (Union[int, float]): Returns the value as an int if successful, otherwise a float NAN if not
#         """
#         try:
#             return int(val, 0)
#         except ValueError:
#             print(f"failed to convert {val} to int")
#             return float("NAN")

#     # get data function
#     def get_my_values(method, address, output, len_wallet_address):
#         call = CallBuilder().from_(tester_address)\
#                         .to('cx0000000000000000000000000000000000000000')\
#                         .params({"address": address})\
#                         .method(method)\
#                         .build()
#         result = icon_service.call(call)

        
#         temp_output = parse_icx(result[output])
        
#         df = {'address': address, output: temp_output}
#         return(df)


#     # get unstakes function
#     def get_my_unstakes(method, address, output, len_wallet_address):
#         call = CallBuilder().from_(tester_address)\
#                         .to('cx0000000000000000000000000000000000000000')\
#                         .params({"address": address})\
#                         .method(method)\
#                         .build()
#         result = icon_service.call(call)
#         try:
#             df = result[output][0]
#             df['address'] = address
#         except:
#             df = {'unstake': np.nan, 'unstakeBlockHeight': np.nan, 'remainingBlocks': np.nan, 'address': address}
    
#         try:
#             df['unstake'] = parse_icx(df['unstake'])
#             df['unstakeBlockHeight'] = hex_to_int(df['unstakeBlockHeight'])
#             df['remainingBlocks'] = hex_to_int(df['remainingBlocks'])
#         except Exception:
#             pass

#         return(df)


#     # get balance function
#     def get_balance(address, len_wallet_address):
#         try:
#             balance = loop_to_icx(icon_service.get_balance(address))
#         except:
#             balance = float("NAN")
#         df = {'address': address, 'balance': balance}
#         return(df)


#     from itertools import cycle, islice

#     max_workers_value = 50
#     end_range = 50
#     wallet_address_clean = wallet_address[:end_range]
#     len_wallet_address = len(wallet_address_clean)


#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     all_iscore = []
#     all_staked = []
#     all_delegating = []
#     all_balance = []
#     all_unstakes = []

#     start = time()
#     with ThreadPoolExecutor(max_workers=max_workers_value) as executor:
#         for i in tqdm(range(0, len_wallet_address), desc="workers"):
#             all_iscore.append(executor.submit(get_my_values, "queryIScore", wallet_address_clean[i], "estimatedICX", len_wallet_address))
#             all_staked.append(executor.submit(get_my_values, "getStake", wallet_address_clean[i], "stake", len_wallet_address))
#             all_delegating.append(executor.submit(get_my_values, "getDelegation", wallet_address_clean[i], "totalDelegated", len_wallet_address))
#             all_balance.append(executor.submit(get_balance, wallet_address_clean[i], len_wallet_address))
#             all_unstakes.append(executor.submit(get_my_unstakes, "getStake", wallet_address_clean[i], "unstakes", len_wallet_address))         

#     print("workers complete")

#     temp_iscore = []
#     for task in tqdm(as_completed(all_iscore), desc="iscore\t\t", total=len_wallet_address):
#         temp_iscore.append(task.result())

#     temp_staked = []
#     for task in tqdm(as_completed(all_staked), desc="staked\t\t", total=len_wallet_address):
#         temp_staked.append(task.result())

#     temp_delegating = []
#     for task in tqdm(as_completed(all_delegating), desc="delegating\t", total=len_wallet_address):
#         temp_delegating.append(task.result())

#     temp_balance = []
#     for task in tqdm(as_completed(all_balance), desc="balance\t", total=len_wallet_address):
#         temp_balance.append(task.result())

#     temp_unstakes = []
#     for task in tqdm(as_completed(all_unstakes), desc="unstakes\t", total=len_wallet_address):
#         temp_unstakes.append(task.result()) 


#     print("building dataframes...")

#     data_frames = []
#     if len(temp_iscore) > 0: 
#         data_frames.append(pd.DataFrame(temp_iscore)) 
#     if len(temp_staked) > 0: 
#         data_frames.append(pd.DataFrame(temp_staked))
#     if len(temp_delegating) > 0: 
#         data_frames.append(pd.DataFrame(temp_delegating))
#     if len(temp_balance) > 0: 
#         data_frames.append(pd.DataFrame(temp_balance))
#     if len(temp_unstakes) > 0: 
#         data_frames.append(pd.DataFrame(temp_unstakes))

#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#     all_df = reduce(lambda left, right: pd.merge(left, right, on=['address'], how='outer'), data_frames)

#     print(f'Time taken: {time() - start}')


#     def preprocess_df(inData):
#         df = inData.drop_duplicates()
#         df = df.groupby('address').sum().reset_index()

#         # str to float and get total icx owned
#         col_list = ['balance', 'stake', 'unstake']
#         df['totalDelegated'] = df['totalDelegated'].astype(float)
#         df[col_list] = df[col_list].astype(float)
#         df['total'] = df[col_list].sum(axis=1)
#         df['staking_but_not_delegating'] = df['stake'] - df['totalDelegated']

#         # only use wallets with 'hx' prefix (problem if they stake)
#         # df = df[df['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

#         return df

#     all_df = preprocess_df(all_df)
#     all_df = pd.merge(all_df, wallet_info, how='left', on='address')

#     # getting total balance
#     all_df = all_df[['address','wallet_def','total']]




#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#     nodesize = all_df.rename(columns = {'wallet_def':'def'}).drop(columns='address')

#     # Add it back to data to be analysed
#     df = pd.merge(df, wallet_info, left_on='from_address', right_on='address', how='left').\
#         drop(columns=['address', 'from_def']).rename(columns={'wallet_def': 'from_def'})

#     df = pd.merge(df, wallet_info, left_on='dest_address', right_on='address', how='left').\
#         drop(columns=['address', 'dest_def']).rename(columns={'wallet_def': 'dest_def'})

#     df[['amount', 'fee']] = df[['amount', 'fee']].apply(pd.to_numeric, errors='coerce', axis=1)

#     # edges (for gephi?)
#     # edges = df.groupby(['from_def', 'dest_def']).from_address.agg('count').reset_index().rename(columns={'from_address': 'weight'})


#     # to change width of edges based on number of tx
#     edge_width_ratio = edge_width_ratio

#     # summarising df
#     temp_df = df[['from_def','dest_def','amount']].groupby(['from_def', 'dest_def']).amount.agg(['sum','count']).reset_index()
#     temp_df = temp_df.rename(columns={'sum':'amount', 'count':'weight'})
#     temp_df['weight_ratio'] = temp_df['weight']/edge_width_ratio # to change width of edges based on number of tx
#     temp_df['color'] = np.where(temp_df['from_def'] == name_this_address, 'r','g')


#     # for node label
#     wallet_001_amount = nodesize.reset_index(drop=True)
#     wallet_001_amount = wallet_001_amount[wallet_001_amount['def'] == name_this_address]
#     wallet_001_amount['text'] = np.where(wallet_001_amount['total']>0, wallet_001_amount['total'], 0)
#     wallet_001_amount['text'] = name_this_address + ': \n' + wallet_001_amount['text'].round(0).astype(int).apply('{:,}'.format).astype(str) + '\n ICX left'
#     wallet_001_amount = wallet_001_amount.drop(columns=['total'])

#     # node label final
#     node_label = []
#     node_label = nodesize.drop(columns=['total'])
#     node_label = pd.merge(node_label, wallet_001_amount, how='left', on='def')
#     node_label['text'] = np.where(node_label['text'].isna(), node_label['def'], node_label['text'])


#     node_labeldict = node_label.set_index('def')['text'].to_dict()

#     edge_labeldict = temp_df.drop(columns='weight')
#     edge_labeldict['amount'] = edge_labeldict['amount'].round(0).astype(int).apply('{:,}'.format).astype(str) + '\n ICX'

#     # going out balance
#     edge_labeldict_r = edge_labeldict[edge_labeldict['color']=='r']
#     edge_labeldict_r = edge_labeldict_r.set_index(['from_def','dest_def']).pop("amount").to_dict()

#     # coming in balance
#     edge_labeldict_g = edge_labeldict[edge_labeldict['color']=='g']
#     edge_labeldict_g = edge_labeldict_g.set_index(['from_def','dest_def']).pop("amount").to_dict()

#     edge_labeldict = edge_labeldict.set_index(['from_def','dest_def']).pop("amount").to_dict()



#     ## figure
#     fig1 = plt.figure(figsize=figsize)
#     ax = plt.gca()
#     ax.set_title(title)
#     G = nx.from_pandas_edgelist(temp_df, 'from_def', 'dest_def', create_using=nx.MultiDiGraph())
#     pos = nx.nx_pydot.graphviz_layout(G)

#     nodesize_ratio = nodesize_ratio
#     nodesize = nodesize.set_index('def').squeeze().loc[list(G.nodes)] # to align the data with G.nodes


#     color_map_1 = []
#     for node in G:
#         if node == name_this_address:
#             color_map_1.append('gold')
#         else:
#             color_map_1.append('black')

#     colors = temp_df['weight'].values
#     cmap = plt.cm.jet
#     # cmap = plt.cm.Blues
#     vmin = 0

#     if vmax_val == 0:
#         vmax = max(temp_df['weight'].values)
#     else: 
#         vmax = vmax_val

#     g = nx.draw(G, node_color=color_map_1, pos=pos,
#             with_labels=True, labels=node_labeldict, connectionstyle='arc3, rad = 0.15',
#             node_size=nodesize/nodesize_ratio, alpha=1, arrows=True,
#             font_size=10, font_color='tab:orange', 
#             edge_color = colors,
#             edge_cmap = cmap,
#             vmin = vmin, vmax = vmax,
#             width = temp_df['weight_ratio'])  # fontweight='bold',
#             #edge_color=temp_df['color'],
#             #width=temp_df['weight'])


#     # for key, value in test_pos.items():
#     #     value = (value[0], value[1] * 0.8)
#     #     test_pos[key] = value

#     nx.draw_networkx_edge_labels(G, pos, font_size=8,
#                                 edge_labels=edge_labeldict_r,
#                                 label_pos=0.6,
#                                 font_color='r')

#     nx.draw_networkx_edge_labels(G, pos, font_size=8,
#                                 edge_labels=edge_labeldict_g,
#                                 label_pos=0.6,
#                                 font_color='g')

#     plt.axis('off')
#     plt.tight_layout()
#     ax.text(leftright, updown, ' (1) Circle size represents current balance \n (2) Arrow thickness/colour represents number of transactions \n (3) Green/Red text = into/out of ' + name_this_address + ' wallet')
    
#     # plt.show()

#     # Set Up Colorbar
#     sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin = vmin, vmax=vmax))
#     sm._A = []
#     clb = plt.colorbar(sm, shrink=0.5)
#     clb.ax.set_title('Number of TX')

#     windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/06_wallet_ranking/results/" + date_today_sav

#     if not os.path.exists(windows_path):
#         os.mkdir(windows_path)

#     plt.savefig(os.path.join(windows_path, name_this_address + "_" + fname + '_' + date_today_sav + ".png"))




# try:
#     my_title = "Since " + name_this_address + " creation" + " (today: " + date_today_text + ")"
#     plot_and_save(df=concat_df_all, title=my_title, fname='historical', leftright=150, updown=0, vmax_val=0)
# except:
#     print('Data may not exist')

# # Year
# try:
#     my_title = "Since " + year_today_text + " (today: " + date_today_text + ")"
#     plot_and_save(df=concat_df_year, edge_width_ratio=2, title=my_title, fname='year', leftright=100, updown=5, vmax_val=0)
# except:
#     print('Data may not exist')

# # Month
# try:
#     my_title = "Since " + month_today_text + " (today: " + date_today_text + ")"
#     plot_and_save(df=concat_df_month, edge_width_ratio=2, title=my_title, fname='month', leftright=100, updown=5, vmax_val=0)
# except:
#     print('Data may not exist')

# # Week
# try:
#     my_title = "Since " + week_today_text + " (today: " + date_today_text + ")"
#     plot_and_save(df=concat_df_week, edge_width_ratio=2, title=my_title, fname='week', leftright=100, updown=5, vmax_val=0)
# except:
#     print('Data may not exist')

# # Date
# try:
#     my_title = "Today: " + date_today_text
#     plot_and_save(df=concat_df_date, edge_width_ratio=1, title=my_title, fname='date', leftright=100, updown=6, vmax_val=5)
# except:
#     print('Data may not exist')




