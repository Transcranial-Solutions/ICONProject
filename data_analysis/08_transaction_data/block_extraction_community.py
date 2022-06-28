#########################################################################
## Project: Transaction per day                                        ##
## Date: June 2022                                                     ##
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
from urllib.request import Request, urlopen
import json
import pandas as pd
import os
from functools import reduce
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from time import time
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random
import requests


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# currPath = os.getcwd()
currPath = "/home/tonoplast/IconProject/"
# currPath = "E:\\GitHub\\ICONProject\\data_analysis"
projectPath = os.path.join(currPath, "08_transaction_data")
if not os.path.exists(projectPath):
    os.mkdir(projectPath)
    
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

def hex_to_icx(x):
    return int(x, base=16)/1000000000000000000

# convert timestamp to datetime
def timestamp_to_date(df, timestamp, dateformat):
    return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)

# today's date
# today = date.today()
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
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

## Creating Wallet (only done for the first time)
# wallet = KeyWallet.create()
# wallet.get_address()
# wallet.get_private_key()
tester_wallet = os.path.join(walletPath, "test_keystore_1")
# wallet.store(tester_wallet, "abcd1234*")

wallet = KeyWallet.load(tester_wallet, "abcd1234*")
tester_address = wallet.get_address()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Block Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# two different methods in case one fails (I think last 2 are the same, but different link)
def block_df_using_community_tracker(total_pages=500):
    # functions to get transactions and the page count needed for webscraping
    def get_block_via_icon_community_tracker(skip_pages):
        """ This function is used to extract json information from icon community site """
        req = requests.get('https://tracker.icon.community/api/v1/blocks?limit=' + str(100) + '&skip=' + str(skip_pages),
                             headers={'User-Agent': 'Mozilla/5.0'})
        jtracker = json.loads(req.text)
        jtracker_df = pd.DataFrame(jtracker)
        return jtracker_df

    skip = 100
    total_pages = total_pages
    last_page = total_pages * skip - skip
    page_count = range(0, last_page, skip)

    block_all = []
    for k in tqdm(page_count):
        try:
            temp_df = get_block_via_icon_community_tracker(k)
            block_all.append(temp_df)
        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    block_df = pd.concat(block_all)
    block_df['datetime'] = timestamp_to_date(block_df, 'timestamp', '%Y-%m-%d')

    rename_these = {'number': 'blockHeight',
            'datetime': 'block_createDate',
            'transaction_count': 'block_txCount',
            'transaction_amount': 'block_amount',
            'transaction_fees': 'block_fee',
            'hash': 'block_hash'}

    block_df.columns = [rename_these.get(i, i) for i in block_df.columns]
    block_df = block_df.drop_duplicates()
    return block_df

def block_df_using_newer_icon_tracker(page_count=500):
    # getting transaction information from icon transaction page
    page_count = page_count
    def get_block_data(page_count):
        block_url = requests.get('https://main.tracker.solidwallet.io/v3/block/list?page=' + str(page_count) + '&count=' +
                                str(100), headers={'User-Agent': 'Mozilla/5.0'})
        jblock = json.loads(block_url.text)['data']
        jblock = pd.DataFrame(jblock)
        jblock['datetime'] = jblock['createDate'].str.split('T').str[0]

        return jblock

    pbar = tqdm(range(1, page_count))
    # pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
    block_all = []
    for k in pbar:
        try:
            this_block = get_block_data(k)
            block_all.append(this_block)
        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    block_df = pd.concat(block_all).sort_values(by="height", ascending=True)

    rename_these = {'height': 'blockHeight',
            'datetime': 'block_createDate',
            'txCount': 'block_txCount',
            'amount': 'block_amount',
            'fee': 'block_fee',
            'hash': 'block_hash'}

    block_df.columns = [rename_these.get(i, i) for i in block_df.columns]
    block_df = block_df.drop_duplicates()
    return block_df

def block_df_using_original_icon_tracker(page_count=500):
    # getting transaction information from icon transaction page
    page_count = page_count
    def get_block_data(page_count):
        block_url = requests.get('https://tracker.icon.foundation/v3/block/list?page=' + str(page_count) + '&count=' +
                                str(100), headers={'User-Agent': 'Mozilla/5.0'})
        jblock = json.loads(block_url.text)['data']
        jblock = pd.DataFrame(jblock)

        return jblock

    pbar = tqdm(range(1, page_count))
    # pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
    block_all = []
    for k in pbar:
        try:
            this_block = get_block_data(k)
            block_all.append(this_block)
        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    block_df = pd.concat(block_all).sort_values(by="height", ascending=True)

    rename_these = {'height': 'blockHeight',
            'createDate': 'block_createDate',
            'txCount': 'block_txCount',
            'amount': 'block_amount',
            'fee': 'block_fee',
            'hash': 'block_hash'}

    block_df.columns = [rename_these.get(i, i) for i in block_df.columns]
    block_df = block_df.drop_duplicates()
    return block_df


## just adding multiple try catch so that hopefully it'll go through
try:
    block_df = block_df_using_community_tracker(total_pages=500)
except:
    try:
        block_df = block_df_using_community_tracker(total_pages=500)
    except:
        try:
            block_df = block_df_using_newer_icon_tracker(page_count=500)
        except:
            block_df = block_df_using_original_icon_tracker(page_count=500)


windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

# saving block data balance
block_df.to_csv(os.path.join(windows_path, 'transaction_blocks_' + date_prev + '.csv'), index=False)