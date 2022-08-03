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

def output_block_data(block_all):
    tracker_height = extract_values(block_all, 'height')
    tracker_createDate = extract_values(block_all, 'createDate')
    tracker_txcount = extract_values(block_all, 'txCount')
    tracker_amount = extract_values(block_all, 'amount')
    tracker_fee = extract_values(block_all, 'fee')
    tracker_hash = extract_values(block_all, 'hash')


    combined = {'blockHeight': tracker_height,
        'block_createDate': tracker_createDate,
        'block_txCount': tracker_txcount,
        'block_amount': tracker_amount,
        'block_fee': tracker_fee,
        'block_hash': tracker_hash,}

    combined_df = pd.DataFrame(data=combined).sort_values(by="blockHeight", ascending=True)
    return combined_df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# collecting block info using multithreading

start = time()

pbar = tqdm(range(page_count))
pbar.bar_format = "{desc:<15}{percentage:3.0f}%|{bar:75}{r_bar}"
block_all = []
for k in pbar:
    try:
        this_block = get_block_data(page_count=k+1, block_count=block_count)
        block_all.append(this_block)
    except:
        random_sleep_except = random.uniform(240,360)
        print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
        time.sleep(random_sleep_except) #sleep the script for x seconds and....#
        continue


print(f'Time taken: {time() - start}')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

combined_df = output_block_data(block_all)



windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

# saving block data balance
combined_df.to_csv(os.path.join(windows_path, 'transaction_blocks_' + date_prev + '.csv'), index=False)