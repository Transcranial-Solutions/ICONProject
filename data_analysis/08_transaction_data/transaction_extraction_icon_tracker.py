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
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from time import time
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

currPath = os.getcwd()
# currPath = "/home/tonoplast/IconProject/"
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
tx_url = Request('https://tracker.icon.foundation/v3/transaction/recentTx?page=1&count=1', headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of contract from website
# (this will need to change because icon page will have 500k max)
jtx = json.load(urlopen(tx_url))

tx_count = 100
listSize = extract_values(jtx, 'listSize')

# get page count to loop over
page_count = round((listSize[0] / tx_count) + 0.5)


def get_tx_data(page_count, tx_count=tx_count):
    tx_url = Request('https://tracker.icon.foundation/v3/transaction/recentTx?page=' + str(page_count) + '&count=' +
                            str(tx_count), headers={'User-Agent': 'Mozilla/5.0'})
    jtx = json.load(urlopen(tx_url))
    return jtx


# getting tx data
def run_tx(workers=1):
    start = time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        mytx = list(tqdm(executor.map(get_tx_data, range(page_count)), total=page_count))

    tx_df = pd.DataFrame.from_dict(mytx)
    tx_df = tx_df.explode('data').reset_index(drop=True)
    dat_df = pd.json_normalize(tx_df['data'])
    # tx_df.join(dat_df).drop(columns=['data', 'listSize', 'totalSize','result'])
    
    print(f'Time taken: {time() - start}')
    return dat_df

tx_df = run_tx()


def cleanup_df(df):
    df[['date', 'time']] = df['createDate'].str.split("T", expand=True)
    df[['time', 'useless_info']] = df['time'].str.split(".", expand=True)
    df = df.drop(columns = ['createDate','useless_info'])
    return df

df = cleanup_df(df=tx_df)

windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/08_transaction_data/data/"

if not os.path.exists(windows_path):
    os.makedirs(windows_path)


df_sliced = {}

# saving tx data by date
for date in df['date'].unique()[1:-1]:
    df_sliced[date] = df[df['date'] == date]
    df_sliced[date].to_csv(os.path.join(windows_path, 'transaction_data_icon_tracker_' + date + '.csv'), index=False)
    print("Transaction data on " + date + " have been saved.")

