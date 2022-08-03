
#########################################################################
## Project: ICON Network Wallet Ranking                                ##
## Date: August 2020                                                   ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# This file gets information from solidwallet (https://ctz.solidwallet.io/api/v3)

# import json library
# import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
# import numpy as np
# from datetime import datetime
import os
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
# import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import seaborn as sns

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "06_wallet_ranking")
if not os.path.exists(outPath):
    os.mkdir(outPath)
resultsPath = os.path.join(outPath, "../results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
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



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting p-rep information from icon governance page
prep_list_url = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000', headers={'User-Agent': 'Mozilla/5.0'})

# json format
jprep_list_url = json.load(urlopen(prep_list_url))

# extracting p-rep information by labels
prep_address = extract_values(jprep_list_url, 'address')
len_prep_address = len(prep_address)


# loop through P-Rep addresses and extract information, make into data frame
count = 0
def get_votes(prep_address, len_prep_address):
    global count
    # Request url, no limit here
    req = Request('https://api.iconvotemonitor.com/delegations?validators=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})
    jreq = json.load(urlopen(req))

    # extracting data by labels
    delegator = extract_values(jreq, 'delegator')
    votes = extract_values(jreq, 'amount')
    validator_name = extract_values(jreq, 'validator_name')

    # combining strings into list
    d = {'delegator': delegator,
         'validator_name': validator_name,
         'votes': votes}

    # convert into dataframe
    df = pd.DataFrame(data=d)

    df['votes'] = pd.to_numeric(df['votes'])
    df = df.groupby(['validator_name', 'delegator']).agg('sum').reset_index()

    count += 1
    try:
       print("Votes for " + validator_name[0] + ": Done - " + str(count) + " out of " + str(len_prep_address))
    except:
       print("An exception occurred - Possibly a new P-Rep without votes")

    return(df)

start = time()

all_votes = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(len(prep_address)):
        all_votes.append(executor.submit(get_votes, prep_address[k], len_prep_address))

temp_df = []
for task in as_completed(all_votes):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')

# all votes per wallet
all_votes = pd.concat(temp_df).groupby('delegator').agg('sum').reset_index()
all_votes['total_votes'] = all_votes['votes'].sum()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# load available wallets
wallet_address = pd.read_csv(os.path.join(outPath, 'wallet_address_20201128.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# clean up the wallet data (remove nan, fix bad addresses)

# drop NaN
wallet_address = wallet_address.dropna()

# finding bad address -- e.g. no 'hx' prefix
bad_address_idx = ~wallet_address['to_address'].str[:2].str.contains('hx|cx', case=False, regex=True, na=False)
bad_address = wallet_address[bad_address_idx]

# adding 'hx' prefix to the bad addresses
fixed_address = 'hx' + bad_address
wallet_address[bad_address_idx] = fixed_address

# remove dups because sometimes there can be
wallet_address = wallet_address.drop_duplicates().reset_index(drop=True)

# to series
wallet_address = wallet_address.rename(columns={'to_address': 'address'}).address

len_wallet_address = len(wallet_address)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# using solidwallet
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

count = 0
def get_balance(wallet_address, len_wallet_address):
    global count
    address = wallet_address
    try:
        balance = loop_to_icx(icon_service.get_balance(wallet_address))
    except:
        balance = float("NAN")
    df = {'address': address, 'balance': balance}
    count += 1
    print(str(count) + " out of " + str(len_wallet_address))
    return(df)

start = time()

all_balance = []
with ThreadPoolExecutor(max_workers=200) as executor:
    for i in range(0, 10000):
    # for i in range(len(wallet_address)):
        all_balance.append(executor.submit(get_balance, wallet_address[i], len_wallet_address))

temp_df = []
for task in as_completed(all_balance):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')


# convert to df
all_balance = pd.DataFrame(temp_df)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

all_balance_hx = all_balance[all_balance['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

all_balance_hx['total_icx'] = all_balance_hx['balance'].sum()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#






def bin_balance(row):

    if 1 <= row['balance'] <= 1000:
            val = '1 - 1K'
    elif 1000 < row['balance'] <= 5000:
            val = '1K - 5K'
    elif 5000 < row['balance'] <= 10000:
            val = '5K - 10K'
    elif 10000 < row['balance'] <= 25000:
            val = '10K - 25K'
    elif 25000 < row['balance'] <= 50000:
            val = '25K - 50K'
    elif 50000 < row['balance'] <= 100000:
            val = '50K - 100K'
    elif 100000 < row['balance'] <= 250000:
            val = '100K - 250K'
    elif 250000 < row['balance'] <= 500000:
            val = '250K - 500K'
    elif 500000 < row['balance'] <= 1000000:
            val = '500K - 1M'
    elif 1000000 < row['balance']:
            val = '1M +'
    else:
        pass
        val = -1
    return val

all_balance['balance_bin'] = all_balance.apply(bin_balance, axis=1)

# combined_df[combined_df['balance_bin'] == -1]

test = all_balance['percentage'].agg(sum)

summary_df_count = all_balance.groupby('balance_bin')['balance'].agg('count')


summary_df_count = summary_df_count.reindex(['1 - 1K', '1K - 5K', '5K - 10K', '10K - 25K',
                                 '25K - 50K', '50K - 100K', '100K - 250K', '250K - 500K',
                                 '500K - 1M', '1M +']).reset_index()

summary_df_sum = all_balance.groupby('balance_bin').agg('sum')
summary_df = summary_df_sum.reindex(['1 - 1K', '1K - 5K', '5K - 10K', '10K - 25K',
                                 '25K - 50K', '50K - 100K', '100K - 250K', '250K - 500K',
                                 '500K - 1M', '1M +']).reset_index()
