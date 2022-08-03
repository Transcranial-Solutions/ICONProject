
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
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider

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



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Wallet Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting address information from icon address page
wallet_balance_url = Request('https://tracker.icon.foundation/v3/address/list?page=1&count=100',
                         headers={'User-Agent': 'Mozilla/5.0'})

wallet_balance_url = Request('https://tracker.icon.foundation/v3/address/list?page=2&count=100',
                         headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of tx from website (this will need to change because icon page will have 500k max)
jwallet_balance_url = json.load(urlopen(wallet_balance_url))
resultSize = extract_values(jwallet_balance_url, 'result')[0]


wallet_balance_all = []
i = []
for i in range(0, int(resultSize)):

    # then apply total pages to extract correct amount of data
    wallet_balance_url = Request('https://tracker.icon.foundation/v3/address/list?page=' + str(i+1) + '&count=100',
                          headers={'User-Agent': 'Mozilla/5.0'})
    # json format
    jwallet_balance_url = json.load(urlopen(wallet_balance_url))
    wallet_balance_all.append(jwallet_balance_url)

    print("Doing " + str(i+1) + " out of " + resultSize + " pages..")

# extracting p-rep information by labels
wallet_address = extract_values(wallet_balance_all, 'address')
wallet_balance = extract_values(wallet_balance_all, 'balance')
wallet_icxUsd = extract_values(wallet_balance_all, 'icxUsd')
wallet_percentage = extract_values(wallet_balance_all, 'percentage')
wallet_txCount = extract_values(wallet_balance_all, 'txCount')

# combining lists
combined = {'address': wallet_address,
     'balance': wallet_balance,
     'icxUsd': wallet_icxUsd,
     'percentage': wallet_percentage,
     'txCount': wallet_txCount}

# convert into dataframe
combined_df = pd.DataFrame(data=combined)
combined_df['balance'] = combined_df['balance'].astype(float)
combined_df['icxUsd'] = combined_df['icxUsd'].astype(float)


# # making bins to extract group
# def bin_balance(row):
#
#
#     if 1 <= round(row['balance']) <= 500:
#         val = '1 – 500'
#     elif 501 <= round(row['balance']) <= 1500:
#         val = '501 – 1500'
#     elif 1501 <= round(row['balance']) <= 3500:
#         val = '1501 – 3500'
#     elif 3501 <= round(row['balance']) <= 5000:
#         val = '3501 – 5000'
#     elif 5001 <= round(row['balance']) <= 7500:
#         val = '5001 – 7500'
#     elif 7501 <= round(row['balance']) <= 9000:
#         val = '7501 – 9000'
#     elif 9001 <= round(row['balance']) <= 10000:
#         val = '9001 – 10000'
#     elif 10001 <= round(row['balance']) <= 15000:
#         val = '10001 – 15000'
#     elif 15001 <= round(row['balance']) <= 20000:
#         val = '15001 – 20000'
#     elif 20001 <= round(row['balance']) <= 25000:
#         val = '20001 – 25000'
#     elif 25001 <= round(row['balance']) <= 35000:
#         val = '25001 – 35000'
#     elif 35001 <= round(row['balance']) <= 50000:
#         val = '35001 – 50000'
#     elif 50001 <= round(row['balance']) <= 75000:
#         val = '50001 – 75000'
#     elif 75001 <= round(row['balance']) <= 125000:
#         val = '75001 – 125000'
#     elif 125001 <= round(row['balance']) <= 175000:
#         val = '125001 – 175000'
#     elif 175001 <= round(row['balance']) <= 250000:
#         val = '175001 – 250000'
#     elif 250001 <= round(row['balance']) <= 500000:
#         val = '250001 – 500000'
#     elif 500001 <= round(row['balance']) <= 1000000:
#         val = '500001 – 1000000'
#     elif 1000001 <= round(row['balance']):
#         val = '1000000 +'
#     else:
#         pass
#         val = - 1
#     return val


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

combined_df['balance_bin'] = combined_df.apply(bin_balance, axis=1)

# combined_df[combined_df['balance_bin'] == -1]

test = combined_df['percentage'].agg(sum)

summary_df_count = combined_df.groupby('balance_bin')['balance'].agg('count')


summary_df_count = summary_df_count.reindex(['1 - 1K', '1K - 5K', '5K - 10K', '10K - 25K',
                                 '25K - 50K', '50K - 100K', '100K - 250K', '250K - 500K',
                                 '500K - 1M', '1M +']).reset_index()

summary_df_sum = combined_df.groupby('balance_bin').agg('sum')
summary_df = summary_df_sum.reindex(['1 - 1K', '1K - 5K', '5K - 10K', '10K - 25K',
                                 '25K - 50K', '50K - 100K', '100K - 250K', '250K - 500K',
                                 '500K - 1M', '1M +']).reset_index()
