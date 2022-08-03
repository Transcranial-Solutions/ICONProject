
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

# This file extracts information from Everstake's iconvote monitor (https://iconvotemonitor.com/)

# import json library
# import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
# from datetime import datetime
import os
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from datetime import date
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
resultsPath = os.path.join(outPath, "results")
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





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting contract information from icon transaction page
monitor_info = Request('https://api.iconvotemonitor.com/accounts?limit=1&offsent=0', headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of record from website
monitor_info = json.load(urlopen(monitor_info))

record_per_page = 50
total_record = extract_values(monitor_info, 'total')

# get page count to loop over
page_count = round((total_record[0] / record_per_page) + 0.5) + 5 # adding 5 pages to cater for extraction time


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting delegator info
count = 0
def get_info(record_per_page, offset):
    global count
    # Request url
    # req = Request('https://api.iconvotemonitor.com/accounts?limit=' + str(record_per_page) + '&offset=' + str(offset), headers={'User-Agent': 'Mozilla/5.0'})

    try:
        req = Request('https://api.iconvotemonitor.com/accounts?limit=' + str(record_per_page) + '&offset=' +
                      str(total_record[0] + 5 - offset), headers={'User-Agent': 'Mozilla/5.0'})

        jreq = json.load(urlopen(req))

        # extracting data by labels
        created_at = extract_values(jreq, 'created_at')
        address = extract_values(jreq, 'address')
        balance = extract_values(jreq, 'balance')
        staking = extract_values(jreq, 'stake')
        delegating = extract_values(jreq, 'delegations')
        unstaking = extract_values(jreq, 'unstaking')

        # combining strings into list
        d = {'created_at': created_at,
             'address': address,
             'balance': balance,
             'staking': staking,
             'delegating': delegating,
             'unstaking': unstaking}

        # convert into dataframe
        df = pd.DataFrame(data=d)

        print("Done - " + str(count) + " out of " + str(page_count))
        # print("Done - " + str(offset) + " out of " + str(total_record[0]))

        count += 1
        # offset += record_per_page
        return(df)

    except:
        print("Something went wrong!!")


start = time()
all_info = []
offset = 50  #this is for extracting data from correct position
with ThreadPoolExecutor(max_workers=2) as executor:
    for k in range(page_count): #page_count
        all_info.append(executor.submit(get_info, record_per_page, offset))
        offset += record_per_page #this will keep adding the record_per_page (50 here) so offset changes each loop

temp_df = []
for task in as_completed(all_info):
    temp_df.append(task.result())

final_info = pd.concat(temp_df)

print(f'Time taken: {time() - start}')

# today date
today = date.today()
day1 = today.strftime("%Y_%m_%d")

# save
final_info.to_csv(os.path.join(resultsPath, 'wallet_info_' + day1 + '.csv'), index=False)
# final_info = pd.read_csv(os.path.join(resultsPath, 'wallet_info_2020_11_30.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# adding date/datetime info
df = final_info.copy()
df = df.drop_duplicates()

# str to float and get total icx owned
col_list = ['balance', 'staking', 'unstaking']
df['delegating'] = df['delegating'].astype(float)
df[col_list] = df[col_list].astype(float)
df['total'] = df[col_list].sum(axis=1)
df['staking_but_not_delegating'] = df['staking'] - df['delegating']

# add date
df['created_at'] = df['created_at'].astype(int)
df['date'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d")

# only use wallets with 'hx' prefix
df = df[df['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

# in case if there is any duplicates, we take the lastest value
df = df.sort_values(by='created_at', ascending=False).groupby(['address']).first().reset_index()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# binning the balance
df_binned = df.copy()

bins = [-1, 0, 1, 1000, 5000, 10000, 25000, 50000, 100000,
        250000, 500000, 1000000, 5000000, 10000000, 25000000, 50000000, 100000000, 9999999999999]

names = ["0", "1 or less", "1 - 1K", "1K - 5K", "5K - 10K", "10K - 25K", "25K - 50K", "50K - 100K",
       "100K - 250K", "250K - 500K", "500K - 1M", "1M - 5M", "5M - 10M", "10M - 25M", "25M - 50M", "50M - 100M", "100M +"]


# bin based on above list and make a table
def get_binned_df(df, inVar):
    binned_df = df[[inVar]].\
        apply(pd.cut, bins=bins, labels=names).\
        groupby([inVar])[inVar].\
        agg('count').\
        reset_index(name='Count').\
        sort_values(by=inVar, ascending=False).\
        rename(columns={inVar: 'Amount (ICX)'}).\
        reset_index(drop=True)

    one_or_less_index = ~binned_df['Amount (ICX)'].isin(["0", "1 or less"])
    sum_1_plus = binned_df[one_or_less_index]['Count'].sum()

    binned_df['Percentage (>1 ICX)'] = binned_df['Count'] / sum_1_plus
    binned_df['Cumulative Percentage (>1 ICX)'] = binned_df['Percentage (>1 ICX)'].cumsum()

    binned_df.loc['Total'] = binned_df.sum(numeric_only=True, axis=0)

    binned_df['Amount (ICX)'] = np.where(
        binned_df['Amount (ICX)'].isna(), 'Total',
        binned_df['Amount (ICX)'])

    one_or_less_index = ~binned_df['Amount (ICX)'].isin(["0", "1 or less", "Total"])
    percentage_sum = binned_df[one_or_less_index]['Percentage (>1 ICX)'].sum()
    binned_df['Percentage (>1 ICX)'] = np.where(binned_df['Amount (ICX)'] == 'Total', percentage_sum, binned_df['Percentage (>1 ICX)'])
    binned_df['Cumulative Percentage (>1 ICX)'] = binned_df[one_or_less_index]['Cumulative Percentage (>1 ICX)'].map("{:.2%}".format)

    one_or_less_index = ~binned_df['Amount (ICX)'].isin(["0", "1 or less"])
    binned_df['Percentage (>1 ICX)'] = binned_df[one_or_less_index]['Percentage (>1 ICX)'].map("{:.2%}".format)
    binned_df['Count'] = binned_df['Count'].astype(int)
    binned_df = binned_df.fillna('-')

    return(binned_df)

# total
get_binned_df(df, 'total')

# balance
get_binned_df(df, 'staking')

# delegating
get_binned_df(df, 'delegating')

# unstaking
get_binned_df(df, 'unstaking')

# balance
get_binned_df(df, 'balance')

# staking but not delegating
get_binned_df(df, 'staking_but_not_delegating')


total_icx = df[['staking','unstaking','balance']].sum(numeric_only=True, axis=0).reset_index().sum()







#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
wallet_address = pd.read_csv(os.path.join(outPath, 'wallet_address_20201128.csv'))

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
wallet_address = wallet_address.drop_duplicates()

# to series
wallet_address = wallet_address.rename(columns={'to_address': 'address'})
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


check = wallet_address[~wallet_address['address'].isin(df_binned['address'])]

# more than 14K wallets missing






