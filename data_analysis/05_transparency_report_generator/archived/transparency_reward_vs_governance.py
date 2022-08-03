
#########################################################################
## Project: Transparency Report Auto Generator                         ##
## Date: August 2020                                                   ##
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
# Data put together in a DataFrame, saved separately in CSV format.

# import json library
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "output")
if not os.path.exists(outPath):
    os.mkdir(outPath)


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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX Address Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
known_address_url = Request('https://iconwat.ch/data/thes', headers={'User-Agent': 'Mozilla/5.0'})
jknown_address = json.load(urlopen(known_address_url))

# add any known addresses here manually
jknown_address.update({'hx02dd8846baddc171302fb88b896f79899c926a5a': 'ICON_Vote_Monitor'})

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



# making same table but with different column names
known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['dest_address', 'dest_def'])
known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from_address', 'from_def'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Rewards Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# prep_address = 'hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b'
# prep_address = 'hxc1977e22673937ab9ec1c7d48a8d5750f2cde1dc'


# prep_address = 'hxa224bb59e9ba930f3919b57feef7656f1139d24b'
# prep_address = 'hx54d6f19c3d16b2ef23c09c885ca1ba776aaa80e2'
# prep_address = 'hx9fa9d224306b0722099d30471b3c2306421aead7'

# prep_address = 'hxab751d4e83b6fda412a38cb5f7f96860396b1327'

# prep_address = 'hx76dcc464a27d74ca7798dd789d2e1da8193219b4'

prep_address = 'hx56ef2fa4ebd736c5565967197194da14d3af88ca'

prep_address = 'hxa224bb59e9ba930f3919b57feef7656f1139d24b' # catalyst

prep_rewards_url = Request('https://tracker.icon.foundation/v3/address/claimIScoreList?count=1&address='
                           + prep_address, headers={'User-Agent': 'Mozilla/5.0'})

# first getting total number of tx from website (this will need to change because icon page will have 500k max)
jprep_rewards_url = json.load(urlopen(prep_rewards_url))
totalSize = extract_values(jprep_rewards_url, 'totalSize')

# get page count to loop over
reward_count = 100
page_count = round((totalSize[0] / reward_count) + 0.5)

prep_rewards_all = []
i = []
for i in range(0, page_count):

    # then apply total pages to extract correct amount of data
    prep_rewards_url = Request('https://tracker.icon.foundation/v3/address/claimIScoreList?page=' + str(i+1) + '&count=' + str(reward_count)
                          + '&address=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})
    # json format
    jprep_rewards_url = json.load(urlopen(prep_rewards_url))
    prep_rewards_all.append(jprep_rewards_url)

# extracting information by labels
rewards_address = extract_values(prep_rewards_all, 'address')
rewards_block = extract_values(prep_rewards_all, 'height')
rewards_date = extract_values(prep_rewards_all, 'createDate')
rewards_icx = extract_values(prep_rewards_all, 'icx')

rewards_d = {'address': rewards_address,
             'block_id': rewards_block,
             'datetime': rewards_date,
             'rewards': rewards_icx}

rewards_df = pd.DataFrame(data=rewards_d)

rewards_df['date'] = pd.to_datetime(rewards_df['datetime']).dt.strftime("%Y-%m-%d")
rewards_df['month'] = pd.to_datetime(rewards_df['datetime']).dt.strftime("%Y-%m")
rewards_df['rewards'] = rewards_df['rewards'].astype(float)
# rewards_df.to_csv(os.path.join(inPath, 'test_rewards_x.csv'), index=False)



rewards_by_month = rewards_df.groupby(['month'])['rewards'].agg('sum')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting transaction information from icon transaction page
prep_tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address='
                      + prep_address, headers={'User-Agent': 'Mozilla/5.0'})


# first getting total number of tx from website (this will need to change because icon page will have 500k max)
jprep_prep_tx_url = json.load(urlopen(prep_tx_url))
totalSize = extract_values(jprep_prep_tx_url, 'totalSize')

# get page count to loop over
tx_count = 100
page_count = round((totalSize[0] / tx_count) + 0.5)

prep_tx_all = []
i = []
for i in range(0, page_count):

    # then apply total pages to extract correct amount of data
    prep_tx_url = Request('https://tracker.icon.foundation/v3/address/txList?page=' + str(i+1) + '&count=' + str(tx_count)
                          + '&address=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})
    # json format
    jprep_list_url = json.load(urlopen(prep_tx_url))
    prep_tx_all.append(jprep_list_url)

# extracting p-rep information by labels
tx_from_address = extract_values(prep_tx_all, 'fromAddr')
tx_to_address = extract_values(prep_tx_all, 'toAddr')
tx_date = extract_values(prep_tx_all, 'createDate')
tx_amount = extract_values(prep_tx_all, 'amount')
tx_fee = extract_values(prep_tx_all, 'fee')
tx_state = extract_values(prep_tx_all, 'state')

# combining lists
combined = {'from_address': tx_from_address,
     'dest_address': tx_to_address,
     'datetime': tx_date,
     'amount': tx_amount,
     'fee': tx_fee,
     'state': tx_state}

# convert into dataframe
combined_df = pd.DataFrame(data=combined)

# removing failed transactions & drop 'state (state for transaction success)'
combined_df = combined_df[combined_df.state != 0].drop(columns='state')

# shorten date info
combined_df['date'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m-%d")
combined_df['month'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m")


# conditionally remove last 2 containing 2000 (registration) -- perhaps leave them in
# if combined_df[-2:-1]['amount'].values[0] == str(2000):
#
#     combined_df.drop(combined_df.tail(2).index, inplace=True)
#
# else:
#     pass

prep_wallet_tx = pd.merge(combined_df, known_address_details_from, how='left', on='from_address')
prep_wallet_tx = pd.merge(prep_wallet_tx, known_address_details_to, how='left', on='dest_address')

# unique address that p-rep sent rewards to to get detailed transactions below
u_address = pd.Series(prep_wallet_tx[prep_wallet_tx['dest_def'].isna()].dest_address.unique())














#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction After P-Rep Wallet  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    # getting transaction information from icon transaction page
    tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address=' + prep_address,
                                   headers={'User-Agent': 'Mozilla/5.0'})

    # first getting total number of tx from website (this will need to change because icon page will have 500k max)
    jtx_list_url = json.load(urlopen(tx_url))
    totalSize = extract_values(jtx_list_url, 'totalSize')

    # get page count to loop over
    tx_count = 100
    page_count = round((totalSize[0] / tx_count) + 0.5)

    tx_all = []

    for i in range(0, page_count):

        # then apply total pages to extract correct amount of data
        tx_url = Request('https://tracker.icon.foundation/v3/address/txList?page=' + str(i+1) + '&count=' + str(tx_count) +
                              '&address=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})
        # json format
        jtx_list_url = json.load(urlopen(tx_url))
        tx_all.append(jtx_list_url)

    # extracting p-rep information by labels
    tx_from_address = extract_values(tx_all, 'fromAddr')
    tx_to_address = extract_values(tx_all, 'toAddr')
    tx_date = extract_values(tx_all, 'createDate')
    tx_amount = extract_values(tx_all, 'amount')
    tx_fee = extract_values(tx_all, 'fee')
    tx_state = extract_values(tx_all, 'state')

    # combining lists
    combined = {'from_address': tx_from_address,
         'dest_address': tx_to_address,
         'datetime': tx_date,
         'amount': tx_amount,
         'fee': tx_fee,
         'state': tx_state}

    # convert into dataframe
    combined_df = pd.DataFrame(data=combined)

    # removing failed transactions & drop 'state (state for transaction success)'
    combined_df = combined_df[combined_df.state != 0].drop(columns='state')

    # shorten date info
    combined_df['date'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m-%d")
    combined_df['month'] = pd.to_datetime(combined_df['datetime']).dt.strftime("%Y-%m")


    combined_df.append(combined_df)


test = combined_df.sort_values(by='datetime', ascending=True)
test['amount'] = test['amount'].astype(float)
test['amount'] = np.where(test['dest_address'] == prep_address, test['amount'], test['amount']*-1)
test['cum_amount'] = test['amount'].cumsum()


import seaborn as sns
import matplotlib.pyplot as plt
sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='date', y='cum_amount', data=test, palette=sns.color_palette('husl', n_colors=2))


plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
n=2
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()

