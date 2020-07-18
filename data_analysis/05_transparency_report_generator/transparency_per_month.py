
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
    res_dct = {lst1[i]: lst2[i] for i in range(0, len(lst1), 2)}
    return res_dct

contract_d = Convert(contract_address, contract_name)

# updating known address with other contract addresses
jknown_address.update(contract_d)



# making same table but with different column names
known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['dest_address', 'dest_def'])
known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from_address', 'from_def'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Rewards Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
prep_address = 'hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b'
# prep_address = 'hxa224bb59e9ba930f3919b57feef7656f1139d24b'
# prep_address = 'hx54d6f19c3d16b2ef23c09c885ca1ba776aaa80e2'
# prep_address = 'hx9fa9d224306b0722099d30471b3c2306421aead7'

# prep_address = 'hxab751d4e83b6fda412a38cb5f7f96860396b1327'


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

# rewards_df.to_csv(os.path.join(inPath, 'test_rewards_x.csv'), index=False)




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
loop_these_wallets = []
after_destination_all = []

loop_these_wallets = u_address

for j in range(len(loop_these_wallets)):

    # getting transaction information from icon transaction page
    tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address=' + loop_these_wallets[j],
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
                              '&address=' + loop_these_wallets[j], headers={'User-Agent': 'Mozilla/5.0'})
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

    # attaching address info (definition)
    df_after_destination = pd.merge(combined_df, known_address_details_from, how='left', on='from_address')
    df_after_destination = pd.merge(df_after_destination, known_address_details_to, how='left', on='dest_address')

    after_destination_all.append(df_after_destination)

    # nan would continue to loop and add to the list
    is_nan = df_after_destination['dest_def'].isna()
    # same_address = df_after_destination['dest_address'] != loop_these_wallets[j]
    # known_address = any(known_address_details_to['dest_address'].isin([loop_these_wallets[j]]))
    # end_loop_condition = is_nan|known_address

    add_to_loop = pd.Series(df_after_destination[is_nan].dest_address.unique())
    add_to_loop = add_to_loop[~add_to_loop.isin(u_address)]     # if already exists then do not add

    # if we find more unknown addresses, add to loop
    if len(add_to_loop) != 0:

        loop_these_wallets.append(add_to_loop)

        j = j + len(add_to_loop)

    else:
        pass


# finally concatenate all the data
after_destination_all = pd.concat(after_destination_all)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Concatenate data together  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# append data, remove duplicates and sort it ascending
concat_df = pd.concat([prep_wallet_tx, after_destination_all]).sort_values(by='datetime').drop_duplicates().reset_index(drop=True)

# indexing first time P-Rep reward was transferred out
first_reward_index = concat_df.loc[(concat_df.from_address == prep_address) & (concat_df.amount != 0)].index[0]

# remove all before first reward transfer
concat_df = concat_df.iloc[first_reward_index:-1].sort_values(by='datetime', ascending=False).reset_index(drop=True)


#~~~~~~~~~~ Assign unknown wallet a name (e.g. wallet_1, wallet_2) by appeared date ~~~~~~~~~#
# get unique address for both from and to transactions
address_1 = concat_df.rename(columns={"from_address": "address"}).sort_values(by=['address', 'datetime']).\
    groupby(['address']).first().reset_index()[['address', 'datetime']]
address_2 = concat_df.rename(columns={"dest_address": "address"}).sort_values(by=['address', 'datetime']).\
    groupby(['address']).first().reset_index()[['address', 'datetime']]

# concatenate & remove duplicates, attach wallet info from iconwatch
concat_address = pd.concat([address_1, address_2]).sort_values(by=['address', 'datetime']).\
    groupby('address').first().reset_index().sort_values(by='datetime')

wallet_info = []
wallet_info = pd.merge(concat_address, known_address_details_from, left_on='address', right_on='from_address', how='left').\
    drop(columns='from_address').rename(columns={"from_def": "wallet_def"})
# if nan, then assign wallet_1, wallet_2 etc
wallet_info['wallet_count'] = 'wallet_' + wallet_info.groupby(['wallet_def']).cumcount().add(1).astype(str).apply(lambda x: x.zfill(3))
wallet_info['wallet_def'].fillna(wallet_info.wallet_count, inplace=True)
wallet_info = wallet_info.drop(columns=['datetime', 'wallet_count'])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# Add it back to data to be analysed
concat_df = pd.merge(concat_df, wallet_info, left_on='from_address', right_on='address', how='left').\
    drop(columns=['address', 'from_def']).rename(columns={'wallet_def': 'from_def'})

concat_df = pd.merge(concat_df, wallet_info, left_on='dest_address', right_on='address', how='left').\
    drop(columns=['address', 'dest_def']).rename(columns={'wallet_def': 'dest_def'})

concat_df[['amount', 'fee']] = concat_df[['amount', 'fee']].apply(pd.to_numeric, errors='coerce', axis=1)


# concat_df.to_csv('test.csv')

# nodes
# nodesize_from = concat_df.rename(columns={'from_def': 'def'}).groupby(['def'])['amount'].sum()
# nodesize_dest = concat_df.rename(columns={'dest_def': 'def'}).groupby(['def'])['amount'].sum()
# nodesize = pd.merge(nodesize_from, nodesize_dest, on='def', how='outer').fillna(0)
# nodesize['amount'] = abs(nodesize['amount_x'] - nodesize['amount_y'])
# nodesize = nodesize.drop(columns=['amount_x', 'amount_y'])
# nodesize.at['governance', 'amount'] = concat_df.fee.sum()    # transaction fee added to governance
# nodesize = nodesize.drop(columns=['amount_x', 'amount_y']).squeeze()





edges = concat_df.groupby(['from_def', 'dest_def']).from_address.agg('count').reset_index().rename(columns={'from_address': 'weight'})


nodesize = concat_df.groupby(['from_def', 'dest_def']).amount.agg('sum').reset_index()
nodesize = pd.melt(nodesize, id_vars=['amount'], value_vars=['from_def', 'dest_def']).drop(columns='variable')
nodesize = nodesize.groupby(['value']).agg('sum')
nodesize.at['governance', 'amount'] = concat_df.fee.sum()
nodesize = nodesize.squeeze()


import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
import matplotlib.pyplot as plt

fig1 = plt.figure(1)
G = nx.from_pandas_edgelist(concat_df, 'from_def', 'dest_def', create_using=nx.DiGraph())

nodesize = nodesize.loc[list(G.nodes)]

nx.draw(G, pos=nx.nx_pydot.graphviz_layout(G), with_labels=True, node_size=nodesize/2, alpha=0.3, arrows=True)





# G = nx.from_pandas_edgelist(concat_df, 'from_def', 'dest_def', True, nx.DiGraph())

# node_attr = concat_df.set_index('amount').to_dict()

# nx.set_node_attributes(G, concat_df.set_index('amount').to_dict())
# nx.draw(G)

# D = nx.to_networkx_graph(a, create_using=nx.DiGraph)



# nx.set_node_attributes(concat_df, nodesize.set_index('amount').to_dict('index'))








# fig2 = plt.figure(2)
# G = nx.from_pandas_edgelist(concat_df, 'from_def', 'dest_def', create_using=nx.DiGraph())
# nx.draw(G, pos=nx.nx_pydot.graphviz_layout(G), with_labels=True, node_size=100, alpha=0.3, arrows=True)
#
#
#
#
# fig3 = plt.figure(3)
# G = nx.from_pandas_edgelist(concat_df, 'from_def', 'dest_def', create_using=nx.DiGraph())
# nx.draw(G, pos=nx.fruchterman_reingold_layout(G), with_labels=True, node_size=nodesize, alpha=0.3, arrows=True)
#
#
# nx.write_gexf(G,'test_this.gexf')
