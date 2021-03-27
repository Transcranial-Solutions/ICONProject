
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
from datetime import date, datetime, timedelta
import numpy as np
import os
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "output")
if not os.path.exists(outPath):
    os.mkdir(outPath)



# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(1)
    if string:
        return yesterday.strftime('%Y_%m_%d')
    return yesterday

# today's date
# to use specific date, otherwise use today
use_specific_prev_date = 0
date_is_range = 0 # if date is range (1) or is one  date (0)

if use_specific_prev_date == 1:
    day_today = "2021_03_04"
    day_prev = "2021_03_03"
    day_today_text = day_today.replace("_", "-")
    day_prev_text = day_prev.replace("_", "-")
else:
    # today
    today = datetime.utcnow()
    day_today = today.strftime("%Y_%m_%d")
    day_today_text = day_today.replace("_","-")

    # day before today
    day_prev = yesterday(today)
    day_prev_text = day_prev.replace("_","-")


date_range = pd.date_range(start=day_prev_text, end=day_today_text, freq='D').strftime("%Y-%m-%d").to_list()


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
jknown_address.update({'hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310': 'upbit_hot1'})
jknown_address.update({'hxfdb57e23c32f9273639d6dda45068d85ee43fe08': 'upbit_hot2'})
jknown_address.update({'hx4a01996877ac535a63e0107c926fb60f1d33c532': 'upbit_hot3'})
jknown_address.update({'hx8d28bc4d785d331eb4e577135701eb388e9a469d': 'upbit_hot4'})
jknown_address.update({'hxf2b4e7eab4f14f49e5dce378c2a0389c379ac628': 'upbit_hot5'})

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

    known_contract_url = Request('https://tracker.icon.foundation/v3/contract/list?page=' + str(i+1) + '&count=' + str(contract_count), headers={'User-Agent': 'Mozilla/5.0'})

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
# making same table but with different column names
known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['dest_address', 'dest_def'])
known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from_address', 'from_def'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# Binance Hot
this_address = 'hxc4193cda4a75526bf50896ec242d6713bb6b02a3'
# this_address = 'hx54d6f19c3d16b2ef23c09c885ca1ba776aaa80e2' #ubik
# this_address = 'hxd0d9b0fee857de26fd1e8b15209ca15b14b851b2' #velic


# getting transaction information from icon transaction page
tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address='
                      + this_address, headers={'User-Agent': 'Mozilla/5.0'})


# first getting total number of tx from website (this will need to change because icon page will have 500k max)
jtx_url = json.load(urlopen(tx_url))
totalSize = extract_values(jtx_url, 'totalSize')

createDate = str(extract_values(jtx_url, 'createDate')[0])
createDate = createDate.split('T', 1)[0]

# get page count to loop over
if totalSize[0] > 100:
    tx_count = 100
    page_count = round((totalSize[0] / tx_count) + 0.5)
else:
    tx_count = totalSize[0]
    page_count = 1

tx_all = []
i = []
for i in range(0, page_count):

    # then apply total pages to extract correct amount of data
    tx_url = Request('https://tracker.icon.foundation/v3/address/txList?page=' + str(i+1) + '&count=' + str(tx_count) + '&address=' + this_address, headers={'User-Agent': 'Mozilla/5.0'})

    # json format
    jlist_url = json.load(urlopen(tx_url))

    ## This will capture everything within the date of interest (last_createDate)
    # first createDate for first record of previous date
    first_createDate = str(extract_values(jlist_url, 'createDate')[0])
    first_createDate = first_createDate.split('T', 1)[0]

    # last createDate for last record of current date of interest
    last_createDate = str(extract_values(jlist_url, 'createDate')[tx_count-1])
    last_createDate = last_createDate.split('T', 1)[0]

    print(first_createDate)
    if use_specific_prev_date == 1:
        tx_all.append(jlist_url)

    if use_specific_prev_date == 0:
        if last_createDate == day_today_text or first_createDate == day_today_text:
            tx_all.append(jlist_url)
        else:
            pass

    if first_createDate <= day_prev_text or first_createDate < day_today_text:
        break

if len(tx_all) != 0:

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


    if use_specific_prev_date == 0:
        combined_df = combined_df[combined_df['date'] == day_today_text]
    if use_specific_prev_date == 1 & date_is_range == 0:
        combined_df = combined_df[combined_df['date'] == day_today_text]
    if use_specific_prev_date == 1 & len(date_range) > 2:
        combined_df = combined_df[combined_df['date'].isin(date_range)]



    wallet_tx = pd.merge(combined_df, known_address_details_from, how='left', on='from_address')
    wallet_tx = pd.merge(wallet_tx, known_address_details_to, how='left', on='dest_address')


    # unique address that interacted with wallet of interest
    u_address = pd.Series(wallet_tx['dest_address'].append(wallet_tx['from_address']).unique())

    wallet_tx = []

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transactions associated with wallets  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    loop_these_wallets = []
    after_destination_all = []

    loop_these_wallets = u_address.copy()

    for j in range(len(loop_these_wallets)):

        # getting transaction information from icon transaction page
        tx_url = Request('https://tracker.icon.foundation/v3/address/txList?count=1&address=' + loop_these_wallets[j], headers={'User-Agent': 'Mozilla/5.0'})

        # first getting total number of tx from website (this will need to change because icon page will have 500k max)
        jtx_list_url = json.load(urlopen(tx_url))
        totalSize = extract_values(jtx_list_url, 'totalSize')

        # get page count to loop over
        if totalSize[0] > 100:
            tx_count = 100
            page_count = round((totalSize[0] / tx_count) + 0.5)
        else:
            tx_count = totalSize[0]
            page_count = 1

        tx_all = []
        i = []
        for i in range(0, page_count):

            # then apply total pages to extract correct amount of data
            tx_url = Request(
                'https://tracker.icon.foundation/v3/address/txList?page=' + str(i + 1) + '&count=' + str(tx_count) + '&address=' + loop_these_wallets[j], headers={'User-Agent': 'Mozilla/5.0'})

            # json format
            jlist_url = json.load(urlopen(tx_url))

            ## This will capture everything within the date of interest (last_createDate)
            # first createDate for first record of previous date
            first_createDate = str(extract_values(jlist_url, 'createDate')[0])
            first_createDate = first_createDate.split('T', 1)[0]

            # last createDate for last record of current date of interest
            last_createDate = str(extract_values(jlist_url, 'createDate')[tx_count - 1])
            last_createDate = last_createDate.split('T', 1)[0]

            print(first_createDate)

            # if use_specific_prev_date == 1:
            #     tx_all.append(jlist_url)

            # if use_specific_prev_date == 0:
            if last_createDate == day_today_text or first_createDate == day_today_text:
                tx_all.append(jlist_url)
            else:
                pass

            if first_createDate <= day_prev_text or first_createDate < day_today_text:
                break

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

        if use_specific_prev_date == 0:
            combined_df = combined_df[combined_df['date'] == day_today_text]
        if use_specific_prev_date == 1 & date_is_range == 0:
            combined_df = combined_df[combined_df['date'] == day_today_text]
        if use_specific_prev_date == 1 & len(date_range) > 2:
            combined_df = combined_df[combined_df['date'].isin(date_range)]

        # attaching address info (definition)

        df_after_destination = pd.merge(combined_df, known_address_details_from, how='left', on='from_address')
        df_after_destination = pd.merge(df_after_destination, known_address_details_to, how='left', on='dest_address')

        after_destination_all.append(df_after_destination)

        # nan would continue to loop and add to the list
        is_nan = df_after_destination['dest_def'].isna()

        add_to_loop = pd.Series(df_after_destination[is_nan].dest_address.unique())
        add_to_loop = add_to_loop[~add_to_loop.isin(u_address)]     # if already exists then do not add

        # if we find more unknown addresses, add to loop
        if len(add_to_loop) != 0:

            loop_these_wallets.append(add_to_loop)

            j = j + len(add_to_loop)

        else:
            pass

        print("done: " + str(j) + " out of " + str(len(loop_these_wallets)))

    # finally concatenate all the data
    after_destination_all = pd.concat(after_destination_all)















    concat_df = after_destination_all.copy()



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Concatenate data together  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


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

    # re-ordering to get the wallet of interest to the top
    wallet_info['this_order'] = np.where(wallet_info['address'] == this_address, 1, 2)
    wallet_info = wallet_info.sort_values(by=['this_order', 'datetime']).drop(columns='this_order')

    # if nan, then assign w_001, w_002 etc
    wallet_info['wallet_count'] = 'w_' + wallet_info.groupby(['wallet_def']).cumcount().add(1).astype(str).apply(lambda x: x.zfill(3))
    wallet_info['wallet_def'].fillna(wallet_info.wallet_count, inplace=True)
    wallet_info = wallet_info.drop(columns=['datetime', 'wallet_count'])


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    # Add it back to data to be analysed
    concat_df = pd.merge(concat_df, wallet_info, left_on='from_address', right_on='address', how='left').\
        drop(columns=['address', 'from_def']).rename(columns={'wallet_def': 'from_def'})

    concat_df = pd.merge(concat_df, wallet_info, left_on='dest_address', right_on='address', how='left').\
        drop(columns=['address', 'dest_def']).rename(columns={'wallet_def': 'dest_def'})

    concat_df[['amount', 'fee']] = concat_df[['amount', 'fee']].apply(pd.to_numeric, errors='coerce', axis=1)

    # concat_df = concat_df[concat_df['month'] == '2021-01']
    # concat_df.to_csv('test.csv')

    # edges (for gephi?)
    edges = concat_df.groupby(['from_def', 'dest_def']).from_address.agg('count').reset_index().rename(columns={'from_address': 'weight'})



    import networkx as nx
    from networkx.drawing.nx_agraph import graphviz_layout
    import matplotlib.pyplot as plt

    # nodesize_ratio = 5
    nodesize_ratio = 100

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # nodes from
    nodesize_from = concat_df.rename(columns={'from_def': 'def'}).groupby(['def'])['amount'].sum()
    nodesize_dest = concat_df.rename(columns={'dest_def': 'def'}).groupby(['def'])['amount'].sum()
    nodesize = pd.merge(nodesize_from, nodesize_dest, on='def', how='outer').fillna(0)



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    name_this_address = known_address_details_from[known_address_details_from['from_address'] == this_address].pop('from_def').iloc[0]

    edge_width_ratio = 5
    # summarising df
    temp_df = concat_df[['from_def', 'dest_def', 'amount']].groupby(['from_def', 'dest_def']).amount.agg(['sum', 'count']).reset_index()
    temp_df = temp_df.rename(columns={'sum': 'amount', 'count': 'weight'})
    temp_df['weight_ratio'] = temp_df['weight'] / edge_width_ratio  # to change width of edges based on number of tx
    temp_df['color'] = np.where(temp_df['from_def'] == name_this_address, 'r', 'g')


    nodesize = nodesize.reset_index()
    nodesize['total'] = nodesize['amount_y'] - nodesize['amount_x']
    nodesize = nodesize[['def','total']]

    # for node label
    wallet_001_amount = nodesize.reset_index(drop=True)
    wallet_001_amount = wallet_001_amount[wallet_001_amount['def'] == name_this_address]
    wallet_001_amount['text'] = wallet_001_amount['total']
    # wallet_001_amount['text'] = np.where(wallet_001_amount['total']>0, wallet_001_amount['total'], 0)
    wallet_001_amount['text'] = name_this_address + ': \n' + wallet_001_amount['text'].round(0).astype(int).apply('{:,}'.format).astype(str) + '\n ICX'
    wallet_001_amount = wallet_001_amount.drop(columns=['total'])

    # node label final
    node_label = []
    node_label = nodesize.drop(columns=['total'])
    node_label = pd.merge(node_label, wallet_001_amount, how='left', on='def')
    node_label['text'] = np.where(node_label['text'].isna(), node_label['def'], node_label['text'])


    node_labeldict = node_label.set_index('def')['text'].to_dict()

    edge_labeldict = temp_df.drop(columns='weight')
    edge_labeldict['amount'] = edge_labeldict['amount'].round(0).astype(int).apply('{:,}'.format).astype(str) + '\n ICX'

    # going out balance
    edge_labeldict_r = edge_labeldict[edge_labeldict['color']=='r']
    edge_labeldict_r = edge_labeldict_r.set_index(['from_def','dest_def']).pop("amount").to_dict()

    # coming in balance
    edge_labeldict_g = edge_labeldict[edge_labeldict['color']=='g']
    edge_labeldict_g = edge_labeldict_g.set_index(['from_def','dest_def']).pop("amount").to_dict()

    edge_labeldict = edge_labeldict.set_index(['from_def','dest_def']).pop("amount").to_dict()


    vmax_val = 0
    leftright = 100
    updown = -220

    ## figure
    plt.style.use(['dark_background'])
    fig = plt.figure(figsize=(12,8))
    # fig, ax = plt.subplots(nrows=1, ncols=1)
    ax = plt.gca()
    ax.set_title('ICX flow from Binance Hot Wallet ' + '(' + day_today_text + ')')
    G = nx.from_pandas_edgelist(temp_df, 'from_def', 'dest_def', create_using=nx.MultiDiGraph())
    pos = nx.nx_pydot.graphviz_layout(G)

    nodesize_ratio = nodesize_ratio
    nodesize = nodesize.set_index('def').squeeze().loc[list(G.nodes)]  # to align the data with G.nodes

    edgelist = list(G.edges)
    edgelist = [el[:2] for el in edgelist]
    temp_df = temp_df.set_index(['from_def','dest_def']).squeeze().loc[edgelist]

    color_map_1 = []
    for node in G:
        if node == name_this_address:
            color_map_1.append('brown')
        else:
            color_map_1.append('steelblue')

    colors = temp_df['weight'].values
    cmap = plt.cm.YlGn
    # cmap = plt.cm.Blues
    vmin = 0

    if vmax_val == 0:
        vmax = max(temp_df['weight'].values)
    else:
        vmax = vmax_val

    g = nx.draw(G, node_color=color_map_1, pos=pos,
                with_labels=True, labels=node_labeldict, connectionstyle='arc3, rad = 0.15',
                node_size=nodesize / nodesize_ratio, alpha=0.8, arrows=True,
                font_size=8, font_color='azure',
                edge_color=colors,
                edge_cmap=cmap,
                vmin=vmin, vmax=vmax,
                width=temp_df['weight_ratio'])  # fontweight='bold',

    # ax.set_facecolor('black')



    # nx.draw_networkx_edge_labels(G, pos, font_size=6,
    #                              edge_labels=edge_labeldict_r,
    #                              label_pos=0.6,
    #                              font_color='r', bbox=dict(alpha=0))
    #
    # nx.draw_networkx_edge_labels(G, pos, font_size=6,
    #                              edge_labels=edge_labeldict_g,
    #                              label_pos=0.6,
    #                              font_color='g', bbox=dict(alpha=0))

    plt.axis('off')
    plt.tight_layout()
    ax.text(leftright, updown,' (1) Circle size represents amount transacted \n (2) Arrow thickness/colour represents number of transactions')


    # # Set Up Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm._A = []
    clb = plt.colorbar(sm, shrink=0.5)
    clb.ax.set_title('Number of TX')

    fig.set_facecolor('black')

else:
    print("no data this time")












