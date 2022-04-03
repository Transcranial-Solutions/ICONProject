
#########################################################################
## Project: Token Transfer Data on ICON Netowkr                        ##
## Date: Jan 2022                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Webscraping

# This file extracts information from ICON tracker website and Balanced statistics site.

# Extract
# Data put together in a DataFrame, saved separately in CSV format.

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
from datetime import date, datetime, timedelta
from time import time
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import matplotlib.pyplot as plt
from typing import Union

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "output")
if not os.path.exists(outPath):
    os.mkdir(outPath)

# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(2)
    if string:
        return yesterday.strftime('%Y_%m_%d')
    return yesterday

def day_to_text(day):
    return day.replace("_", "-")

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Date ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# today's date
date_is_range = 0 # if date is range (1) or is one  date (0)
use_specified_date = 0 # yes(1) no(0)

# date is range
if date_is_range == 1:
    day_1 = "2021_03_23"; day_2 = "2021_03_24"
    day_1_text = day_to_text(day_1); day_2_text = day_to_text(day_2)
    date_of_interest = pd.date_range(start=day_1_text, end=day_2_text, freq='D').strftime("%Y-%m-%d").to_list()

# specified date
if date_is_range == 0 and use_specified_date == 1:
   day_1 = "2022_03_25"
   day_prev = "2022_03_24"
   date_of_interest = [day_to_text(day_1)]

# today
if date_is_range == 0 and use_specified_date == 0:
   today = datetime.utcnow() - timedelta(1) #### IMPORTANT -- Note that this is 1 day before!! ###
   day_1 = today.strftime("%Y_%m_%d")
   date_of_interest = [day_to_text(day_1)]

# title based on range or not
if date_is_range == 1:
    title_date = date_of_interest[0] + ' ~ ' + date_of_interest[-1]
else:
    title_date = date_of_interest[0]

day_today = title_date.replace("-","_")
day_today_text = title_date.replace("-","/")

# to use specific date, otherwise use yesterday
if use_specified_date == 1:
    day_prev = day_prev
else:
    today = datetime.utcnow() - timedelta(1) ### IMPORTANT !! NOTE THAT THIS IS 1 DAY BEFORE !!!
    day_prev = yesterday(today)
day_prev_text = day_prev.replace("_","/")

windows_path = "E:/GitHub/ICONProject/data_analysis/10_token_transfer/results/" + day_today
windows_path_prev = "E:/GitHub/ICONProject/data_analysis/10_token_transfer/results/" + day_prev

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Some things from legacy code that it requires ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# wallet of interest
this_address = 'cx14002628a4599380f391f708843044bc93dce27d' # iAM

# from wallet or into the wallet
tx_flow = 'both' # 'in', 'out', 'both'
tx_type = 'token_txlist' # 'normal', 'internal', 'contract', 'token (individual wallet)', 'token_txlist (token that has been xferred), 'token_list'

# this is for getting only 1 interaction (WOI <-> wallet_x)
first_degree = 1

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Converter ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Extracting Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Contract Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# functions to get transactions and the page count needed for webscraping
def get_tx_via_url(tx_type=tx_type,
                   this_address='hx0b047c751658f7ce1b2595da34d57a0e7dad357d',
                   page_count=1,
                   tx_count=1):
    """ This function is used to extract json information from icon page (note that total number of
    tx from the website is 500k max, so we can't go back too much """

    # getting transaction information from icon transaction page
    if tx_type == 'normal':
        link_text = 'txList'
    elif tx_type == 'internal':
        link_text = 'internalTxList'

    # this is token transfer within a wallet
    elif tx_type == 'token':
        link_text = 'tokenTxList'

    if tx_type in ['normal','internal','token']:
        # tx url
        tx_url = Request('https://tracker.icon.foundation/v3/address/'+ link_text + '?page=' + str(page_count) + '&count=' + str(tx_count) + '&address=' + this_address,
                         headers={'User-Agent': 'Mozilla/5.0'})

    # contract information
    elif tx_type in ['contract']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/contract/list?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # list of tokens
    elif tx_type in ['token_list']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/token/list?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # all token transfer (different from main token tx which is on its own)
    elif tx_type in ['token_txlist']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/token/txList?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # extracting info into json
    jtx_url = json.load(urlopen(tx_url))

    return jtx_url

def get_page_tx_count(tx_type=tx_type, this_address=this_address):
    """ This function is to get the number of elements per page and number of page to be extracted """

    if tx_type in ['normal', 'internal']:
        jtx_url = get_tx_via_url(tx_type=tx_type, this_address=this_address, page_count=1, tx_count=1)
        totalSize = extract_values(jtx_url, 'totalSize')

    elif tx_type in ['contract','token_txlist']:
        jtx_url = get_tx_via_url(tx_type=tx_type, page_count=1, tx_count=1)
        totalSize = extract_values(jtx_url, 'listSize')

    # to get the tx count (loading how many elements in a page, and how many pages there are) for web scraping
    if totalSize[0] != 0:

        # get page count to loop over
        if totalSize[0] > 100:
            tx_count = 100
            page_count = round((totalSize[0] / tx_count) + 0.5)
        else:
            tx_count = totalSize[0]
            page_count = 1

    elif totalSize[0] == 0:
        page_count=0
        tx_count=0
        print("No records found!")

    return page_count, tx_count


# getting contract information from icon transaction page
page_count = []
tx_count = []
page_count, tx_count = get_page_tx_count(tx_type='contract')


# collecting contact info using multithreading
start = time()

known_contract_all = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(0, page_count):
        known_contract_all.append(executor.submit(get_tx_via_url, tx_type='contract', page_count=k+1, tx_count=tx_count))

temp_df = []
for task in as_completed(known_contract_all):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')

# extracting information by labels
contract_address = extract_values(temp_df, 'address')
contract_name = extract_values(temp_df, 'contractName')

# converting list into dictionary
def Convert(lst1, lst2):
    res_dct = {lst1[i]: lst2[i] for i in range(0, len(lst1), 1)}
    return res_dct

contract_d = Convert(contract_address, contract_name)



page_count = []
tx_count = []
page_count, tx_count = get_page_tx_count(tx_type='token_txlist')
#~~~~~~~~~~~~~~~~~~~~~~~~~~ Collecting data that meets date criteria Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# collects data that meets date criteria
def data_meeting_date_criteria(tx_type=tx_type, date_of_interest=date_of_interest, this_address=this_address,
                               page_count=page_count, tx_count=tx_count):
    """
    :param date_of_interest: date could be range or a specific date
    :return: appended data that meets the criteria -- requires loop
    """
    if tx_type in ['normal', 'internal']:
        jtx_url = get_tx_via_url(tx_type=tx_type, this_address=this_address, page_count=page_count, tx_count=tx_count)
        createDateTime = 'createDateTime'
        totalSize = 'totalSize'

    elif tx_type in ['token_txlist']:
        jtx_url = get_tx_via_url(tx_type=tx_type, page_count=page_count, tx_count=tx_count)
        createDateTime = 'age'
        totalSize = 'listSize'

    if jtx_url[totalSize] != 0:
        # changing datetime to date
        for d in jtx_url['data']:
            if tx_type in ['token_txlist']:
                d['createDateTime'] = d[createDateTime]
                d['createDate'] = d['createDateTime']
            elif tx_type in ['normal', 'internal']:
                d['createDateTime'] = d['createDate']

            date = d['createDate'].split('T', 1)[0]
            d['createDate'] = date

        first_createDate = jtx_url['data'][0]['createDate']
        # print(first_createDate)

        # collecting data that meets criteria -- given date/range and state == 1 (successful tx)
        my_item = []
        for item in jtx_url['data']:
            if item['createDate'] in date_of_interest and item['state'] == 1:
                my_item.append(item)
        return first_createDate, my_item

# collecting data
def collect_data(tx_type=tx_type, date_of_interest=date_of_interest, this_address=this_address):
    """ collects data and breaks loop if the date is before the date of interest (so it doesn't loop over all the data available) """

    page_count, tx_count = get_page_tx_count(tx_type=tx_type, this_address=this_address)
    if page_count != 0:
        tx_all = []
        for k in range(0, page_count):
            first_createDate, my_item = data_meeting_date_criteria(tx_type=tx_type,
                                                                    date_of_interest=date_of_interest,
                                                                    this_address=this_address,
                                                                    page_count=k+1,
                                                                    tx_count=tx_count)

            ## overall collection as it loops
            # if there's data then add to tx_all
            if len(my_item) != 0:
                tx_all.append(my_item)

            # if first date in the page is after the date of interest, then break of loop
            if date_of_interest[0] > first_createDate:
                break

        # in case there is no data then it'll skip all the processing
        if len(tx_all) != 0:

            # if normal of internal transactions
            if tx_type in ['normal', 'internal']:
                # extracting transaction information by labels
                tx_from_address = extract_values(tx_all, 'fromAddr')
                tx_to_address = extract_values(tx_all, 'toAddr')
                tx_date = extract_values(tx_all, 'createDate')
                tx_datetime = extract_values(tx_all, 'createDateTime')
                tx_amount = extract_values(tx_all, 'amount')
                # tx_fee = extract_values(tx_all, 'fee')
                tx_state = extract_values(tx_all, 'state')

                # combining lists
                combined = {'from_address': tx_from_address,
                            'dest_address': tx_to_address,
                            'date': tx_date,
                            'datetime': tx_datetime,
                            'amount': tx_amount,
                            # 'fee': tx_fee,
                            'state': tx_state}

            # if token tx
            if tx_type in ['token_txlist']:
                # extracting transaction information by labels
                tx_from_address = extract_values(tx_all, 'fromAddr')
                tx_to_address = extract_values(tx_all, 'toAddr')
                tx_date = extract_values(tx_all, 'createDate')
                tx_datetime = extract_values(tx_all, 'createDateTime')
                tx_amount = extract_values(tx_all, 'quantity')
                tx_fee = extract_values(tx_all, 'fee')
                symbol = extract_values(tx_all, 'symbol')
                token_name = extract_values(tx_all, 'tokenName')
                tx_state = extract_values(tx_all, 'state')

                # combining lists
                combined = {'from_address': tx_from_address,
                            'dest_address': tx_to_address,
                            'date': tx_date,
                            'datetime': tx_datetime,
                            'amount': tx_amount,
                            'fee': tx_fee,
                            'symbol': symbol,
                            'token_name': token_name,
                            'state': tx_state}

            # convert into dataframe
            combined_df = pd.DataFrame(data=combined)

            # removing failed transactions & drop 'state (state for transaction success)'
            combined_df = combined_df[combined_df.state != 0].drop(columns='state')

            return combined_df


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Token transfer Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting token_txlist information from icon transaction page
token_xfer_df = collect_data(tx_type='token_txlist', date_of_interest=date_of_interest)
token_xfer_df[['amount']] = token_xfer_df[['amount']].apply(pd.to_numeric, errors='coerce', axis=1)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Token price data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
def request_into_df(url):
    req_url = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    req_url_json = json.load(urlopen(req_url))
    req_df = pd.DataFrame(req_url_json)
    return(req_df)

# balanced
# token_price_balanced = request_into_df(url='https://balanced.geometry.io/api/v1/stats/token-stats').reset_index()
token_price_balanced = request_into_df(url='https://balanced.sudoblock.io/api/v1/stats/token-stats').reset_index()

# unifi protocol
token_price_unifi = request_into_df(url='https://assets.unifiprotocol.com/pools-icon.json')

# token price simple
token_price_balanced['Price in USD'] = token_price_balanced['tokens'].apply(lambda x: x.get('price'))
token_price_balanced['Price in USD'] = loop_to_icx(token_price_balanced['Price in USD'].apply(int, base=16))
token_price_balanced = token_price_balanced.drop(columns=['tokens', 'timestamp'])

bnusd_in_usd = token_price_balanced[token_price_balanced['index'] == 'bnUSD']['Price in USD'].iloc[0]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FIN price (if does not exist)
if not any(token_price_balanced['index'] == 'FIN'):
    from iconsdk.builder.call_builder import CallBuilder
    from iconsdk.icon_service import IconService
    from iconsdk.providers.http_provider import HTTPProvider

    DEX_CONTRACT = "cxa0af3165c08318e988cb30993b3048335b94af6c"
    nid = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
    EXA = 10 ** 18

    finbnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
        .to(DEX_CONTRACT) \
        .method("getPrice") \
        .params({"_id": "31"}) \
        .build()
    finbnusdpriceresult = nid.call(finbnusdpricecall)

    finbnusdindec = int(finbnusdpriceresult, 16)
    finbnusdfloatindec = float(finbnusdindec)
    finbnusdconverted = finbnusdfloatindec / EXA

    # convert fin_bnusd into fin_usd value
    fin_in_usd = pd.DataFrame({'index': ['FIN'], 'Price in USD': [finbnusdconverted * bnusd_in_usd]})

    # add FIN price to token price list
    token_price_balanced = token_price_balanced.append(fin_in_usd).reset_index(drop=True)
else:
    pass
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ICX price
ICX_price = token_price_balanced[token_price_balanced['index'] == 'ICX']['Price in USD']

token_price_unifi['Price in USD'] = token_price_unifi['price'].astype(float).apply(lambda x: x * ICX_price)
token_price_unifi = token_price_unifi[['name', 'Price in USD']].rename(columns={'name':'index'})
token_price_unifi = token_price_unifi[~token_price_unifi['index'].isin(token_price_balanced['index'])]

token_price_balanced = token_price_balanced[token_price_balanced['index'] != 'ICX']

token_price_all = token_price_balanced.append(token_price_unifi).reset_index(drop=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data output ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


table_now = token_xfer_df.copy()
table_now = table_now[['symbol','amount']].groupby(['symbol']).amount.agg(['sum', 'count']).reset_index()
table_now = table_now.sort_values(by='count', ascending=False).reset_index(drop=True)
table_now = table_now.rename(columns={'symbol': 'IRC Token', 'sum': 'Amount', 'count': 'No. of Transactions'})


from fuzzywuzzy import process
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    s = df_2[key2].tolist()
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
    df_1['matches'] = m
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    return df_1

# fuzzy matched (high threshold)
table_now = fuzzy_merge(table_now, token_price_all, 'IRC Token', 'index', threshold=60) #.drop(columns=['logo'])
table_now = table_now.rename(columns={'matches': 'index'})
table_now = pd.merge(table_now, token_price_all, on='index', how='left')
print(table_now)

table_now['Value Transferred in USD'] = table_now['Amount'] * table_now['Price in USD']
table_now = table_now.drop(columns='index')

ICX_price = pd.DataFrame({'IRC Token': 'ICX'}, ICX_price).reset_index()
table_now = table_now.append(ICX_price).reset_index(drop=True)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Save ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# saving this term's token transfer
table_now.to_csv(os.path.join(windows_path, 'IRC_token_transfer_' + day_today + '.csv'), index=False)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Save ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#






#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Outputting Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load previous data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# reading previous term data
table_prev = pd.read_csv(os.path.join(windows_path_prev, 'IRC_token_transfer_' + day_prev + '.csv'))
table_now = pd.read_csv(os.path.join(windows_path, 'IRC_token_transfer_' + day_today + '.csv'))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load previous data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

## removing ICX from the data (leaving ICX in the saved data for other purposes)
def remove_icx(df):
    return df[df['IRC Token'] != 'ICX']

table_now = remove_icx(table_now)
table_prev = remove_icx(table_prev)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data manipulation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
def add_total_tx(df):
    df.loc['Total'] = df.sum(numeric_only=True, axis=0)
    # df['Amount'] = np.where(df['Amount'] == df['Amount'].max(), '-', df['Amount'])
    df['IRC Token'] = np.where(df['IRC Token'].isna(), 'Total', df['IRC Token'])
    df = df.reset_index(drop=True)
    return df

table_now = add_total_tx(table_now)
table_prev = add_total_tx(table_prev)

# table_prev = table_now.copy()
# table_prev['Price in USD'] = table_prev['Price in USD'].apply(lambda x: x - 0.1)


def nan_to_zero(df, invar):
    df[invar] = np.where(df[invar].isna(), 0, df[invar])
    return df

def nan_to_zero_columns(df):
    df = nan_to_zero(df, 'Amount')
    df = nan_to_zero(df, 'No. of Transactions')
    df = nan_to_zero(df, 'Price in USD')
    df = nan_to_zero(df, 'Value Transferred in USD')
    return df

# reindexing last term's based on this term
def reindex_df(df_now, df_prev, my_index):
    df_prev = pd.merge(df_now, df_prev, how='left', on=my_index)
    df_prev = df_prev.rename(columns={'Amount_y': 'Amount', 'No. of Transactions_y': 'No. of Transactions', 'Price in USD_y': 'Price in USD',
                                      'Value Transferred in USD_y': 'Value Transferred in USD'}). \
        drop(columns=['Amount_x', 'No. of Transactions_x', 'Price in USD_x', 'Value Transferred in USD_x'])
    return df_prev

# function to add count difference between before and now
def add_val_differences(df_now, df_past, diff_var):
    if 'Price' in diff_var:
        df_now['diff_val'] = (df_now[diff_var] - df_past[diff_var]).astype(float)
        df_now[diff_var] = df_now[diff_var].astype(float).apply('{:.4f}'.format)
        df_now['diff_symbol'] = df_now['diff_val'].apply(lambda x: "+" if x>0 else '')
        df_now['diff_val'] = df_now['diff_val'].astype(float).apply('{:.4f}'.format)

    else:
        df_now['diff_val'] = (df_now[diff_var] - df_past[diff_var]).round().astype(int)
        df_now[diff_var] = df_now[diff_var].round().astype(int).apply('{:,}'.format)
        df_now['diff_symbol'] = df_now['diff_val'].apply(lambda x: "+" if x>0 else '')
        df_now['diff_val'] = df_now['diff_val'].apply('{:,}'.format)

    # df_now['diff_val'] = np.where(df_now['diff_val'] == 0, '=', df_now['diff_val'])
    df_now['diff_val'] = '(' + df_now['diff_symbol'] + df_now['diff_val'].astype(str) + ')'
    df_now[diff_var] = df_now[diff_var].astype(str) + ' ' + df_now['diff_val']
    df_now = df_now.drop(columns=['diff_val','diff_symbol'])
    return df_now

table_prev = reindex_df(table_now, table_prev, 'IRC Token')

table_prev = nan_to_zero_columns(table_prev)
table_now = nan_to_zero_columns(table_now)

table_now = add_val_differences(table_now, table_prev, 'Amount')
table_now = add_val_differences(table_now, table_prev, 'No. of Transactions')
table_now = add_val_differences(table_now, table_prev, 'Price in USD')
table_now = add_val_differences(table_now, table_prev, 'Value Transferred in USD')
table_now['Amount'].iloc[-1] = '-'
table_now['Price in USD'].iloc[-1] = '-'


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plot / Table ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

import six
my_title = "IRC Token Transfer Breakdown - " + day_today_text + " (Δ since " + day_prev_text + ")"

def render_mpl_table(data, col_width=3.0, row_height=0.325, font_size=10,
                     header_color='#40466e', row_colors=['black', 'black'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
        ax.set_title(my_title, fontsize=12,
                     weight='bold', pad=30)
        plt.tight_layout()

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors)])
    return ax

plt.style.use(['dark_background'])
render_mpl_table(table_now)

plt.savefig(os.path.join(windows_path, "IRC_token_transfer_" + day_today + "_vs_" + day_prev + ".png"))
