
#########################################################################
## Project: Token Transfer Data on ICON Network                        ##
## Date: June 2022                                                     ##
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
import requests
import random

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = '/home/tono/ICONProject/data_analysis/10_token_transfer'

resultPath = os.path.join(currPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

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

windows_path = os.path.join(resultPath, day_today)
windows_path_prev = os.path.join(resultPath, day_prev)

# windows_path = "E:/GitHub/ICONProject/data_analysis/10_token_transfer/results/" + day_today
# windows_path_prev = "E:/GitHub/ICONProject/data_analysis/10_token_transfer/results/" + day_prev

if not os.path.exists(windows_path):
    os.makedirs(windows_path)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Converter ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

def hex_to_icx(x):
    return int(x, base=16)/1000000000000000000

# convert timestamp to datetime
def timestamp_to_date(df, timestamp, dateformat):
    return pd.to_datetime(df[timestamp] / 1000000, unit='s').dt.strftime(dateformat)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Extracting Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# two different methods in case one fails (I think last 2 are the same, but different link)
def token_tx_using_community_tracker(total_pages=500):
    # functions to get transactions and the page count needed for webscraping
    def get_tx_via_icon_community_tracker(skip_pages):
        """ This function is used to extract json information from icon community site """
        req = requests.get('https://tracker.icon.community/api/v1/transactions/token-transfers?limit=' + str(100) + '&skip=' + str(skip_pages),
                             headers={'User-Agent': 'Mozilla/5.0'})
        jtracker = json.loads(req.text)
        jtracker_df = pd.DataFrame(jtracker)
        jtracker_df['datetime'] = timestamp_to_date(jtracker_df, 'block_timestamp', '%Y-%m-%d')
        return jtracker_df

    skip = 100
    total_pages = total_pages
    last_page = total_pages * skip - skip
    page_count = range(0, last_page, skip)

    tx_all = []
    for k in tqdm(page_count):
        try:
            temp_df = get_tx_via_icon_community_tracker(k)
            if date_of_interest[0] > temp_df['datetime'].iloc[0]:
                break
                print('Done collecting...')
            else:
                tx_all.append(temp_df)

        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    token_xfer_df = pd.concat(tx_all)
    token_xfer_df['transaction_fee'] = token_xfer_df['transaction_fee'].apply(hex_to_icx)
    rename_these = {'to_address': 'dest_address',
                    'value_decimal': 'amount',
                    'transaction_fee': 'fee',
                    'token_contract_symbol': 'symbol',
                    'token_contract_name': 'token_name'}

    token_xfer_df.columns = [rename_these.get(i, i) for i in token_xfer_df.columns]
    # tx_all = tx_all.rename(columns = rename_these)
    return token_xfer_df

def token_tx_using_newer_icon_tracker(total_pages=500):

    def get_tx_via_newer_icon_tracker(page_no):
        req = requests.get('https://main.tracker.solidwallet.io/v3/token/txList?page=' + str(page_no) + '&count=' + str(100),
                         headers={'User-Agent': 'Mozilla/5.0'})
        jtracker = json.loads(req.text)['data']
        jtracker_df = pd.DataFrame(jtracker)
        jtracker_df['datetime'] = jtracker_df['age'].str.split('T').str[0]
        return jtracker_df

    total_pages = total_pages
    page_count = range(1, total_pages)

    tx_all = []
    for k in tqdm(page_count):
        try:
            temp_df = get_tx_via_newer_icon_tracker(k)
            if date_of_interest[0] > temp_df['datetime'].iloc[0]:
                break
                print('Done collecting...')
            else:
                tx_all.append(temp_df)
        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    token_xfer_df = pd.concat(tx_all)
    rename_these = {'fromAddr': 'from_address',
                    'toAddr': 'dest_address',
                    'quantity': 'amount',
                    'tokenName': 'token_name'}

    token_xfer_df.columns = [rename_these.get(i, i) for i in token_xfer_df.columns]
    return token_xfer_df

def token_tx_using_original_icon_tracker(total_pages=500):

    def get_tx_via_newer_icon_tracker(page_no):
        req = Request('https://tracker.icon.foundation/v3/token/txList?page=' + str(page_no) + '&count=' + str(100),
                      headers={'User-Agent': 'Mozilla/5.0'})
        jtracker = json.loads(req.text)['data']
        jtracker_df = pd.DataFrame(jtracker)
        jtracker_df['datetime'] = jtracker_df['age'].str.split('T').str[0]
        return jtracker_df

    total_pages = total_pages
    page_count = range(1, total_pages)

    tx_all = []
    for k in tqdm(page_count):
        try:
            temp_df = get_tx_via_newer_icon_tracker(k)
            if date_of_interest[0] > temp_df['datetime'].iloc[0]:
                break
                print('Done collecting...')
            else:
                tx_all.append(temp_df)
        except:
            random_sleep_except = random.uniform(200,300)
            print("I've encountered an error! I'll pause for"+str(random_sleep_except/60) + " minutes and try again \n")
            time.sleep(random_sleep_except) #sleep the script for x seconds and....#
            continue

    token_xfer_df = pd.concat(tx_all)
    rename_these = {'fromAddr': 'from_address',
                    'toAddr': 'dest_address',
                    'quantity': 'amount',
                    'tokenName': 'token_name'}

    token_xfer_df.columns = [rename_these.get(i, i) for i in token_xfer_df.columns]
    return token_xfer_df


def try_multiple_times_if_fails():
    ## just adding multiple try catch so that hopefully it'll go through
    try:
        df = token_tx_using_community_tracker(total_pages=500)
    except:
        try:
            df = token_tx_using_newer_icon_tracker(total_pages=500)
        except:
            df = token_tx_using_original_icon_tracker(total_pages=500)
    finally:
        return df

token_xfer_df = try_multiple_times_if_fails()

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


def get_token_price(token_price_balanced, bnusd_in_usd, token_name, token_contract_address):
    if not any(token_price_balanced['index'] == token_name):
        from iconsdk.builder.call_builder import CallBuilder
        from iconsdk.icon_service import IconService
        from iconsdk.providers.http_provider import HTTPProvider

        DEX_CONTRACT = "cxa0af3165c08318e988cb30993b3048335b94af6c"
        BNUSD_CONTRACT = "cx88fd7df7ddff82f7cc735c871dc519838cb235bb"
        nid = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
        EXA = 10 ** 18

        token_idcall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
            .to(DEX_CONTRACT) \
            .method("getPoolId") \
            .params({"_token1Address": token_contract_address, "_token2Address": BNUSD_CONTRACT}) \
            .build()
        token_id = nid.call(token_idcall)
        token_idint = int(token_id, 16)

        token_bnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
            .to(DEX_CONTRACT) \
            .method("getPrice") \
            .params({"_id": token_idint}) \
            .build()

        token_bnusdpriceresult = nid.call(token_bnusdpricecall)

        token_bnusdindec = int(token_bnusdpriceresult, 16)
        token_bnusdfloatindec = float(token_bnusdindec)
        token_bnusdconverted = token_bnusdfloatindec / EXA

        # convert fin_bnusd into fin_usd value
        token_in_usd = pd.DataFrame({'index': [token_name], 'Price in USD': [token_bnusdconverted * bnusd_in_usd]})

        # add token price to the existing list
        token_price_balanced = token_price_balanced.append(token_in_usd).reset_index(drop=True)
    else:
        pass
    return token_price_balanced



## add token price if does not exist
token_price_balanced = get_token_price(token_price_balanced, bnusd_in_usd, 'FRAMD', "cx2aa9b28a657e3121b75d3d4fe65e569398645d56")
token_price_balanced = get_token_price(token_price_balanced, bnusd_in_usd, 'FIN', "cx785d504f44b5d2c8dac04c5a1ecd75f18ee57d16")
token_price_balanced = get_token_price(token_price_balanced, bnusd_in_usd, 'iAM', "cxe7c05b43b3832c04735e7f109409ebcb9c19e664")
#
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # FIN price (if does not exist)
# if not any(token_price_balanced['index'] == 'FIN'):
#     from iconsdk.builder.call_builder import CallBuilder
#     from iconsdk.icon_service import IconService
#     from iconsdk.providers.http_provider import HTTPProvider
#
#     DEX_CONTRACT = "cxa0af3165c08318e988cb30993b3048335b94af6c"
#     nid = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))
#     EXA = 10 ** 18
#
#     finbnusdpricecall = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
#         .to(DEX_CONTRACT) \
#         .method("getPrice") \
#         .params({"_id": "31"}) \
#         .build()
#     finbnusdpriceresult = nid.call(finbnusdpricecall)
#
#     finbnusdindec = int(finbnusdpriceresult, 16)
#     finbnusdfloatindec = float(finbnusdindec)
#     finbnusdconverted = finbnusdfloatindec / EXA
#
#     # convert fin_bnusd into fin_usd value
#     fin_in_usd = pd.DataFrame({'index': ['FIN'], 'Price in USD': [finbnusdconverted * bnusd_in_usd]})
#
#     # add FIN price to token price list
#     token_price_balanced = token_price_balanced.append(fin_in_usd).reset_index(drop=True)
# else:
#     pass
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ICX price
ICX_price = token_price_balanced[token_price_balanced['index'] == 'ICX']['Price in USD']

token_price_unifi['Price in USD'] = token_price_unifi['price'].astype(float).apply(lambda x: x * ICX_price)
token_price_unifi = token_price_unifi[['name', 'Price in USD']].rename(columns={'name':'index'})
token_price_unifi = token_price_unifi[~token_price_unifi['index'].isin(token_price_balanced['index'])]

token_price_balanced = token_price_balanced[token_price_balanced['index'] != 'ICX']

token_price_all = token_price_balanced.append(token_price_unifi).reset_index(drop=True)


token_xfer_df['token_name'] = np.where(token_xfer_df['token_name'] == '', 
                                       'unknown_token', 
                                       token_xfer_df['token_name'])

token_xfer_df['symbol'] = np.where(token_xfer_df['symbol'] == '', 
                                       '$unknown', 
                                       token_xfer_df['symbol'])
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data output ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


table_now = token_xfer_df.copy()
table_now = table_now[['symbol','amount']].groupby(['symbol']).amount.agg(['sum', 'count']).reset_index()
table_now = table_now.sort_values(by='count', ascending=False).reset_index(drop=True)
table_now = table_now.rename(columns={'symbol': 'IRC Token', 'sum': 'Amount', 'count': 'No. of Transactions'})

from fuzzywuzzy import process
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    s = df_2[key2].tolist()
    temp_m = df_1[[key1]].reset_index(drop=True)
    temp_m[key1] = np.where(temp_m[key1].str.startswith('fin') & (temp_m[key1].str.endswith('ICX') | temp_m[key1].str.endswith('OMM')),
                                  temp_m[key1].str.split('fin').str[-1],
                                  temp_m[key1])
    m = temp_m[key1].apply(lambda x: process.extract(x, s, limit=limit))
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
