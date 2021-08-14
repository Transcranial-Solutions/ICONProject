#########################################################################
## Project: ICON Network Wallet Ranking                                ##
## Date: December 2020                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

import pandas as pd
from functools import reduce
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from iconsdk.builder.call_builder import CallBuilder
from typing import Union
from time import time
from datetime import date, datetime, timedelta
from urllib.request import Request, urlopen
import json
from tqdm import tqdm
import matplotlib.pyplot as plt

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# currPath = os.getcwd()
currPath = "/home/tonoplast/IconProject/"
projectPath = os.path.join(currPath, "wallet_ranking")
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

# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(1)
    if string:
        return yesterday.strftime('%Y_%m_%d')
    return yesterday

# today's date
# today = date.today()
today = datetime.utcnow()
day_today = today.strftime("%Y_%m_%d")
day_today_text = day_today.replace("_","/")

# to use specific date, otherwise use yesterday
use_specific_prev_date = 0
day_prev = "2021_02_08"

if use_specific_prev_date == 1:
    day_prev = day_prev
else:
    day_prev = yesterday(today)
day_prev_text = day_prev.replace("_","/")



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# load available wallets
# wallet_from_address = pd.read_csv(os.path.join(dataPath, 'wallet_from_address_20201203.csv'))\
#     .rename(columns={'from_address':'address'})
# wallet_to_address = pd.read_csv(os.path.join(dataPath, 'wallet_to_address_20201203.csv'))\
#     .rename(columns={'to_address':'address'})

# exchange wallets
exchange_wallets = ['hx1729b35b690d51e9944b2e94075acff986ea0675',
                    'hx99cc8eb746de5885f5e5992f084779fc0c2c135b',
                    'hx9f0c84a113881f0617172df6fc61a8278eb540f5',
                    'hxc4193cda4a75526bf50896ec242d6713bb6b02a3',
                    'hxa527f96d8b988a31083167f368105fc0f2ab1143',
                    'hx307c01535bfd1fb86b3b309925ae6970680eb30d',
                    'hxff1c8ebad1a3ce1ac192abe49013e75db49057f8',
                    'hx14ea4bca6f205fecf677ac158296b7f352609871',
                    'hx3881f2ba4e3265a11cf61dd68a571083c7c7e6a5',
                    'hxd9fb974459fe46eb9d5a7c438f17ae6e75c0f2d1',
                    'hx68646780e14ee9097085f7280ab137c3633b4b5f',
                    'hxbf90314546bbc3ed980454c9e2a9766160389302',
                    'hx562dc1e2c7897432c298115bc7fbcc3b9d5df294',
                    'hxb7f3d4bb2eb521f3c68f85bbc087d1e56a816fd6',
                    'hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310',
                    'hxfdb57e23c32f9273639d6dda45068d85ee43fe08',
                    'hx4a01996877ac535a63e0107c926fb60f1d33c532',
                    'hx8d28bc4d785d331eb4e577135701eb388e9a469d',
                    'hxf2b4e7eab4f14f49e5dce378c2a0389c379ac628',
                    'hx6eb81220f547087b82e5a3de175a5dc0d854a3cd',
                    'hx0cdf40498ef03e6a48329836c604aa4cea48c454',
                    'hx6d14b2b77a9e73c5d5804d43c7e3c3416648ae3d',
                    'hx85532472e789802a943bd34a8aeb86668bc23265',
                    'hx94a7cd360a40cbf39e92ac91195c2ee3c81940a6',
                    'hxe5327aade005b19cb18bc993513c5cfcacd159e9',
                    'hx23cb1d823ef96ac22ae30c986a78bdbf3da976df',
                    'hxa390d24fdcba68515b492ffc553192802706a121']
                    


exchange_names = ['binance_cold1',
                'binance_cold2',
                'binance_cold3',
                'binance_hot',
                'binance.us',
                'velic_hot',
                'velic_stave',
                'latoken',
                'coinex',
                'huobi',
                'kraken_hot',
                'upbit_hot_old',
                'upbit_cold',
                'crypto.com',
                'upbit_hot1',
                'upbit_hot2',
                'upbit_hot3',
                'upbit_hot4',
                'upbit_hot5',
                'bithumb_1',
                'bithumb_2',
                'bithumb_3',
                'unkEx_c1',
                'unkEx_c2',
                'unkEx_d1',
                'bitvavo_cold',
                'bitvavo_hot']

exchange_details = {'address': exchange_wallets,
                    'names': exchange_names}

# dataframe
exchange_details = pd.DataFrame(exchange_details)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## cleaning

# to series
wallet_address = exchange_details.address

# how many wallets
len_wallet_address = len(wallet_address)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
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

# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

def parse_icx(val: str) -> Union[float, int]:
    """
    Attempts to convert a string loop value into icx
        Parameters:
            val (str): Loop value
        Returns:
            (Union[float, int]): Will return the converted value as an int if successful, otherwise it will return NAN
    """
    try:
        return loop_to_icx(int(val, 0))
    except ZeroDivisionError:
        return float("NAN")
    except ValueError:
        return float("NAN")

def hex_to_int(val: str) -> Union[int, float]:
    """
    Attempts to convert a string based hex into int
        Parameters:
            val (str): Value in hex
        Returns:
            (Union[int, float]): Returns the value as an int if successful, otherwise a float NAN if not
    """
    try:
        return int(val, 0)
    except ValueError:
        print(f"failed to convert {val} to int")
        return float("NAN")

# get data function
def get_my_values(method, address, output, len_wallet_address):
    call = CallBuilder().from_(tester_address)\
                    .to('cx0000000000000000000000000000000000000000')\
                    .params({"address": address})\
                    .method(method)\
                    .build()
    result = icon_service.call(call)
    # try:
    #     temp_output = loop_to_icx(int(result[output], 0))
    # except:
    #     temp_output = float("NAN")   
    
    temp_output = parse_icx(result[output])
    
    df = {'address': address, output: temp_output}
    return(df)


# get unstakes function
def get_my_unstakes(method, address, output, len_wallet_address):
    call = CallBuilder().from_(tester_address)\
                    .to('cx0000000000000000000000000000000000000000')\
                    .params({"address": address})\
                    .method(method)\
                    .build()
    result = icon_service.call(call)
    try:
        df = result[output][0]
        df['address'] = address
    except:
        df = {'unstake': np.nan, 'unstakeBlockHeight': np.nan, 'remainingBlocks': np.nan, 'address': address}
        # df = {'unstake': float("NAN"), 'unstakeBlockHeight': float("NAN"), 'remainingBlocks': float("NAN"), 'address': address}
   
    try:
        df['unstake'] = parse_icx(df['unstake'])
        df['unstakeBlockHeight'] = hex_to_int(df['unstakeBlockHeight'])
        df['remainingBlocks'] = hex_to_int(df['remainingBlocks'])
    except Exception:
        pass

    return(df)


# get balance function
def get_balance(address, len_wallet_address):
    try:
        balance = loop_to_icx(icon_service.get_balance(address))
    except:
        balance = float("NAN")
    df = {'address': address, 'balance': balance}
    return(df)


from itertools import cycle, islice

max_workers_value = 50
end_range = 50
wallet_address_clean = wallet_address[:end_range]
len_wallet_address = len(wallet_address_clean)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# count = 0

all_iscore = []
all_staked = []
all_delegating = []
all_balance = []
all_unstakes = []

start = time()
with ThreadPoolExecutor(max_workers=max_workers_value) as executor:
    for i in tqdm(range(0, len_wallet_address), desc="workers"):
        all_iscore.append(executor.submit(get_my_values, "queryIScore", wallet_address_clean[i], "estimatedICX", len_wallet_address))
        all_staked.append(executor.submit(get_my_values, "getStake", wallet_address_clean[i], "stake", len_wallet_address))
        all_delegating.append(executor.submit(get_my_values, "getDelegation", wallet_address_clean[i], "totalDelegated", len_wallet_address))
        all_balance.append(executor.submit(get_balance, wallet_address_clean[i], len_wallet_address))
        all_unstakes.append(executor.submit(get_my_unstakes, "getStake", wallet_address_clean[i], "unstakes", len_wallet_address))         

print("workers complete")

temp_iscore = []
for task in tqdm(as_completed(all_iscore), desc="iscore\t\t", total=len_wallet_address):
    temp_iscore.append(task.result())

temp_staked = []
for task in tqdm(as_completed(all_staked), desc="staked\t\t", total=len_wallet_address):
    temp_staked.append(task.result())

temp_delegating = []
for task in tqdm(as_completed(all_delegating), desc="delegating\t", total=len_wallet_address):
    temp_delegating.append(task.result())

temp_balance = []
for task in tqdm(as_completed(all_balance), desc="balance\t", total=len_wallet_address):
    temp_balance.append(task.result())

temp_unstakes = []
for task in tqdm(as_completed(all_unstakes), desc="unstakes\t", total=len_wallet_address):
    temp_unstakes.append(task.result()) 


print("building dataframes...")

data_frames = []
if len(temp_iscore) > 0: 
    data_frames.append(pd.DataFrame(temp_iscore)) 
if len(temp_staked) > 0: 
    data_frames.append(pd.DataFrame(temp_staked))
if len(temp_delegating) > 0: 
    data_frames.append(pd.DataFrame(temp_delegating))
if len(temp_balance) > 0: 
    data_frames.append(pd.DataFrame(temp_balance))
if len(temp_unstakes) > 0: 
    data_frames.append(pd.DataFrame(temp_unstakes))

total_supply = round(loop_to_icx(icon_service.get_total_supply()))
total_supply_text = '{:,}'.format(total_supply) + ' ICX'
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# data_frames = [all_iscore, all_staked, all_delegating, all_balance, all_unstakes]
# nan_value = 0
# all_df = pd.concat(data_frames, join='outer', axis=1).fillna(nan_value)
all_df = reduce(lambda left, right: pd.merge(left, right, on=['address'], how='outer'), data_frames)

print(f'Time taken: {time() - start}')


def preprocess_df(inData):
    df = inData.drop_duplicates()
    df = df.groupby('address').sum().reset_index()

    # str to float and get total icx owned
    col_list = ['balance', 'stake', 'unstake']
    df['totalDelegated'] = df['totalDelegated'].astype(float)
    df[col_list] = df[col_list].astype(float)
    df['total'] = df[col_list].sum(axis=1)
    df['staking_but_not_delegating'] = df['stake'] - df['totalDelegated']

    # only use wallets with 'hx' prefix
    df = df[df['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

    return df

all_df = preprocess_df(all_df)
all_df = pd.merge(all_df, exchange_details, how='left', on='address')



windows_path = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/06_wallet_ranking/results/" + day_today

if not os.path.exists(windows_path):
    os.makedirs(windows_path)

# saving this term's exchange wallet balance
all_df.to_csv(os.path.join(windows_path, 'exchange_wallet_balance_' + day_today + '.csv'), index=False)

# reading previous term data
windows_path_prev = "/mnt/e/GitHub/Icon/ICONProject/data_analysis/06_wallet_ranking/results/" + day_prev
all_df_prev = pd.read_csv(os.path.join(windows_path_prev, "exchange_wallet_balance_" + day_prev + '.csv'))


# getting only exchange wallets
def get_exchange_amount(df):
    total_exchange = df[['address', 'names', 'balance', 'totalDelegated', 'total']]\
        .rename(columns={'balance':'Available ICX', 'totalDelegated': 'Delegated ICX', 'names':'Exchanges'})\
        .sort_values('total', ascending=False)\
        .reset_index(drop=True)
    return total_exchange

# getting total amount
def get_total(df):
    df['% Total'] = (df['total'] / df['total'].sum()).apply('{:.2%}'.format)
    df['% Total Supply'] = (df['total'] / total_supply).apply('{:.2%}'.format)
    df['     % Total     |     % Total Supply'] = df['% Total']  + '     |     ' + df['% Total Supply'] + '                          '
    df = df.drop(columns=['% Total',  '% Total Supply'])
    df.loc['Total']= df.sum(numeric_only=True, axis=0)
    if 'address' in df.columns:
        df['address'] = np.where(df['address'].isna(), 'Total', df['address'])
        df['Exchanges'] = np.where(df['Exchanges'].isna(), '-', df['Exchanges'])
    else:
        df['Exchanges'] = np.where(df['Exchanges'].isna(), 'Total', df['Exchanges']) 

    df['     % Total     |     % Total Supply'] = np.where(df['     % Total     |     % Total Supply'].isna(), '-', df['     % Total     |     % Total Supply'])
    df = df.reset_index(drop=True)
    return df

# function to add count difference between before and now
def add_val_differences(df_now, df_past, diff_var):
    df_now['diff_val'] = (df_now[diff_var] - df_past[diff_var]).round().astype(int)
    df_now[diff_var] = df_now[diff_var].round().astype(int).apply('{:,}'.format)
    df_now['diff_symbol'] = df_now['diff_val'].apply(lambda x: "+" if x>0 else '')
    df_now['diff_val'] = df_now['diff_val'].apply('{:,}'.format)
    df_now['diff_val'] = np.where(df_now['diff_val'] == 0, '=', df_now['diff_val'])
    df_now['diff_val'] = '(' + df_now['diff_symbol'] + df_now['diff_val'].astype(str) + ')'
    df_now[diff_var] = df_now[diff_var].astype(str) + ' ' + df_now['diff_val']
    df_now = df_now.drop(columns=['diff_val','diff_symbol'])
    return df_now

def exchanges_grouping(df, in_exchange, else_exchange):
    df['group'] = np.where(df['Exchanges'].str.contains(in_exchange), in_exchange, else_exchange)
    return df

def group_exchanges(df):
    df = exchanges_grouping(df, 'binance', df['Exchanges'])
    df = exchanges_grouping(df, 'bithumb', df['group'])
    df = exchanges_grouping(df, 'upbit', df['group'])
    df = exchanges_grouping(df, 'velic', df['group'])
    df = exchanges_grouping(df, 'bitvavo', df['group'])
    df = exchanges_grouping(df, 'unkEx_c', df['group'])
    df = exchanges_grouping(df, 'unkEx_d', df['group'])
    df = exchanges_grouping(df, 'kraken', df['group'])
    return df


# this term's wallet amount
total_exchange_now = get_exchange_amount(all_df)
total_exchange_now = group_exchanges(total_exchange_now)
total_exchange_now_grouped = total_exchange_now\
    .groupby('group')[['Available ICX','Delegated ICX','total']]\
    .agg(sum)\
    .reset_index()\
    .rename(columns={'group': 'Exchanges'})\
    .sort_values(by='total', ascending=False)\
    .reset_index(drop=True)


total_exchange_now = get_total(total_exchange_now)
total_exchange_now_grouped = get_total(total_exchange_now_grouped)

# last term's wallet amount
total_exchange_prev = get_exchange_amount(all_df_prev)
total_exchange_prev = group_exchanges(total_exchange_prev)

total_exchange_prev_grouped = total_exchange_prev\
    .groupby('group')[['Available ICX','Delegated ICX','total']]\
    .agg(sum)\
    .reset_index()\
    .rename(columns={'group': 'Exchanges'})\
    .sort_values(by='total', ascending=False)\
    .reset_index(drop=True)

total_exchange_prev = get_total(total_exchange_prev)
total_exchange_prev_grouped = get_total(total_exchange_prev_grouped)


# reindexing last term's based on this term
def reindex_exchanges(df_now, df_prev, keep_these, my_index):
    df_prev = pd.merge(df_now[keep_these], df_prev, how='left', on=my_index)
    if len(keep_these) > 1:
        df_prev = df_prev.rename(columns={'Exchanges_x':'Exchanges'}).drop(columns = 'Exchanges_y')
    df_prev['Available ICX'] = np.where(df_prev['Available ICX'].isna(), 0, df_prev['Available ICX'])
    df_prev['Delegated ICX'] = np.where(df_prev['Delegated ICX'].isna(), 0, df_prev['Delegated ICX'])
    df_prev['total'] = np.where(df_prev['total'].isna(), 0, df_prev['total'])

    df_prev = df_prev.set_index(my_index)
    df_prev = df_prev.reindex(index=df_now[my_index])
    df_prev = df_prev.reset_index()
    return df_prev

total_exchange_prev = reindex_exchanges(total_exchange_now, total_exchange_prev, ['address','Exchanges'], 'address')
total_exchange_prev_grouped = reindex_exchanges(total_exchange_now_grouped, total_exchange_prev_grouped, ['Exchanges'], 'Exchanges')

total_exchange_now = add_val_differences(total_exchange_now, total_exchange_prev, 'Available ICX')
total_exchange_now = add_val_differences(total_exchange_now, total_exchange_prev, 'Delegated ICX')
total_exchange_now = total_exchange_now.drop(columns='total')

total_exchange_now_grouped = add_val_differences(total_exchange_now_grouped, total_exchange_prev_grouped, 'Available ICX')
total_exchange_now_grouped = add_val_differences(total_exchange_now_grouped, total_exchange_prev_grouped, 'Delegated ICX')
total_exchange_now_grouped = add_val_differences(total_exchange_now_grouped, total_exchange_prev_grouped, 'total')
total_exchange_now_grouped = total_exchange_now_grouped.drop(columns='total')


# function to make table
import six

def render_mpl_table(data, col_width=4.0, row_height=0.425, font_size=12, title = 'my_title',
                    header_color='#40466e', row_colors=['black', 'black'], edge_color='w',
                    bbox=[0, 0, 1, 1], header_columns=0,
                    ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
        ax.set_title(title, fontsize=15,
                    weight='bold', pad=20)
        plt.tight_layout()

    mpl_table = ax.table(cellText=data.values, bbox=bbox, cellLoc='right', colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors)])


    return ax


plt.style.use(['dark_background'])

# make table figure
render_mpl_table(total_exchange_now.drop(columns='group'),
                header_color='tab:pink',
                header_columns=0,
                col_width=5,
                title="Major Exchange Wallets - " + 
                day_today_text + 
                " (Δ since " + day_prev_text + ")" + 
                "\n Total Supply: " + total_supply_text)

plt.savefig(os.path.join(windows_path, "exchange_wallets_" + day_today + "_vs_" + day_prev + ".png"))

# grouped
render_mpl_table(total_exchange_now_grouped,
                header_color='tab:pink',
                header_columns=0,
                col_width=5,
                title="Major Exchanges (grouped) - " + 
                day_today_text + 
                " (Δ since " + day_prev_text + ")" + 
                "\n Total Supply: " + total_supply_text)

plt.savefig(os.path.join(windows_path, "exchange_wallets_grouped_" + day_today + "_vs_" + day_prev + ".png"))