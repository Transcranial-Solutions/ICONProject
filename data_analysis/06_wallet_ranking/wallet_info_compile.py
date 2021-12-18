#########################################################################
## Project: Data compiler                                              ##
## Date: December 2021                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# webscraping icon.community and save into csv

import pandas as pd
import numpy as np
import os
import glob
import re
from urllib.request import Request, urlopen
import json
from datetime import date, datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns


# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "06_wallet_ranking")
if not os.path.exists(outPath):
    os.mkdir(outPath)
dataPath = os.path.join(outPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)
# walletsPath = os.path.join(resultsPath, "wallet_balance")
# if not os.path.exists(walletsPath):
#     os.mkdir(walletsPath)


# getting 2021 05 data only
listData = glob.glob((dataPath + "\\address_*.csv"), recursive=True)


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

all_df =[]
for k in range(len(listData)):
    this_date = re.findall(r'exchange_wallet_balance_(.+?).csv', listData[k])[0].replace("_", "/")
    df = pd.read_csv(listData[k]) #.head(1)
    df['date'] = this_date
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    all_df.append(df)

df = pd.concat(all_df)

# re-attaching names as they have changed over time
df = df.drop(columns='names')
df = pd.merge(df, exchange_details, how='left', on='address')


df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
# df.index = pd.DatetimeIndex(df['date']).floor('D')
# all_days = pd.date_range(df.index.min(), df.index.max(), freq='D')
# df = df.reindex(all_days)
# df.reset_index(inplace=True)
# df = df.drop(columns='date').rename(columns={'index':'date'})
# df['date'] = df['date'].dt.date
# df.set_index('date', inplace=True)
#
# # this is stupid..
# df['Market Cap (USD)'] = df['Market Cap (USD)'].str.replace(',','').astype('float')
# df['Circulating Supply'] = df['Circulating Supply'].str.replace(',','').astype('float')
# df['Total Supply'] = df['Total Supply'].str.replace(',','').astype('float')
# df['Public Treasury \xa0'] = df['Public Treasury \xa0'].str.replace(',','').astype('float')
# df['total_staked_ICX'] = df['total_staked_ICX'].str.replace(',','').astype('float')
#
# df['Total Staked \xa0'] = df['Total Staked \xa0'].str.rstrip('%').astype('float') / 100.0
# df['Circulation Staked \xa0'] = df['Circulation Staked \xa0'].str.rstrip('%').astype('float') / 100.0
# df['Total Voted \xa0'] = df['Total Voted \xa0'].str.rstrip('%').astype('float') / 100.0
#
# df = df.interpolate()
#
# df['Market Cap (USD)'] = df['Market Cap (USD)'].astype('int')
# df['Circulating Supply'] = df['Circulating Supply'].astype('int')
# df['Total Supply'] = df['Total Supply'].astype('int')
# df['Public Treasury \xa0'] = df['Public Treasury \xa0'].astype('int')
# df['total_staked_ICX'] = df['total_staked_ICX'].astype('int')

# df.to_csv(os.path.join(outDataPath, 'icx_stat_compiled.csv'), index=False)

def exchanges_grouping(df, in_exchange, else_exchange):
    df['group'] = np.where(df['names'].str.contains(in_exchange), in_exchange, else_exchange)
    return df

def group_exchanges(df):
    df = exchanges_grouping(df, 'binance', df['names'])
    df = exchanges_grouping(df, 'bithumb', df['group'])
    df = exchanges_grouping(df, 'upbit', df['group'])
    df = exchanges_grouping(df, 'velic', df['group'])
    df = exchanges_grouping(df, 'bitvavo', df['group'])
    df = exchanges_grouping(df, 'unkEx_c', df['group'])
    df = exchanges_grouping(df, 'unkEx_d', df['group'])
    df = exchanges_grouping(df, 'kraken', df['group'])
    return df

df_grouped = group_exchanges(df)

df_grouped = df_grouped\
    .groupby(['date','group'])[['balance','totalDelegated','total']]\
    .agg(sum)\
    .reset_index()\
    .rename(columns={'group': 'Exchanges'})\
    .sort_values(by='total', ascending=False)\
    .reset_index(drop=True)


# from scipy.ndimage.filters import gaussian_filter1d
# Final_array_smooth = gaussian_filter1d(total_df_grouped['balance'], sigma=2)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Coin Market Cap Data Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

which_coin = '2099' # ICON/$ICX
which_currency = 'USD' # USD, BTC, ETH
today = datetime.now()

def get_utc_timestampe(YYYY, MM, DD):
    dt = datetime(YYYY, MM, DD, 0, 0, 0, 0)
    return(pd.Timestamp(dt, tz='utc').timestamp())

ts1 = int(get_utc_timestampe(2017, 10, 27))
# ts2 = int(get_utc_timestampe(2021, 4, 1))
ts2 = int(pd.Timestamp(today, tz='utc').timestamp())

this_url = Request('https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?id=' +
                   which_coin +
                   '&convert=' + which_currency +
                   '&time_start=' + str(ts1) +
                   '&time_end=' + str(ts2),
                   headers={'User-Agent': 'Mozilla/5.0'})

# json format
jthis_url = json.load(urlopen(this_url))

# extracting icx price information by labels & combining strings into list
d = {'time_open': extract_values(jthis_url, 'time_open'),
     'time_close': extract_values(jthis_url, 'time_close'),
     'time_high': extract_values(jthis_url, 'time_high'),
     'time_low': extract_values(jthis_url, 'time_low'),
     'open_price': extract_values(jthis_url, 'open'),
     'close_price': extract_values(jthis_url, 'close'),
     'high_price': extract_values(jthis_url, 'high'),
     'low_price': extract_values(jthis_url, 'low'),
     'volume': extract_values(jthis_url, 'volume'),
     'market_cap': extract_values(jthis_url, 'market_cap'),
     'date': extract_values(jthis_url, 'timestamp')}

# removing first element -- timestamp of data extraction
d['date'].pop(0)

# convert into dataframe
df_price = pd.DataFrame(data=d)

def get_date_only(x):
    return(x.str.split('T').str[0])

# cleaning up dates
# time_cols = ['time_open', 'time_close', 'time_high', 'time_low', 'timestamp']
time_cols = ['date']
df_price = df_price.apply(lambda x: get_date_only(x) if x.name in time_cols else x)
df_price['return_price'] = df_price['close_price']/df_price['close_price'].shift()-1
df_price['date'] = pd.to_datetime(df_price['date'], format='%Y/%m/%d')



df_price_short = df_price[['date','close_price']]

df_grouped = pd.merge(df_grouped, df_price_short, how='left', on='date')

df_price_short = df_grouped.drop_duplicates('date')[['date','close_price']].sort_values(by='date').reset_index(drop=True)
df_grouped = df_grouped.drop(columns='close_price')
df_grouped_total = df_grouped.groupby(['date']).agg('sum').reset_index()

df_grouped_total_corr = pd.merge(df_grouped_total, df_price_short, how='left', on='date')
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#












from matplotlib.ticker import StrMethodFormatter
import math


sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='date', y='balance', hue='Exchanges', data=df_grouped) #, palette=sns.color_palette('husl'))
# sns.lineplot(x='date', y='balance', data=df_grouped_total, alpha=0.5, linewidth=10) #, palette=sns.color_palette('husl'))

h,l = ax.get_legend_handles_labels()
ax.legend(reversed(h), reversed(l),
           loc='center', bbox_to_anchor=(0.5, -0.18),
           fancybox=True, shadow=True, ncol=5)
# plt.tight_layout(rect=[0,0,1,1])

ax.set_xlabel('Date', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Amount (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Exchange ICX Balance', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/10e5) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
ax2 = ax.twinx()
sns.lineplot(x='date', y='close_price', ax=ax2, data=df_price_short, color='teal', alpha=0.5, linewidth=8)
ymin2, ymax2 = ax2.get_ylim()
ax2.set_ylim([0,math.ceil(ymax2)])
ax2.yaxis.set_major_formatter(StrMethodFormatter('{x:,.2f}' + ' ICX'))
ax2.set_ylabel('Close Price (USD)', fontsize=14, weight='bold', labelpad=10)

plt.tight_layout()
# ax.set_yscale('log')

# sns.despine(offset=10, trim=False)



sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='date', y='totalDelegated', hue='Exchanges', data=df_grouped) #, palette=sns.color_palette('husl'))
# sns.lineplot(x='date', y='balance', data=df_grouped_total, alpha=0.5, linewidth=10) #, palette=sns.color_palette('husl'))

h,l = ax.get_legend_handles_labels()
ax.legend(reversed(h), reversed(l),
           loc='center', bbox_to_anchor=(0.5, -0.18),
           fancybox=True, shadow=True, ncol=5)
# plt.tight_layout(rect=[0,0,1,1])

ax.set_xlabel('Date', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Amount (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Exchange ICX Staking', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/10e5) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
ax2 = ax.twinx()
sns.lineplot(x='date', y='close_price', ax=ax2, data=df_price_short, color='teal', alpha=0.5, linewidth=8)
ymin2, ymax2 = ax2.get_ylim()
ax2.set_ylim([0,math.ceil(ymax2)])
ax2.yaxis.set_major_formatter(StrMethodFormatter('{x:,.2f}' + ' ICX'))
ax2.set_ylabel('Close Price (USD)', fontsize=14, weight='bold', labelpad=10)

plt.tight_layout()


import scipy.stats as sp
from scipy import stats

def corr_plot(df, inVar1, inVar2, this_xlabel, this_ylabel, this_title):

    # m_var1 = df.groupby('validator_name')[[inVar1]].apply(np.median).reset_index(name='var1')
    # m_var2 = df.groupby('validator_name')[[inVar2]].apply(np.median).reset_index(name='var2')
    # m_days_ranking = pd.merge(m_var1, m_var2, on='validator_name', how='left')

    # plot
    sns.set(style="ticks")
    plt.style.use("dark_background")
    g = sns.jointplot(inVar1, inVar2, data=df,
                      kind="reg", height=6, truncate=False, color='m',
                      joint_kws={'scatter_kws': dict(alpha=0.5)}
                      ).annotate(sp.pearsonr)
    g.set_axis_labels(this_xlabel, this_ylabel, fontsize=14, weight='bold', labelpad=10)
    g = g.ax_marg_x
    g.set_title(this_title, fontsize=12, weight='bold')
    plt.tight_layout()


corr_plot(df_grouped_total_corr,
          'total',
          'close_price',
          'Exchange Total Balance',
          'ICX Price' + ' (' + which_currency + ')',
          'Exchange Balance vs ICX Price')