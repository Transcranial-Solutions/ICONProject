
#########################################################################
## Project: ICON Network Wallet Ranking                                ##
## Date: January 2021                                                  ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# We extract the wallet addresses from Insight's database and extract the data from the chain.
# The following codes load the extracted data and generates tables as needed.

import pandas as pd
import numpy as np
# from datetime import datetime
import os
from datetime import date
import matplotlib.pyplot as plt


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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Loading wallet info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

fname = 'wallet_balance_2021_01_22'
fdate = fname.rsplit('wallet_balance_', 1)[1]

# for output (tables)
outputPath = os.path.join(resultsPath, fdate)
if not os.path.exists(outputPath):
    os.mkdir(outputPath)

fdate = fdate.replace('_','-')

ori_df = pd.read_csv(os.path.join(resultsPath, fname + '.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# adding date/datetime info
df = ori_df.drop_duplicates()
df = df.groupby('address').sum().reset_index()

# str to float and get total icx owned
col_list = ['balance', 'stake', 'unstake']
df['totalDelegated'] = df['totalDelegated'].astype(float)
df[col_list] = df[col_list].astype(float)
df['total'] = df[col_list].sum(axis=1)
df['staking_but_not_delegating'] = df['stake'] - df['totalDelegated']

# add date
# df['created_at'] = df['created_at'].astype(int)
# df['date'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d")

# only use wallets with 'hx' prefix
df = df[df['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

# in case if there is any duplicates, we take the lastest value
# df = df.sort_values(by='created_at', ascending=False).groupby(['address']).first().reset_index()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# binning the balance
# df_binned = df.copy()

# bins = [-1, 0, 1, 1000, 5000, 10000, 25000, 50000, 100000,
#         250000, 500000, 1000000, 5000000, 10000000, 25000000,
#         50000000, 100000000, 9999999999999]
#
# names = ["0", "1 or less", "1 - 1K", "1K - 5K", "5K - 10K", "10K - 25K", "25K - 50K", "50K - 100K",
#        "100K - 250K", "250K - 500K", "500K - 1M", "1M - 5M", "5M - 10M", "10M - 25M",
#          "25M - 50M", "50M - 100M", "100M +"]

bins = [-1, 0, 1, 1000, 5000, 10000, 15000, 20000,
        25000, 30000, 35000, 40000, 45000, 50000, 60000, 75000,
        100000, 150000, 250000, 500000,
        1000000, 5000000, 10000000, 25000000,
        50000000, 100000000, 9999999999999]

names = ["0", "1 or less", "1 - 1K", "1K - 5K", "5K - 10K", "10K - 15K", "15K - 20K",
         "20K - 25K", "25K - 30K", "30K - 35K", "35K - 40K", "40K - 45K", "45K - 50K", "50K - 60K", "60K - 75K",
         "75K - 100K", "100K - 150K", "150K - 250K", "250K - 500K",
         "500K - 1M", "1M - 5M", "5M - 10M", "10M - 25M",
         "25M - 50M", "50M - 100M", "100M +"]


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
    binned_df['Cumulative Percentage (>1 ICX)'] = binned_df[one_or_less_index]['Cumulative Percentage (>1 ICX)'].map("{:.3%}".format)

    one_or_less_index = ~binned_df['Amount (ICX)'].isin(["0", "1 or less"])
    binned_df['Percentage (>1 ICX)'] = binned_df[one_or_less_index]['Percentage (>1 ICX)'].map("{:.3%}".format)
    binned_df['Count'] = binned_df['Count'].astype(int)
    binned_df = binned_df.fillna('-')

    return(binned_df)

# # total
# get_binned_df(df, 'total')
#
# # balance
# get_binned_df(df, 'stake')
#
# # delegating
# get_binned_df(df, 'totalDelegated')
#
# # unstaking
# get_binned_df(df, 'unstake')
#
# # balance
# get_binned_df(df, 'balance')
#
# # staking but not delegating
# get_binned_df(df, 'staking_but_not_delegating')
#
# # i_score that will be collected
# get_binned_df(df, 'estimatedICX')


# total_icx = df[['stake','unstake','balance']].sum(numeric_only=True, axis=0).reset_index().sum()

df[df['total'].max() == df['total']]


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

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

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

# render_mpl_table(get_binned_df(df, 'estimatedICX'), header_columns=0, col_width=4, title="i-score unclaimed")

# # today date
# today = date.today()
# day1 = today.strftime("%Y_%m_%d")

# total
render_mpl_table(get_binned_df(df, 'total'),
                 header_color='green',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Owned - " + fdate + "\n (Staking + Unstaking + Balance)")

plt.savefig(os.path.join(outputPath, "total_balance_" + fdate + ".png"))

# staked
render_mpl_table(get_binned_df(df, 'stake'),
                 header_color='tab:purple',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Staked - " + fdate)

plt.savefig(os.path.join(outputPath, "staking_balance_" + fdate + ".png"))

# unstaking
render_mpl_table(get_binned_df(df, 'unstake'),
                 header_color='firebrick',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Unstaking - " + fdate)

plt.savefig(os.path.join(outputPath, "unstaking_balance_" + fdate + ".png"))

# i-score
render_mpl_table(get_binned_df(df, 'estimatedICX'),
                 header_color='darkorange',
                 header_columns=0,
                 col_width=3.5,
                 title="Total I-Score Unclaimed - " + fdate)

plt.savefig(os.path.join(outputPath, "iscore_balance_" + fdate + ".png"))


# exchange wallets
exchange_wallets = ['hx1729b35b690d51e9944b2e94075acff986ea0675',
                    'hx99cc8eb746de5885f5e5992f084779fc0c2c135b',
                    'hx9f0c84a113881f0617172df6fc61a8278eb540f5',
                    'hxc4193cda4a75526bf50896ec242d6713bb6b02a3',
                    'hx307c01535bfd1fb86b3b309925ae6970680eb30d',
                    'hxff1c8ebad1a3ce1ac192abe49013e75db49057f8',
                    'hx14ea4bca6f205fecf677ac158296b7f352609871',
                    'hx3881f2ba4e3265a11cf61dd68a571083c7c7e6a5',
                    'hxd9fb974459fe46eb9d5a7c438f17ae6e75c0f2d1',
                    'hx68646780e14ee9097085f7280ab137c3633b4b5f',
                    'hxbf90314546bbc3ed980454c9e2a9766160389302',
                    'hx562dc1e2c7897432c298115bc7fbcc3b9d5df294']

exchange_names = ['binance_cold1',
                  'binance_cold2',
                  'binance_cold3',
                  'binance_hot',
                  'velic_hot',
                  'velic_stave',
                  'latoken',
                  'coinex',
                  'huobi',
                  'kraken_hot',
                  'upbit',
                  'upbit_cold']

exchange_details = {'address': exchange_wallets,
                    'names': exchange_names}

# dataframe
exchange_details = pd.DataFrame(exchange_details)

# getting only exchange wallets
df_exchange = df[df['address'].isin(exchange_wallets)]
df_exchange = pd.merge(df_exchange,
                       exchange_details,
                       how='left',
                       on='address')

total_exchange = df_exchange[['address', 'names', 'total']]\
    .rename(columns={'total':'Amount (ICX)', 'names':'Exchanges'})\
    .sort_values('Amount (ICX)', ascending=False)

total_exchange['Amount (ICX)'] = total_exchange['Amount (ICX)'].astype(int).apply('{:,}'.format)

render_mpl_table(total_exchange,
                 header_color='tab:pink',
                 header_columns=0,
                 col_width=5,
                 title="Major Exchange Wallets - " + fdate)

plt.savefig(os.path.join(outputPath, "exchange_wallets_" + fdate + ".png"))