
#########################################################################
## Project: ICON Network Wallet Ranking                                ##
## Date: August 2021                                                   ##
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
if not "06_wallet_ranking" in currPath:
    projectPath = os.path.join(currPath, "06_wallet_ranking")
    if not os.path.exists(projectPath):
        os.mkdir(projectPath)
else:
    projectPath = currPath

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Loading wallet info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

fname_now = 'wallet_balance_2021-08-30'
fname_past = 'wallet_balance_2021-08-29' # for comparison

fdate = fname_now.rsplit('wallet_balance_', 1)[1]
fdate_past = fname_past.rsplit('wallet_balance_', 1)[1]


# for output (tables)
outputPath = os.path.join(resultPath, fdate)
if not os.path.exists(outputPath):
    os.mkdir(outputPath)

# fdate = fdate.replace('_','-')
# fdate_past = fdate_past.replace('_','-')

ori_df_now = pd.read_csv(os.path.join(dataPath, fname_now + '.csv'))
ori_df_past = pd.read_csv(os.path.join(dataPath, fname_past + '.csv'))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# adding date/datetime info
def preprocess_df(inData):
    df = inData.drop_duplicates()
    df = df.groupby('address').sum().reset_index()

    # str to float and get total icx owned
    col_list = ['balance', 'stake', 'unstake', 'balancedCollateral']
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

    return df

df_now = preprocess_df(ori_df_now)
df_past = preprocess_df(ori_df_past)
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
# df_now[df_now['total'].max() == df_now['total']]

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

# function to add count difference between before and now
def add_count_differences(df_now, df_past, invar, diff_var):
    df_now = get_binned_df(df_now, invar)
    df_past = get_binned_df(df_past, invar)
    df_now['diff_val'] = df_now[diff_var] - df_past[diff_var]
    df_now['diff_symbol'] = df_now['diff_val'].apply(lambda x: "+" if x>0 else '')
    df_now['diff_val'] = np.where(df_now['diff_val'] == 0, '=', df_now['diff_val'])
    df_now['diff_val'] = '(' + df_now['diff_symbol'] + df_now['diff_val'].astype(str) + ')'
    df_now[diff_var] = df_now[diff_var].astype(str) + ' ' + df_now['diff_val']
    df_now = df_now.drop(columns=['diff_val','diff_symbol'])
    return df_now

# total
total_text = '{:,}'.format(df_now['total'].sum().round(0).astype(int)) + ' ICX'

df = add_count_differences(df_now, df_past, invar='total', diff_var='Count')

render_mpl_table(df,
                 header_color='green',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Owned - " + fdate + " (Δ since " + fdate_past + ")" + "\n (Staking + Unstaking + Balance + Balanced Collateral)")

plt.savefig(os.path.join(outputPath, "total_balance_" + fdate + ".png"))



# staked
total_staked_but_not_delegated_text = '{:,}'.format(df_now['staking_but_not_delegating'].sum().round(0).astype(int)) + ' ICX'

df = add_count_differences(df_now, df_past, invar='stake', diff_var='Count')
render_mpl_table(df,
                 header_color='tab:purple',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Staked - " + fdate + " (Δ since " + fdate_past + ")" + '\n(Staked but Not delegating: ' + total_staked_but_not_delegated_text + ')')

plt.savefig(os.path.join(outputPath, "staking_balance_" + fdate + ".png"))



# unstaking
total_unstake_text = '{:,}'.format(df_now['unstake'].sum().round(0).astype(int)) + ' ICX'

df = add_count_differences(df_now, df_past, invar='unstake', diff_var='Count')
render_mpl_table(df,
                 header_color='firebrick',
                 header_columns=0,
                 col_width=3.5,
                 title="Total ICX Unstaking - " + fdate + " (Δ since " + fdate_past + ")" + '\n(' + total_unstake_text + ')')

plt.savefig(os.path.join(outputPath, "unstaking_balance_" + fdate + ".png"))



# i-score
total_iscore_text = '{:,}'.format(df_now['estimatedICX'].sum().round(0).astype(int)) + ' ICX'

df = add_count_differences(df_now, df_past, invar='estimatedICX', diff_var='Count')
render_mpl_table(df,
                 header_color='darkorange',
                 header_columns=0,
                 col_width=3.5,
                 title="Total I-Score Unclaimed - " + fdate + " (Δ since " + fdate_past + ")" + '\n(' + total_iscore_text + ')')

plt.savefig(os.path.join(outputPath, "iscore_balance_" + fdate + ".png"))



#
# # exchange wallets
# exchange_wallets = ['hx1729b35b690d51e9944b2e94075acff986ea0675',
#                     'hx99cc8eb746de5885f5e5992f084779fc0c2c135b',
#                     'hx9f0c84a113881f0617172df6fc61a8278eb540f5',
#                     'hxc4193cda4a75526bf50896ec242d6713bb6b02a3',
#                     'hxa527f96d8b988a31083167f368105fc0f2ab1143',
#                     'hx307c01535bfd1fb86b3b309925ae6970680eb30d',
#                     'hxff1c8ebad1a3ce1ac192abe49013e75db49057f8',
#                     'hx14ea4bca6f205fecf677ac158296b7f352609871',
#                     'hx3881f2ba4e3265a11cf61dd68a571083c7c7e6a5',
#                     'hxd9fb974459fe46eb9d5a7c438f17ae6e75c0f2d1',
#                     'hx68646780e14ee9097085f7280ab137c3633b4b5f',
#                     'hxbf90314546bbc3ed980454c9e2a9766160389302',
#                     'hx562dc1e2c7897432c298115bc7fbcc3b9d5df294',
#                     'hxb7f3d4bb2eb521f3c68f85bbc087d1e56a816fd6']
#
# exchange_names = ['binance_cold1',
#                   'binance_cold2',
#                   'binance_cold3',
#                   'binance_hot',
#                   'binance.us',
#                   'velic_hot',
#                   'velic_stave',
#                   'latoken',
#                   'coinex',
#                   'huobi',
#                   'kraken_hot',
#                   'upbit',
#                   'upbit_cold',
#                   'crypto.com']
#
# exchange_details = {'address': exchange_wallets,
#                     'names': exchange_names}
#
# # dataframe
# exchange_details = pd.DataFrame(exchange_details)
#
# # getting only exchange wallets
# def get_exchange_amount(df):
#     df_exchange = df[df['address'].isin(exchange_wallets)]
#     df_exchange = pd.merge(df_exchange,
#                            exchange_details,
#                            how='left',
#                            on='address')
#
#     total_exchange = df_exchange[['address', 'names', 'total']]\
#         .rename(columns={'total':'Amount (ICX)', 'names':'Exchanges'})\
#         .sort_values('Amount (ICX)', ascending=False)
#     return total_exchange
#
# total_exchange_now = get_exchange_amount(df_now)
# total_exchange_past = get_exchange_amount(df_past)
#
#
# # function to add count difference between before and now
# def add_val_differences(df_now, df_past, diff_var):
#     df_now['diff_val'] = (df_now[diff_var] - df_past[diff_var]).astype(int)
#     df_now[diff_var] = df_now[diff_var].astype(int).apply('{:,}'.format)
#     df_now['diff_symbol'] = df_now['diff_val'].apply(lambda x: "+" if x>0 else '')
#     df_now['diff_val'] = df_now['diff_val'].apply('{:,}'.format)
#     df_now['diff_val'] = np.where(df_now['diff_val'] == 0, '=', df_now['diff_val'])
#     df_now['diff_val'] = '(' + df_now['diff_symbol'] + df_now['diff_val'].astype(str) + ')'
#     df_now[diff_var] = df_now[diff_var].astype(str) + ' ' + df_now['diff_val']
#     df_now = df_now.drop(columns=['diff_val','diff_symbol'])
#     return df_now
#
# total_exchange = add_val_differences(total_exchange_now, total_exchange_past, 'Amount (ICX)')
#
# render_mpl_table(total_exchange,
#                  header_color='tab:pink',
#                  header_columns=0,
#                  col_width=5,
#                  title="Major Exchange Wallets - " + fdate)
#
# plt.savefig(os.path.join(outputPath, "exchange_wallets_" + fdate + ".png"))