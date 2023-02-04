#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 09:47:52 2023

@author: tono
"""
import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet
import matplotlib.ticker as ticker
import six
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from typing import Union


# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

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
    
walletPath = '/home/tono/ICONProject/data_analysis/wallet'
  
## Creating Wallet if does not exist (only done for the first time)
tester_wallet = os.path.join(walletPath, "test_keystore_1")

if os.path.exists(tester_wallet):
    wallet = KeyWallet.load(tester_wallet, "abcd1234*")
else:
    wallet = KeyWallet.create()
    wallet.get_address()
    wallet.get_private_key()
    wallet.store(tester_wallet, "abcd1234*")

tester_address = wallet.get_address()
SYSTEM_ADDRESS = "cx0000000000000000000000000000000000000000"
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))


def get_iiss_info():
    call = CallBuilder().from_(tester_address) \
        .to(SYSTEM_ADDRESS) \
        .method("getIISSInfo") \
        .build()
    result = icon_service.call(call)['variable']

    df = {'Icps': hex_to_int(result['Icps']), 'Iglobal': parse_icx(result['Iglobal']),
          'Iprep': hex_to_int(result['Iprep']),
          'Irelay': hex_to_int(result['Irelay']), 'Ivoter': hex_to_int(result['Ivoter'])}

    return df

iglobal = get_iiss_info()['Iglobal']
daily_issuance = iglobal*12/365
daily_issuance_text = '{:,}'.format(round(daily_issuance)) + ' ICX'
# daily_issuance_voter = 

total_supply = round(loop_to_icx(icon_service.get_total_supply()))
total_supply_text = '{:,}'.format(total_supply) + ' ICX'
yearly_inflation = '{:.2%}'.format(iglobal*12/total_supply)


today = datetime.utcnow() - timedelta(days=1)
day_today = today.strftime("%Y-%m-%d")
day_today_fn = today.strftime("%Y_%m_%d")
# day_today_text = today.strftime("%Y/%m/%d")
this_year = day_today_fn[0:4]

# making path for saving
currPath = '/home/tono/ICONProject/data_analysis/'
inPath = '/home/tono/ICONProject/data_analysis/output/'
prepvotePath = os.path.join(inPath, "prep_votes")
savePath = os.path.join(prepvotePath, this_year)

fn = f'prep_top_100_votes_and_bond_{day_today_fn}.csv'
df = pd.read_csv(os.path.join(savePath, fn), low_memory=False)

df['Bond Status'] = np.where(df['bond'] != 0, 'Bonded', 'Not bonded')
df = df.rename(columns = {'prep_type': 'P-Rep Type'})


df_plot_count = df.groupby(['Bond Status','P-Rep Type'])\
    .size()\
    .reset_index()\
    .pivot(columns='P-Rep Type', index='Bond Status', values=0)
df_plot_count = df_plot_count[['main','sub','candidate']]

df_plot_votes = df.groupby(['Bond Status','P-Rep Type'])['delegation']\
    .sum()\
    .reset_index()\
    .pivot(columns='P-Rep Type', index='Bond Status', values='delegation')
# df_plot = df_plot[['candidate','sub','main']]
df_plot_votes = df_plot_votes[['main','sub','candidate']]

def plot_bonded_status(df, my_title, ylab):
    
    def shorten_number(x):
        if x>= 1000000:
            x = '{:,.1f}'.format(x / 1e6) + ' M'
        elif x>= 100000:
            x = '{:,.0f}'.format(x / 1e3) + ' K'
        elif x>= 1000:
            x = '{:,.0f}'.format(x)
        elif x< 1000:
            x = int(x)
        return x

    sns.set(style="ticks", rc={"lines.linewidth": 1})
    plt.style.use(['dark_background'])
    
    ax = df.plot(kind='bar', stacked=True)
    plt.setp(ax.get_xticklabels(), rotation=0)
    handles, labels = ax.get_legend_handles_labels()
    # ax.legend(reversed(handles), reversed(labels))
    
    for c in ax.containers:

        labels = ['{:,.1f}'.format(x / 1e6) + ' M' if x>= 1000000\
                  else '{:,.0f}'.format(x / 1e3) + ' K' if x>= 100000\
                  else '{:,.0f}'.format(x) if x>= 1000\
                  else int(x) if x< 1000\
                  else x for x in c.datavalues]
        
        labels = [a if a else "" for a in labels]


        ax.bar_label(c, labels=labels, label_type='center', color='black',
                     path_effects=[pe.withStroke(linewidth=2, foreground='white')])
    
    
    my_labels = [shorten_number(i) for i in df.sum(axis=1).to_list()]
    
    ax.bar_label(ax.containers[2], labels=my_labels, padding=10, color='cyan', weight='bold')
    
    ax.margins(y=0.2)
    
    ax.legend(title='P-Rep Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_xlabel('Bond status', fontsize=14, weight='bold', labelpad=10)
    ax.set_ylabel(ylab, fontsize=14, weight='bold', labelpad=10)
    ax.set_title(my_title, fontsize=14, weight='bold', loc='left', pad=10)

    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    
    if ymax >= 1000:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x)))
    if ymax >= 100000:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
    if ymax >= 1000000:
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))
    plt.tight_layout()
    
    plt.savefig(os.path.join(savePath, my_title\
                             .replace(' ', '_')\
                             .replace('(','')\
                             .replace(')','')\
                             .replace('$','')\
                             .replace('-','_')\
                                 .replace('\n','_')\
                                 + '.png'))


plot_bonded_status(df_plot_count, f'Number of P-Rep types by\nbond status ({day_today})', 'Number of P-Reps')
plot_bonded_status(df_plot_votes, f'$ICX delegated to P-Rep types by\nbond status ({day_today})', '$ICX')
