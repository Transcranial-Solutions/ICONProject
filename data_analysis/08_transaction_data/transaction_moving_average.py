#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 28 18:23:21 2023

@author: tono
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

import seaborn as sns
from datetime import date, datetime, timedelta


dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data/'

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(projectPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)

    
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")
yesterday = datetime.utcnow() - timedelta(1)
date_yesterday = yesterday.strftime("%Y-%m-%d")

days_to_plot = 365


def yesterday(doi = "2021-08-20"):
    yesterday = datetime.fromisoformat(doi) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20"):
    tomorrow = datetime.fromisoformat(doi) + timedelta(1)
    return tomorrow.strftime('%Y-%m-%d')

def x_days_ago(doi = "2021-08-20", x_days='180'):
    x_days_ago = datetime.fromisoformat(doi) - timedelta(x_days)
    return x_days_ago.strftime('%Y-%m-%d')

# to use specific date (1), use yesterday (0), use range(2)
use_specific_prev_date = 0
date_prev = "2022-07-12"

if use_specific_prev_date == 1:
    date_of_interest = [date_prev]
elif use_specific_prev_date == 0:
    date_of_interest = [yesterday(date_today)]
elif use_specific_prev_date == 2:
    # for loop between dates
    day_1 = "2022-07-13"; day_2 = "2022-07-16"
    date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()
else:
    date_of_interest=[]
    print('No date selected.')

print(date_of_interest)

df_tx = pd.read_csv(os.path.join(resultPath, 'compiled_tx_summary_date.csv'), low_memory=False)


for date_prev in date_of_interest:

    this_year = date_prev[0:4]
    
    resultPath_year = os.path.join(resultPath, this_year)
    if not os.path.exists(resultPath_year):
        os.mkdir(resultPath_year)

    starting_date = x_days_ago(date_yesterday, days_to_plot)
    
    df_tx = df_tx[df_tx['date'] >= starting_date]
    
    df_tx['Regular & Interal Tx'] = df_tx['Regular Tx'] + df_tx['Internal Tx']
    df_tx['Regular & Interal Tx (MA7)'] = df_tx['Regular & Interal Tx'].rolling(window=7).mean()
    df_tx['Regular & Interal Tx (MA30)'] = df_tx['Regular & Interal Tx'].rolling(window=30).mean()
    
    # df_tx['Regular Tx (MA7)'] = df_tx['Regular Tx'].rolling(window=7).mean()
    # df_tx['Regular Tx (MA30)'] = df_tx['Regular Tx'].rolling(window=30).mean()
    
    # df_tx['Internal Tx (MA7)'] = df_tx['Internal Tx'].rolling(window=7).mean()
    # df_tx['Internal Tx (MA30)'] = df_tx['Internal Tx'].rolling(window=30).mean()
    
    # df_tx['Internal Event (MA7)'] = df_tx['Internal Event (excluding Tx)'].rolling(window=7).mean()
    # df_tx['Internal Event (MA30)'] = df_tx['Internal Event (excluding Tx)'].rolling(window=30).mean()
    
    df_tx['Fees burned (MA7)'] = df_tx['Fees burned'].rolling(window=7).mean()
    df_tx['Fees burned (MA30)'] = df_tx['Fees burned'].rolling(window=30).mean()
    
    average_fee_per_day = df_tx['Fees burned'].sum()/(len(df_tx)-2)
    average_fee_per_week = df_tx['Fees burned'].sum()/round((len(df_tx)-2)/7)
    average_fee_per_month = df_tx['Fees burned'].sum()/round((len(df_tx)-2)/30)
    
    
    average_tx_per_day = df_tx['Regular & Interal Tx'].sum()/(len(df_tx)-2)
    average_tx_per_week = df_tx['Regular & Interal Tx'].sum()/round((len(df_tx)-2)/7)
    average_tx_per_month = df_tx['Regular & Interal Tx'].sum()/round((len(df_tx)-2)/30)
    
    
    df_tx = df_tx.reset_index(drop=True)
    df_tx.to_csv(os.path.join(resultPath_year, f'tx_trend_{date_prev}.csv'))
    
    def plot_ma(invar1, invar2, invar3,
                average_per_day, average_per_week, average_per_month,
                tickertype,
                ylab,
                ylab_color,
                my_title):
        sns.set(style="ticks", rc={"lines.linewidth": 2})
        plt.style.use(['dark_background'])
        f, ax = plt.subplots(figsize=(12, 8))
        ax.plot(df_tx['date'], df_tx[invar1], label=invar1, alpha=0.3)
        ax.plot(df_tx['date'], df_tx[invar2], label=invar2)
        ax.plot(df_tx['date'], df_tx[invar3], label=invar3)
        
        ax.axhline(y = average_per_day, color='b', linestyle=':', label = f"Daily average ({'{:,}'.format(round(average_per_day))} {tickertype})")
        ax.axhline(y = average_per_day, color='k', linestyle='None', label = f"Weekly average ({'{:,}'.format(round(average_per_week))} {tickertype})")
        ax.axhline(y = average_per_day, color='k', linestyle='None', label = f"Monthly average ({'{:,}'.format(round(average_per_month))} {tickertype})")
        
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        sns.despine(offset=5, trim=True)
        plt.setp(ax.get_xticklabels(), rotation=90)
        ax.set_xlabel('Dates', fontsize=14, weight='bold', labelpad=10)
        ax.set_ylabel(ylab, fontsize=14, weight='bold', labelpad=10)
        ax.set_title(my_title, fontsize=14, weight='bold', loc='left', pad=10)
        plt.legend(loc="upper left")
        plt.tight_layout()
        
        handles, labels = ax.get_legend_handles_labels()
        
        ax.legend(handles, labels,
                loc='upper right', bbox_to_anchor=(1, 1.08),
                frameon=False, fancybox=True, shadow=True, ncol=2)
        
        ax.yaxis.label.set_color(ylab_color)
        
        xmin, xmax = ax.get_xlim()
        ymin, ymax = ax.get_ylim()
        
        if ymax >= 1000:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x)))
        if ymax >= 100000:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
        if ymax >= 1000000:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))
            
        plt.savefig(os.path.join(resultPath_year, my_title.replace(' ', '_') + '_' + date_prev + '.png'))
    
    
    plot_ma('Fees burned', 'Fees burned (MA7)', 'Fees burned (MA30)',
            average_fee_per_day, average_fee_per_week, average_fee_per_month, 'ICX',
            '$ICX',
            'cyan',
            'Fees ($ICX) burned (last 12 months)'
            )
    
    plot_ma('Regular & Interal Tx', 'Regular & Interal Tx (MA7)', 'Regular & Interal Tx (MA30)',
            average_tx_per_day, average_tx_per_week, average_tx_per_month, 'Tx',
            'Transactions',
            'white',
            'Transactions (last 12 months)'
            )