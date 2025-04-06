#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 05:43:58 2024

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


def compute_rolling_means(df, column_specs):
    """
    Compute rolling means for specified columns with given windows.
    
    :param df: DataFrame to process
    :param column_specs: Dictionary where keys are column names and values are lists of window sizes
    :return: DataFrame with rolling means added
    """
    for col, windows in column_specs.items():
        for window in windows:
            df[f'{col} (MA{window})'] = df[col].rolling(window=window).mean()
    return df

def compute_averages(df, column_specs):
    """
    Compute daily, weekly, and monthly averages for specified columns.
    
    :param df: DataFrame to process
    :param column_specs: List of column names to compute averages for
    :return: Dictionary with computed averages
    """
    num_rows = len(df) - 2
    if num_rows <= 0:
        num_rows = 1  # Avoid division by zero if DataFrame is too small
    
    averages = {}
    for col in column_specs:
        total_sum = df[col].sum()
        averages[f'average_{col}_per_day'] = total_sum / num_rows
        averages[f'average_{col}_per_week'] = total_sum / round(num_rows / 7)
        averages[f'average_{col}_per_month'] = total_sum / round(num_rows / 30)
    
    return averages



print(date_of_interest)

df_tx = pd.read_csv(os.path.join(resultPath, 'tx_detail_summary.csv'), low_memory=False)


for date_prev in date_of_interest:

    this_year = date_prev[0:4]
    
    resultPath_year = os.path.join(resultPath, this_year)
    if not os.path.exists(resultPath_year):
        os.mkdir(resultPath_year)

    starting_date = x_days_ago(date_yesterday, days_to_plot)
    
    df_tx = df_tx[df_tx['date'] >= starting_date].reset_index(drop=True)
    
    # Compute total fees
    df_tx['total_fees'] = df_tx['tx_fees_L1'] + df_tx['tx_fees_DEX']
    
    # Define column specifications for rolling means and averages
    rolling_mean_specs = {
        'tx_count': [7, 30],
        'total_fees': [7, 30],
        'hx_address_count': [7, 30]
    }
    
    average_specs = [
        'total_fees',
        'tx_count',
        'hx_address_count'
    ]
    

    rename_mapping = {
        'tx_count': 'Transactions (fees-incurring)',
        'tx_count (MA7)': 'Transactions (MA7)',
        'tx_count (MA30)': 'Transactions (MA30)',
        'total_fees': 'Fees burned',
        'total_fees (MA7)': 'Fees burned (MA7)',
        'total_fees (MA30)': 'Fees burned (MA30)',
        'hx_address_count': 'Active wallet count',
        'hx_address_count (MA7)': 'Active wallet count (MA7)',
        'hx_address_count (MA30)': 'Active wallet count (MA30)'
    }
    
    
    df_tx = compute_rolling_means(df_tx, rolling_mean_specs)
    averages = compute_averages(df_tx, average_specs)
    
    df_tx.rename(columns=rename_mapping, inplace=True)

    df_tx = df_tx.reset_index(drop=True)
    df_tx.to_csv(os.path.join(resultPath_year, f'tx_trend_{date_prev}.csv'))
    
    df_tx['date'] = pd.to_datetime(df_tx['date'], errors='coerce')
    
    def plot_ma(invar1, invar2, invar3,
                average_per_day, average_per_week, average_per_month,
                tickertype,
                ylab,
                ylab_color,
                my_title,
                months_to_plot=12,
                show_ma7=True,
                show_ma30=True,
                show_daily_average=True,
                ):
            
        end_date = df_tx['date'].max()
        start_date = end_date - pd.DateOffset(months=months_to_plot)
        filtered_df = df_tx[(df_tx['date'] >= start_date) & (df_tx['date'] <= end_date)]
    
        sns.set(style="ticks", rc={"lines.linewidth": 2})
        plt.style.use(['dark_background'])
        f, ax = plt.subplots(figsize=(12, 8))
        ax.plot(filtered_df['date'], filtered_df[invar1], label=invar1, alpha=0.3)
        
        if show_ma7:
            ax.plot(filtered_df['date'], filtered_df[invar2], label=invar2)
        if show_ma30:
            ax.plot(filtered_df['date'], filtered_df[invar3], label=invar3)
        
        # Compute averages over filtered data
        average_per_day_filtered = filtered_df[invar1].mean()
        
        # Plot averages as horizontal lines
        if show_daily_average:
            ax.axhline(y=average_per_day_filtered, color='b', linestyle=':',
                       label=f"Daily average ({'{:,}'.format(round(average_per_day_filtered))}{tickertype})")
    
        # Adjust x-axis based on months_to_plot
        if months_to_plot <= 1:
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        elif months_to_plot < 6:
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=15))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        else:
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        
        # Set x-axis limits to the data range
        ax.set_xlim(filtered_df['date'].min(), filtered_df['date'].max())
        
        sns.despine(offset=5, trim=False)
        plt.setp(ax.get_xticklabels(), rotation=90)
        ax.set_xlabel('Dates', fontsize=14, weight='bold', labelpad=10)
        ax.set_ylabel(ylab, fontsize=14, weight='bold', labelpad=10)
        ax.set_title(my_title, fontsize=14, weight='bold', loc='left', pad=10)
        
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels,
                  loc='upper right', bbox_to_anchor=(1, 1.08),
                  frameon=False, fancybox=True, shadow=True, ncol=2)
        
        ax.yaxis.label.set_color(ylab_color)
        
        # Collect all y-values from the data being plotted
        y_values = []
    
        # Add the main data
        y_values.extend(filtered_df[invar1].dropna().values)
    
        # Add moving averages if plotted
        if show_ma7:
            y_values.extend(filtered_df[invar2].dropna().values)
        if show_ma30:
            y_values.extend(filtered_df[invar3].dropna().values)
    
        # Add averages if they are being plotted
        if show_daily_average:
            y_values.append(average_per_day_filtered)
    
        # Compute y-axis limits
        y_min = min(y_values)
        y_max = max(y_values)
        y_range = y_max - y_min
    
        # Handle case when y_range is small or zero
        if y_range == 0:
            padding = y_max * 0.1 if y_max != 0 else 1  # Avoid zero padding
        else:
            padding = y_range * 0.1  # 10% of y_range
    
        # Ensure padding is at least a minimal value
        min_padding = y_max * 0.02 if y_max != 0 else 1
        padding = max(padding, min_padding)
    
        # Set y-limits with padding
        ax.set_ylim(y_min - padding, y_max + padding)
    
        # Set the number of y-ticks
        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=5, integer=True))
    
        # Update y-axis formatter
        if y_max >= 1e6:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f} M'.format(x / 1e6)))
        elif y_max >= 1e4:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f} K'.format(x / 1e3)))
        else:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,}'.format(int(x))))
        
        plt.tight_layout()
        plt.savefig(os.path.join(resultPath_year, my_title.replace(' ', '_') + '_' + date_prev + '.png'))
    
    
    plot_ma('Fees burned', 'Fees burned (MA7)', 'Fees burned (MA30)',
            averages.get('average_total_fees_per_day'), 
            averages.get('average_total_fees_per_week'),
            averages.get('average_total_fees_per_month'),
            ' ICX',
            '$ICX',
            'cyan',
            'Fees ($ICX) burned (last 12 months)'
            )
    
    plot_ma('Transactions (fees-incurring)', 'Transactions (MA7)', 'Transactions (MA30)',
            averages.get('average_tx_count_per_day'), 
            averages.get('average_tx_count_per_week'),
            averages.get('average_tx_count_per_month'),
            ' Tx',
            'Transactions',
            'white',
            'Transactions (last 12 months)'
            )
    
    months_to_plot=1
    plot_ma('Active wallet count', 'Active wallet count (MA7)', 'Active wallet count (MA30)',
            averages.get('average_hx_address_count_per_day'), 
            averages.get('average_hx_address_count_per_week'),
            averages.get('average_hx_address_count_per_month'),
            '',
            'Wallet count',
            'white',
            f'Number of active wallets (last {months_to_plot} months)',
            months_to_plot=months_to_plot,
            show_ma7=True,
            show_ma30=False,
            show_daily_average=False,
            )