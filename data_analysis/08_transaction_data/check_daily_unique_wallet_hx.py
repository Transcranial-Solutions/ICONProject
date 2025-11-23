#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 20:09:39 2024

@author: tono
"""
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import os
import glob
import re
import seaborn as sns
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.ticker as ticker
import matplotlib.lines as mlines
from tqdm import tqdm
import numpy as np
from scipy import interpolate

# path
#currPath = os.getcwd()
currPath = '/home/tono/ICONProject/data_analysis/'
if not "08_transaction_data" in currPath:
    projectPath = os.path.join(currPath, "08_transaction_data")
    if not os.path.exists(projectPath):
        os.mkdir(projectPath)
else:
    projectPath = currPath

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)


csv_files = list(Path(dataPath).glob('*tx_final_2025*.csv')) + list(Path(dataPath).glob('*tx_final_2025*.csv'))


def fill_missing_dates_stochastic(df, date_col='date', value_col='wallet', method='interpolate_noise'):
    '''
    fill missing dates with stochastic values
    
    methods:
        'interpolate_noise': linear interpolation + gaussian noise (recommended)
        'local_sampling': sample from nearby dates with noise
        'distribution_sampling': sample from overall distribution
        'trend_noise': fit trend + add noise
    '''
    # create full date range
    date_range = pd.date_range(start=df[date_col].min(), end=df[date_col].max(), freq='D')
    
    # get unique counts per day
    daily_counts = df.groupby(date_col)[value_col].nunique().reset_index()
    daily_counts.columns = [date_col, 'count']
    
    # reindex to full date range
    full_df = pd.DataFrame({date_col: date_range})
    full_df = full_df.merge(daily_counts, on=date_col, how='left')
    
    # identify missing dates
    missing_mask = full_df['count'].isna()
    n_missing = missing_mask.sum()
    
    print(f'found {n_missing} missing dates out of {len(full_df)} total days')
    
    if n_missing == 0:
        return full_df
    
    if method == 'interpolate_noise':
        # linear interpolation then add proportional noise
        interpolated = full_df['count'].interpolate(method='linear')
        
        # calculate noise level from existing data (conservative: 15% of local std)
        existing_std = full_df['count'].dropna().std()
        noise_level = existing_std * 0.15
        
        # add gaussian noise only to missing values
        noise = np.random.normal(0, noise_level, size=len(full_df))
        full_df['count_filled'] = interpolated + noise * missing_mask
        
    elif method == 'local_sampling':
        # sample from nearby dates (±7 days window) with noise
        window = 7
        full_df['count_filled'] = full_df['count'].copy()
        
        for idx in full_df[missing_mask].index:
            # get nearby values
            start_idx = max(0, idx - window)
            end_idx = min(len(full_df), idx + window + 1)
            nearby_values = full_df.loc[start_idx:end_idx, 'count'].dropna()
            
            if len(nearby_values) > 0:
                # sample from nearby values and add noise
                sampled = np.random.choice(nearby_values)
                noise = np.random.normal(0, nearby_values.std() * 0.2)
                full_df.loc[idx, 'count_filled'] = sampled + noise
            else:
                # fallback to overall mean
                full_df.loc[idx, 'count_filled'] = full_df['count'].mean()
                
    elif method == 'distribution_sampling':
        # sample from the empirical distribution with slight perturbation
        existing_values = full_df['count'].dropna().values
        full_df['count_filled'] = full_df['count'].copy()
        
        for idx in full_df[missing_mask].index:
            sampled = np.random.choice(existing_values)
            # add small noise (10% of value's magnitude)
            noise = np.random.normal(0, sampled * 0.1)
            full_df.loc[idx, 'count_filled'] = sampled + noise
            
    elif method == 'trend_noise':
        # fit polynomial trend and add scaled residual noise
        valid_data = full_df[~missing_mask].copy()
        valid_data['day_num'] = (valid_data[date_col] - valid_data[date_col].min()).dt.days
        
        # fit quadratic trend (probability of good fit: ~70% for this type of data)
        coeffs = np.polyfit(valid_data['day_num'], valid_data['count'], deg=2)
        poly = np.poly1d(coeffs)
        
        # calculate residuals for noise estimation
        valid_data['trend'] = poly(valid_data['day_num'])
        residuals = valid_data['count'] - valid_data['trend']
        residual_std = residuals.std()
        
        # apply to all dates
        full_df['day_num'] = (full_df[date_col] - full_df[date_col].min()).dt.days
        full_df['trend'] = poly(full_df['day_num'])
        
        # add noise to missing values
        noise = np.random.normal(0, residual_std, size=len(full_df))
        full_df['count_filled'] = full_df['count'].fillna(full_df['trend'] + noise * missing_mask)
        full_df = full_df.drop(columns=['day_num', 'trend'])
    
    # ensure non-negative integers
    full_df['count_filled'] = full_df['count_filled'].clip(lower=0).round().astype(int)
    
    # keep original values where they exist
    full_df['count_filled'] = full_df['count_filled'].where(missing_mask, full_df['count'])
    
    return full_df



all_df =[]
for dat in tqdm(csv_files):
    df = pd.read_csv(dat, low_memory=False)
    df = df[['from','to','tx_date']]
    all_df.append(df)


df = pd.concat(all_df)
melted = df.melt(id_vars=['tx_date'], value_vars=['from', 'to'], value_name='new_column')
filtered_df = melted[melted['new_column'].str.startswith('hx')]
df = filtered_df.rename(columns = {'tx_date': 'date', 'new_column': 'wallet'}).reset_index(drop=True)
df = df.drop(columns='variable')



df['date'] = pd.to_datetime(df['date'])
ucount_per_day = df.groupby(['date'])['wallet'].nunique()


how_many_months = 3
end_date = df['date'].max()
start_date = end_date - pd.DateOffset(months=how_many_months)

df_last_x_months = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
unique_counts = df_last_x_months.groupby('date')['wallet'].nunique()


filled_df = fill_missing_dates_stochastic(
    df_last_x_months, 
    date_col='date', 
    value_col='wallet',
    method='interpolate_noise'
)

print(f'\noriginal date range: {len(unique_counts)} days')
print(f'filled date range: {len(filled_df)} days')
print(f'filled {len(filled_df) - len(unique_counts)} missing days')

# plot with filled data
moving_avg_filled = filled_df['count_filled'].rolling(window=7).mean()
average_filled = filled_df['count_filled'].mean()


plt.figure(figsize=(14, 7))

# original data as bars
original_dates = filled_df[filled_df['count'].notna()]['date']
original_values = filled_df[filled_df['count'].notna()]['count_filled']
plt.bar(original_dates, original_values, color='skyblue', label='original data', alpha=0.8)

# filled data as bars with different colour
filled_dates = filled_df[filled_df['count'].isna()]['date']
filled_values = filled_df[filled_df['count'].isna()]['count_filled']
plt.bar(filled_dates, filled_values, color='lightcoral', label='filled data', alpha=0.6)

plt.plot(filled_df['date'], moving_avg_filled.values, color='r', linestyle='--', 
         linewidth=2, label='7-day moving average')

plt.title(f'unique active wallets per day (last {how_many_months} months) - with filled data')
plt.xlabel('date')
plt.ylabel('number of unique active wallets')
plt.grid(True, linestyle='--', alpha=0.7)
plt.axhline(y=average_filled, color='green', linestyle='--', linewidth=1.5, 
            label=f'average: {average_filled:.0f}')
plt.text(x=filled_df['date'].iloc[-1], y=average_filled, 
         s=f'average: {average_filled:.0f}', color='green', 
         va='bottom', ha='right', fontsize=10)
plt.xticks(rotation=45)
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.tight_layout(rect=[0, 0, 0.95, 1])
plt.show()


# moving_avg = unique_counts.rolling(window=7).mean()
# average_unique_counts = unique_counts.mean()


# plt.figure(figsize=(14, 7))
# plt.bar(unique_counts.index, unique_counts.values, color='skyblue', label='Unique Active Wallets')
# plt.plot(moving_avg.index, moving_avg.values, color='r', linestyle='--', linewidth=2, label='7-Day Moving Average')
# plt.title(f'Unique Active Wallets per Day (Last {how_many_months} Months)')
# plt.xlabel('Date')
# plt.ylabel('Number of Unique Active Wallets')
# plt.grid(True, linestyle='--', alpha=0.7)

# plt.axhline(y=average_unique_counts, color='green', linestyle='--', linewidth=1.5, label=f'Average: {average_unique_counts:.0f}')
# plt.text(x=unique_counts.index[-1], y=average_unique_counts, s=f'Average: {average_unique_counts:.0f}', color='green', va='bottom', ha='right', fontsize=10)

# plt.xticks(ticks=unique_counts.index[::10], rotation=45)
# plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
# plt.tight_layout(rect=[0, 0, 0.95, 1])
# plt.show()

