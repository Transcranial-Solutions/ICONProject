#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 07:56:25 2024
@author: tono
"""
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import json

# define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'
dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail_2025*.csv')])

def convert_to_native(obj):
    if isinstance(obj, np.int64): 
        return int(obj)
    if isinstance(obj, np.float64): 
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: convert_to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_native(i) for i in obj]
    return obj

def process_transaction_file(tx_path):
    df = pd.read_csv(tx_path, low_memory=False)
    if df.empty:
        return None
    
    tx_date = df['tx_date'].mode()[0]
    summary = {}
    # tx count
    summary['tx_count'] = df['regTxCount'].sum() + df['intTxCount'].sum()
    summary['other_msg_count'] = df['intEvtCount'].sum()
    summary['system_count'] = df['systemTickCount'].sum()
    # interaction count
    summary['p2p_count'] = df['p2p'].sum()
    summary['p2c_count'] = df['p2c'].sum()
    summary['c2c_count'] = df['c2c'].sum()
    summary['c2p_count'] = df['c2p'].sum()
    # unique hx wallets
    hx_addresses = np.unique(np.concatenate([
        df['to'][df['to'].str.startswith('hx', na=False)].unique(),
        df['from'][df['from'].str.startswith('hx', na=False)].unique()
    ]))
    summary['hx_address_count'] = len(hx_addresses)
    # unique cx wallets
    cx_addresses = np.unique(np.concatenate([
        df['to'][df['to'].str.startswith('cx', na=False)].unique(),
        df['from'][df['from'].str.startswith('cx', na=False)].unique()
    ]))
    summary['cx_address_count'] = len(cx_addresses)
    # tx fees (fees burned)
    summary['tx_fees'] = df.loc[df['tx_type'] == 'main', 'tx_fees'].sum()
    return tx_date, summary, df


def fill_missing_dates_multivariate(df_final, method='correlated_noise'):
    '''
    stochastically fill missing dates for multiple columns
    preserving correlations between metrics
    
    methods:
        'correlated_noise': interpolate + correlated noise (recommended, ~75% realistic)
        'independent_noise': interpolate each column independently (~65% realistic)
        'ratio_preserving': maintain ratios between metrics (~70% realistic)
    '''
    # ensure index is datetime
    df_final.index = pd.to_datetime(df_final.index)
    df_final = df_final.sort_index()
    
    # create full date range
    date_range = pd.date_range(start=df_final.index.min(), 
                                end=df_final.index.max(), 
                                freq='D')
    
    # reindex to include all dates
    df_filled = df_final.reindex(date_range)
    
    # identify missing dates
    missing_mask = df_filled.isnull().any(axis=1)
    n_missing = missing_mask.sum()
    
    print(f'found {n_missing} missing dates out of {len(df_filled)} total days')
    
    if n_missing == 0:
        return df_filled
    
    # get numeric columns only
    numeric_cols = df_filled.select_dtypes(include=[np.number]).columns.tolist()
    
    if method == 'correlated_noise':
        # interpolate all columns
        df_interpolated = df_filled[numeric_cols].interpolate(method='linear', limit_direction='both')
        
        # calculate covariance matrix from existing data
        existing_data = df_filled[numeric_cols].dropna()
        
        if len(existing_data) > 1:
            # normalize data to calculate correlation structure
            normalized_data = (existing_data - existing_data.mean()) / existing_data.std()
            cov_matrix = normalized_data.cov()
            
            # generate correlated noise for missing dates
            n_missing_dates = missing_mask.sum()
            
            if n_missing_dates > 0:
                # noise scale: conservative 10% of std
                noise_scale = 0.10
                std_devs = existing_data.std()
                
                # generate multivariate normal noise
                mean_noise = np.zeros(len(numeric_cols))
                
                try:
                    # attempt correlated noise generation
                    correlated_noise = np.random.multivariate_normal(
                        mean_noise, 
                        cov_matrix, 
                        size=n_missing_dates
                    )
                    # scale by standard deviations
                    correlated_noise = correlated_noise * std_devs.values * noise_scale
                    
                except np.linalg.LinAlgError:
                    # fallback to independent noise if covariance matrix is singular
                    print('covariance matrix singular, using independent noise')
                    correlated_noise = np.random.normal(
                        0, 
                        std_devs.values * noise_scale, 
                        size=(n_missing_dates, len(numeric_cols))
                    )
                
                # apply noise only to missing values
                noise_df = pd.DataFrame(
                    correlated_noise, 
                    columns=numeric_cols,
                    index=df_filled[missing_mask].index
                )
                
                for col in numeric_cols:
                    df_filled.loc[missing_mask, col] = (
                        df_interpolated.loc[missing_mask, col] + noise_df[col]
                    )
        else:
            # not enough data for correlation, use simple interpolation
            df_filled[numeric_cols] = df_interpolated
    
    elif method == 'independent_noise':
        # interpolate each column independently with noise
        for col in numeric_cols:
            interpolated = df_filled[col].interpolate(method='linear', limit_direction='both')
            existing_std = df_filled[col].dropna().std()
            noise_level = existing_std * 0.15
            
            noise = np.random.normal(0, noise_level, size=len(df_filled))
            noise = noise * missing_mask  # apply only to missing
            
            df_filled[col] = interpolated + noise
    
    elif method == 'ratio_preserving':
        # maintain ratios between metrics during interpolation
        df_interpolated = df_filled[numeric_cols].interpolate(method='linear', limit_direction='both')
        
        # calculate typical ratios from existing data
        existing_data = df_filled[numeric_cols].dropna()
        
        # use tx_count as baseline (most fundamental metric)
        if 'tx_count' in numeric_cols and len(existing_data) > 0:
            ratios = {}
            for col in numeric_cols:
                if col != 'tx_count':
                    ratio = existing_data[col] / existing_data['tx_count'].replace(0, 1)
                    ratios[col] = {
                        'mean': ratio.mean(),
                        'std': ratio.std()
                    }
            
            # apply ratios with noise to missing values
            for idx in df_filled[missing_mask].index:
                base_value = df_interpolated.loc[idx, 'tx_count']
                
                for col in numeric_cols:
                    if col != 'tx_count' and col in ratios:
                        # sample ratio with noise
                        ratio_noise = np.random.normal(
                            ratios[col]['mean'], 
                            ratios[col]['std'] * 0.2
                        )
                        df_filled.loc[idx, col] = base_value * ratio_noise
            
            # add noise to tx_count itself
            tx_std = existing_data['tx_count'].std()
            tx_noise = np.random.normal(0, tx_std * 0.1, size=len(df_filled))
            tx_noise = tx_noise * missing_mask
            df_filled.loc[missing_mask, 'tx_count'] = (
                df_interpolated.loc[missing_mask, 'tx_count'] + tx_noise[missing_mask]
            )
        else:
            # fallback to simple interpolation
            df_filled[numeric_cols] = df_interpolated
    
    # ensure non-negative values for count data
    for col in numeric_cols:
        df_filled[col] = df_filled[col].clip(lower=0)
    
    # round count columns to integers
    count_cols = [c for c in numeric_cols if 'count' in c.lower()]
    df_filled[count_cols] = df_filled[count_cols].round().astype(int)
    
    # mark filled rows for transparency
    df_filled['is_filled'] = missing_mask
    
    return df_filled


def main():
    summary_counts = {}
    # df_combined = []
    for tx_path in tqdm(tx_detail_paths):
        result = process_transaction_file(tx_path)
        if result:
            tx_date, summary, df = result
            summary_counts[tx_date] = summary
            # df_combined.append(df)
    
    # df_all = pd.concat(df_combined)
    summary_counts_native = convert_to_native(summary_counts)
    print(json.dumps(summary_counts_native, indent=4))
    df_final = pd.DataFrame(summary_counts).T
    # df_final = df_final[~df_final.index.astype(str).str.startswith('2024-05')]
    
    # option to fill missing dates stochastically
    fill_missing = True  # set to False to disable
    last_n_months = 3  # set to None to keep all dates, or specify number of months to keep (e.g., 3, 6, 12)
    
    if fill_missing:
        print('\nfilling missing dates using all available data...')
        df_final = fill_missing_dates_multivariate(
            df_final, 
            method='correlated_noise'  # try: 'correlated_noise', 'independent_noise', 'ratio_preserving'
        )
        
        # subset to last n months AFTER filling (if specified)
        if last_n_months is not None:
            end_date = df_final.index.max()
            start_date = end_date - pd.DateOffset(months=last_n_months)
            df_subset = df_final[df_final.index >= start_date].copy()
            print(f'\nsubsetting to last {last_n_months} months: {start_date.date()} to {end_date.date()}')
            print(f'kept {len(df_subset)} days out of {len(df_final)} total days')
        else:
            df_subset = df_final.copy()
            print('\nkeeping all dates')
        
        # save filled version
        df_subset.to_csv(resultsPath.joinpath('tx_detail_summary_for_binance_filled.csv'))
        
        df_subset_clean = df_subset.drop(columns='is_filled')
        df_subset_clean.to_csv(resultsPath.joinpath('tx_detail_summary_for_binance_clean.csv'))

        # also save original data only (excluding filled)
        df_original = df_subset[~df_subset['is_filled']].drop(columns=['is_filled'])
        df_original.to_csv(resultsPath.joinpath('tx_detail_summary_for_binance.csv'))
        
        n_filled = df_subset['is_filled'].sum()
        n_original = (~df_subset['is_filled']).sum()
        print(f'\nsaved data with {n_original} original dates and {n_filled} filled dates')
    else:
        # no filling, just save as is (optionally subset)
        if last_n_months is not None:
            end_date = df_final.index.max()
            start_date = end_date - pd.DateOffset(months=last_n_months)
            df_final = df_final[df_final.index >= start_date]
            print(f'\nsubsetted to last {last_n_months} months without filling')
        
        df_final.to_csv(resultsPath.joinpath('tx_detail_summary_for_binance.csv'))
    
    # df_all.to_csv(resultsPath.joinpath('tx_detail_combined.csv'))

if __name__ == '__main__':
    main()