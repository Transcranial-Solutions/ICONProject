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

# Define paths
dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = Path(projectPath).joinpath('data')
resultsPath = Path(projectPath).joinpath('results')

tx_detail_paths = sorted([i for i in dataPath.glob('tx_detail*.csv')])



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
    df_final = df_final[~df_final.index.astype(str).str.startswith('2024-05')]
    df_final.to_csv(resultsPath.joinpath('tx_detail_summary.csv'))
    # df_all.to_csv(resultsPath.joinpath('tx_detail_combined.csv'))

if __name__ == "__main__":
    main()


