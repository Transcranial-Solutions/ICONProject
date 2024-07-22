#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 19:21:41 2024

@author: tono
"""

from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
# from collections import OrderedDict
import json

dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

# save_data = True
# log_scale = False

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


summary_counts = {}

# summary_counts = OrderedDict()
for tx_path in tqdm(tx_detail_paths):
    df = pd.read_csv(tx_path, low_memory=False)
    tx_date = df['tx_date'].mode()[0]
    summary_counts[tx_date] = {}
    
    # tx count
    tx_count = df['regTxCount'].sum() + df['intTxCount'].sum()
    other_msg_count = df['intEvtCount'].sum()
    system_count = df['systemTickCount'].sum()
    
    # interaction count
    p2p_count = df['p2p'].sum()
    p2c_count = df['p2c'].sum()
    c2c_count = df['c2c'].sum()
    c2p_count = df['c2p'].sum()
    

    # unique hx wallets
    hx_unique_to = df['to'][df['to'].str.startswith('hx', na=False)].unique()
    hx_unique_from = df['from'][df['from'].str.startswith('hx', na=False)].unique()
    hx_address_count = len(np.unique(np.concatenate((hx_unique_to, hx_unique_from))))
    
    # unique cx wallets
    cx_unique_to = df['to'][df['to'].str.startswith('cx', na=False)].unique()
    cx_unique_from = df['from'][df['from'].str.startswith('cx', na=False)].unique()
    cx_address_count = len(np.unique(np.concatenate((cx_unique_to, cx_unique_from))))
    
    # tx fees (fees burned)
    tx_fees = df[df['tx_type'] == 'main']['tx_fees'].sum()
    
    
    # =============================================================================
    #     # dict
    # =============================================================================
    summary_counts[tx_date]['tx_count'] = tx_count
    summary_counts[tx_date]['other_msg_count'] = other_msg_count
    summary_counts[tx_date]['system_count'] = system_count

    summary_counts[tx_date]['p2p_count'] = p2p_count
    summary_counts[tx_date]['p2c_count'] = p2c_count
    summary_counts[tx_date]['c2c_count'] = c2c_count
    summary_counts[tx_date]['c2p_count'] = c2p_count

    summary_counts[tx_date]['hx_address_count'] = hx_address_count
    summary_counts[tx_date]['cx_address_count'] = cx_address_count

    summary_counts[tx_date]['tx_fees'] = tx_fees


summary_counts_native = convert_to_native(summary_counts)

print(json.dumps(summary_counts_native, indent=4))

df = pd.DataFrame(summary_counts).T

df = df[~df.index.astype(str).str.startswith('2024-05')]

df.to_csv(resultsPath.joinpath('tx_detail_summary.csv'))


