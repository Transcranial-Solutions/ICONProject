#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 20:20:39 2024

@author: tono
"""
import requests
import json
import pandas as pd
import numpy as np
from tqdm import tqdm
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from pathlib import Path
import re

wallet_address_path = Path('/home/tono/ICONProject/data_analysis/wallet_addresses')



def fetch_data(url_base, limit=100):
    """
    Fetch paginated data from the given URL base.
    """
    all_data = []
    skip = 0
    while True:
        url = f'{url_base}&limit={limit}&skip={skip}'
        req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        if req.status_code != 200:
            break
        
        jtracker = json.loads(req.text)
        if not jtracker:
            break
        
        all_data.extend(jtracker)
        skip += limit
    return all_data

def is_hex(val):
    """
    Check if a given string is a valid hexadecimal number.
    """
    if isinstance(val, str) and re.fullmatch(r'0x[0-9a-fA-F]+', val):
        return True
    return False

def hex_to_int(val):
    if pd.isna(val) or not is_hex(val):
        return np.nan
    try:
        return int(val, 0)
    except ValueError:
        print(f"Failed to convert {val} to int")
        return np.nan

def get_token_decimals(address_list, nid):
    """
    Get token decimals for a list of addresses.
    """
    address_collector = {}
    for address in tqdm(address_list):
        address_collector[address] = {}
        try:
            cx_address_decimals = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
                                                .to(address) \
                                                .method("decimals") \
                                                .build()
            token_decimals = nid.call(cx_address_decimals)
            token_decimals_int = int(token_decimals, 16)
            
            
            cx_address_symbols = CallBuilder().from_("hx0000000000000000000000000000000000000001") \
                                                .to(address) \
                                                .method("symbol") \
                                                .build()
            token_symbols = nid.call(cx_address_symbols)            
        
        except:
            token_decimals_int = np.nan
            token_symbols = '$unknown'
        
        address_collector[address]['token_decimals'] = token_decimals_int
        address_collector[address]['token_symbols'] = token_symbols
    return address_collector

def save_to_json(data, filename):
    """
    Save dictionary data to a JSON file.
    """
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
        
def main():
    # URL Base
    url_base = 'https://tracker.icon.community/api/v1/addresses/contracts?search=&is_token=true'
    
    # Fetch Data
    all_data = fetch_data(url_base)
    jtracker_df = pd.DataFrame(all_data)
    
    # Get Address List
    address_list = jtracker_df['address'].to_list()
    
    local_endpoint = "http://127.0.0.1:9000/api/v3"
    icon_solidwallet_endpoint = "https://ctz.solidwallet.io/api/v3"
    
    # Initialize Icon Service
    try:
        print("Trying local endpoint")
        nid = IconService(HTTPProvider(local_endpoint))
    except:
        print("Local endpoint failed. Using Solidwallet instead.")
        nid = IconService(HTTPProvider(icon_solidwallet_endpoint))
    
    # Get Token Decimals
    address_collector = get_token_decimals(address_list, nid)
    save_to_json(address_collector, wallet_address_path.joinpath('token_addresses.json'))
    
    
    
if __name__ == "__main__":
    main()
