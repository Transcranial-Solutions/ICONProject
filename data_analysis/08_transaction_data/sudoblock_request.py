#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 16:03:18 2024

@author: tono
"""

import requests
from pprint import pprint

base_url = "https://tracker.v2.mainnet.sng.vultr.icon.community/api/v1/statistics"

endpoint = "ecosystem"
response = requests.get(f"{base_url}/{endpoint}")


if response.status_code == 200:
    data = response.json()
    pprint(data)
else:
    print(f"Failed to retrieve data: {response.status_code}")
    
    
