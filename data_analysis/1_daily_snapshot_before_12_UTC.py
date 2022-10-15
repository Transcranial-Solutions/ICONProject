#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 05:14:12 2022

@author: tono
"""

import os

daily_path = '/home/tono/ICONProject/data_analysis/'
wallet_ranking_path = '/home/tono/ICONProject/data_analysis/06_wallet_ranking'


program_list = [os.path.join(daily_path, "0_icon_community_tracker_webscraping_ubuntu.py"),
                # os.path.join(daily_path,"0_daily_prep_votes_snapshot_ubuntu.py"), 
                os.path.join(daily_path,"0_daily_prep_votes_snapshot_ctz_node_ubuntu.py"),
                # os.path.join(daily_path,"0_icon_tracker_webscraping_ubuntu.py"),
                os.path.join(wallet_ranking_path,"exchange_wallet_data_extract_single_api_ubuntu.py"),
                ]

for program in program_list:
    try:
        exec(open(program).read())
    except:
        print("\nSomething went wrong: " + program)
        pass
    print("\nFinished: " + program)
