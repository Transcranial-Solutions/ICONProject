#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 05:14:12 2022

@author: tono
"""

import os

token_transfer_path = '/home/tono/ICONProject/data_analysis/10_token_transfer'
transaction_data_path = '/home/tono/ICONProject/data_analysis/08_transaction_data'


program_list = [os.path.join(token_transfer_path, "token_transfer_community_ubuntu.py"),
                os.path.join(transaction_data_path,"block_extraction_community_ubuntu.py"), 
                os.path.join(transaction_data_path,"transaction_extraction_ctznode_ubuntu.py"),
                os.path.join(transaction_data_path,"transaction_analysis_ctznode_ubuntu.py"),
                os.path.join(transaction_data_path,"transaction_analysis_ctznode_weekly_ubuntu.py"),
                os.path.join(transaction_data_path,"tx_summary_compiler_ubuntu.py"),
                ]

for program in program_list:
    exec(open(program).read())
    print("\nFinished: " + program)
