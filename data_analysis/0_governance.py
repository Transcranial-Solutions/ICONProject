#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 09:47:52 2023

@author: tono
"""
import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta


today = datetime.utcnow() #- timedelta(days=1)
day_today = today.strftime("%Y-%m-%d")
day1 = today.strftime("%Y_%m_%d")
this_year = day1[0:4]

# making path for saving
currPath = '/home/tono/ICONProject/data_analysis/'
inPath = '/home/tono/ICONProject/data_analysis/output/'
prepvotePath = os.path.join(inPath, "prep_votes")
savePath = os.path.join(prepvotePath, this_year)

fn = f'prep_top_100_votes_and_bond_{day1}.csv'


df = pd.read_csv(os.path.join(savePath, fn), low_memory=False)

df['bond_status'] = np.where(df['bond'] != 0, 'Bonded', 'Not bonded')

df.groupby(['bond_status'])['prep_type'].value_counts()

df.groupby(['bond_status','prep_type']).agg(['count'])
