#########################################################################
## Project: ICON in Numbers                                            ##
## Date: July 2020                                                     ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# webscraping icon.community and save into csv

import pandas as pd
import os
import glob
import re

# path to save
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outDataPath = os.path.join(inPath, "icon_community")
if not os.path.exists(outDataPath):
    os.mkdir(outDataPath)

listData = glob.glob(os.path.join(outDataPath, "basic*.csv"))


all_df =[]
for k in range(len(listData)):
    this_date = re.findall(r'stat_df_(.+?).csv', listData[k])[0].replace("_", "/")
    df = pd.read_csv(listData[k]).head(1)
    df['date'] = this_date
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    all_df.append(df)

df = pd.concat(all_df)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
df.index = pd.DatetimeIndex(df['date']).floor('D')
all_days = pd.date_range(df.index.min(), df.index.max(), freq='D')
df = df.reindex(all_days)
df.reset_index(inplace=True)
df = df.drop(columns='date').rename(columns={'index':'date'})
df['date'] = df['date'].dt.date
df.set_index('date', inplace=True)

# this is stupid..
df['Market Cap (USD)'] = df['Market Cap (USD)'].str.replace(',','').astype('float')
df['Circulating Supply'] = df['Circulating Supply'].str.replace(',','').astype('float')
df['Total Supply'] = df['Total Supply'].str.replace(',','').astype('float')
df['Public Treasury \xa0'] = df['Public Treasury \xa0'].str.replace(',','').astype('float')
df['total_staked_ICX'] = df['total_staked_ICX'].str.replace(',','').astype('float')

df['Total Staked \xa0'] = df['Total Staked \xa0'].str.rstrip('%').astype('float') / 100.0
df['Circulation Staked \xa0'] = df['Circulation Staked \xa0'].str.rstrip('%').astype('float') / 100.0
df['Total Voted \xa0'] = df['Total Voted \xa0'].str.rstrip('%').astype('float') / 100.0

df = df.interpolate()

df['Market Cap (USD)'] = df['Market Cap (USD)'].astype('int')
df['Circulating Supply'] = df['Circulating Supply'].astype('int')
df['Total Supply'] = df['Total Supply'].astype('int')
df['Public Treasury \xa0'] = df['Public Treasury \xa0'].astype('int')
df['total_staked_ICX'] = df['total_staked_ICX'].astype('int')

df.to_csv(os.path.join(outDataPath, 'basic_icx_stat_compiled.csv'), index=False)


