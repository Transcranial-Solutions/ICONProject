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
import requests
from bs4 import BeautifulSoup
import os
from datetime import date

# path to save
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outDataPath = os.path.join(inPath, "icon_community")
if not os.path.exists(outDataPath):
    os.mkdir(outDataPath)

# URL to be scraped
URL = 'https://icon.community'
page = requests.get(URL)
soup = BeautifulSoup(page.content, 'html.parser')

# getting data from icon.community - webscraping (data within '<script>')
scripts = soup.find_all('script')
all_vars = scripts[2]

def get_data_from_script(corresponding_number):
    for s in all_vars:
            if corresponding_number == 11 or corresponding_number == 12: # treat 11 and 12 differently
                df = s.split('var ')[corresponding_number].split(' =', 1)[0] +  ',' + \
                          s.split('=')[corresponding_number].split(';', 1)[0]
            else:
                df = "'" + s.split('var ')[corresponding_number].split(' =', 1)[0] + "', " + \
                              s.split('[')[corresponding_number].split(']', 1)[0] # extract from 'script'

            df = pd.DataFrame([x.split(',') for x in df.split('\n')]).T.\
                apply(lambda s:s.str.replace("'", "")) # convert string to dataframe & remove quotes
            df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True) # first row into col name & remove quotes

    return(df)


# getting data from div  -- marketcap, supply etc
div_df = soup.find_all("div", {"class": "numbers"})

def get_data_from_div(corresponding_number):
    df = div_df[corresponding_number].get_text()
    df = pd.DataFrame([df.split('\n')]).T
    df = df.drop(df.index[0]).drop(df.index[3])
    df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)
    return (df)


## dataframes

# transactions
dates_tx = get_data_from_script(1)
data_tx = get_data_from_script(2)

# balace and reward
these_dates = get_data_from_script(4)
balance_wallet = get_data_from_script(5)
total_wallet = get_data_from_script(6)
annual_real_yield = get_data_from_script(7)
annual_reward = get_data_from_script(8)
annual_inflation = get_data_from_script(9)

# staked
total_staked_ICX = get_data_from_script(11)
total_staked_ICX = total_staked_ICX.rename(columns={'total_staked': 'total_staked_ICX'})
total_staked_ICX['total_staked_ICX'] = total_staked_ICX['total_staked_ICX'].astype(int).map('{:,}'.format)

# regular ICX stats
market_cap = get_data_from_div(3)
circulating_supply = get_data_from_div(4)
total_supply = get_data_from_div(5)
public_treasury = get_data_from_div(6)
circulation_staked = get_data_from_div(7)
total_staked = get_data_from_div(8)
total_voted = get_data_from_div(9)

# merge dataframes
tx_df = pd.concat([dates_tx, data_tx], axis=1, sort=False)

wallet_and_reward_df = pd.concat([these_dates, balance_wallet, total_wallet,
                                  annual_real_yield, annual_reward, annual_inflation], axis=1, sort=False)

basic_icx_stat = pd.concat([market_cap, circulating_supply,
                   total_supply, public_treasury,
                   total_staked_ICX, total_staked,
                   circulation_staked, total_voted], axis=1, sort=False)

# today date
today = date.today()
day1 = today.strftime("%d_%m_%Y")

# save as csv
tx_df.to_csv(os.path.join(outDataPath, 'tx_df_' + day1 + '.csv'), index=False)
wallet_and_reward_df.to_csv(os.path.join(outDataPath, 'wallet_and_reward_df_' + day1 + '.csv'), index=False)
basic_icx_stat.to_csv(os.path.join(outDataPath, 'basic_icx_stat_df_' + day1 + '.csv'), index=False)


