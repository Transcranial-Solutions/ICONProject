#########################################################################
## Project: Misc.                                                      ##
## Date: February 2022                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Using Geometry's new Icon tracker

# import json library
# from urllib.request import Request, urlopen
import json
import pandas as pd
# from scipy import stats
import numpy as np
import os
from datetime import date, datetime, timedelta
# import glob
from typing import Union
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet
# import math
import requests
import random
from time import sleep
from tqdm import tqdm
from datetime import date, datetime
# from bs4 import BeautifulSoup


# desired_width = 320
# pd.set_option('display.width', desired_width)
# pd.set_option('display.max_columns', 10)
#
# # path to save
# currPath = '/home/tono/ICONProject/data_analysis/'
# inPath = os.path.join(currPath, "output")
#
# resultPath = os.path.join(inPath, "prep_info")
# if not os.path.exists(resultPath):
#     os.mkdir(resultPath)
#
# dataPath = os.path.join(resultPath, "data")
# if not os.path.exists(dataPath):
#     os.mkdir(dataPath)
#
# walletPath = os.path.join(currPath, "wallet")
# if not os.path.exists(walletPath):
#     os.mkdir(walletPath)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ wallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Creating Wallet if does not exist (only done for the first time)
# tester_wallet = os.path.join(walletPath, "test_keystore_1")
#
# if os.path.exists(tester_wallet):
#     wallet = KeyWallet.load(tester_wallet, "abcd1234*")
# else:
#     wallet = KeyWallet.create()
#     wallet.get_address()
#     wallet.get_private_key()
#     wallet.store(tester_wallet, "abcd1234*")
#
# tester_address = wallet.get_address()
#
# SYSTEM_ADDRESS = "cx0000000000000000000000000000000000000000"
#
# # using solidwallet
# icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))


# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return (icx)


def parse_icx(val: str) -> Union[float, int]:
    """
    Attempts to convert a string loop value into icx
        Parameters:
            val (str): Loop value
        Returns:
            (Union[float, int]): Will return the converted value as an int if successful, otherwise it will return NAN
    """
    try:
        return loop_to_icx(int(val, 0))
    except ZeroDivisionError:
        return float("NAN")
    except ValueError:
        return float("NAN")

def distribute_by_sqrt(votes, dividends):
    weights = np.sqrt(votes)
    total_weight = np.sum(weights)
    dividend_shares = (weights / total_weight) * dividends
    return dividend_shares

def distribute_by_modified_sqrt(votes, dividends, bias=1):
    # Add a bias to votes before taking square root to give more to fewer votes
    adjusted_votes = votes + bias  # Increasing the bias gives relatively more to those with fewer votes
    weights = np.sqrt(adjusted_votes)
    total_weight = np.sum(weights)
    dividend_shares = (weights / total_weight) * dividends
    return dividend_shares

def distribute_by_weighted_sqrt(votes, additional_weights, dividends, bias=1):
    # Adjust votes by adding a bias
    adjusted_votes = votes + bias
    # Combine the square root of adjusted votes with the additional weights
    combined_weights = np.sqrt(adjusted_votes) * additional_weights
    total_weight = np.sum(combined_weights)
    dividend_shares = (combined_weights / total_weight) * dividends
    return dividend_shares

def find_by_node_address(node_address, data_list):
    # Iterate over each dictionary in the list
    for entry in data_list:
        # Check if the 'node_address' key matches the provided node address
        if entry.get('node_address') == node_address:
            return entry
    return None  # Return None if no match is found


def timestamp_to_date(df, timestamp, dateformat):
    return pd.to_datetime(df[timestamp] / 1000000, unit='s')#.dt.strftime(dateformat)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX tracker Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
prep_address = 'hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b'
req_preps = requests.get('https://tracker.icon.community/api/v1/governance/preps')
preps_info = json.loads(req_preps.text)
node_detail = find_by_node_address(prep_address, preps_info)

monthly_reward = node_detail.get('reward_monthly')
base_takeaway = monthly_reward*0.7
dividends = monthly_reward-base_takeaway # 5_000

req = requests.get(f'https://tracker.icon.community/api/v1/governance/votes/{prep_address}?limit=100&skip=0')
voter_details = json.loads(req.text)

df_voters = pd.DataFrame(voter_details)
df_voters['votes_in_icx'] = df_voters['value'].apply(loop_to_icx)
df_voters['voter_type'] = np.where(df_voters['address'] == 'hx0452d7441e010a4e09f5f1941f1c398afcd449b9', 'foundation', 'regular')
df_voters_regular = df_voters[df_voters['voter_type'] == 'regular'].reset_index(drop=True)
# df_voters_regular = df_voters_regular[df_voters_regular['votes_in_icx'] >= 1_000].reset_index(drop=True) ## at least 1000 icx in rewards


## calculate weight / inclusion

addresses_to_check = list(df_voters_regular['address'])

voter_block_times = {}

for address in tqdm(addresses_to_check):
    sleep(0.5)  # Throttle request rate
    # Make the request
    req_individual = requests.get(
        f'https://tracker.icon.community/api/v1/transactions/address/{address}?limit=10&skip=0')
    # Parse the JSON response
    voter_recent_tx = json.loads(req_individual.text)

    # Check if there is at least one transaction in the response
    if voter_recent_tx and len(voter_recent_tx) > 0:
        # Store data in the dictionary using the address as a key
        voter_block_times[address] = {
            'block_timestamp': voter_recent_tx[0].get('block_timestamp'),
            'block_number': voter_recent_tx[0].get('block_number')
        }


df_voter_info = pd.DataFrame.from_dict(voter_block_times, orient='index').reset_index()
df_voter_info.columns = ['address', 'block_timestamp', 'block_number']
merged_df = pd.merge(df_voters_regular, df_voter_info, on='address', how='left')


today = datetime.utcnow()
merged_df['days_since_last_tx'] = (today - timestamp_to_date(merged_df, 'block_timestamp', "%m/%d/%Y")).dt.days
df_voters_regular = merged_df[merged_df['days_since_last_tx'] <= 10].reset_index(drop=True) ## anyone who has been active in the last 150 days

# df_voters_regular = df_voters_regular[df_voters_regular['votes_in_icx'] >= 1_000].reset_index(drop=True) ## at least 1000 icx in rewards



## TODO: change weights based on activity in the future
df_voters_regular['weights'] = 1 / np.log(df_voters_regular['days_since_last_tx'] + 14 + 1) #1
additional_weights = np.asarray(df_voters_regular['weights'])
votes = np.asarray(df_voters_regular['votes_in_icx'])
# dividend_shares_sqrt = distribute_by_modified_sqrt(votes, dividends, bias=25)
dividend_shares_sqrt = distribute_by_weighted_sqrt(votes, additional_weights, dividends, bias=25)

df_voters_regular['reward_share_monthly'] = dividend_shares_sqrt
df_voters_regular['reward_share_daily'] = df_voters_regular['reward_share_monthly']/30
df_voters_regular['reward_share_weekly'] = df_voters_regular['reward_share_daily']*7

df_voters_regular['reward_share_percentage'] = round(df_voters_regular['reward_share_monthly'] / df_voters_regular['votes_in_icx'], 2)



# today date
# today = date.today()
today = datetime.utcnow()
day_today = today.strftime("%Y-%m-%d")
day_excel = today.strftime("%d/%m/%Y")

df_voters_regular.insert(loc=0, column='date', value=day_excel)

# =============================================================================
# # save as csv
# =============================================================================
# icx_df.to_csv(os.path.join(dataPath, 'prep_info_' + day_today + '.csv'), index=False)

print(' ### Done extracting p-rep info from geometry tracker. ### ')
