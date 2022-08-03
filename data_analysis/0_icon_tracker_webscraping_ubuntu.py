#########################################################################
## Project: Misc.                                                      ##
## Date: December 2021                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# High-level information from ICX tracker

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
from scipy import stats
import numpy as np
import os
from datetime import date, datetime, timedelta
import glob
from typing import Union
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet
import random
from time import sleep

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# path to save
currPath = '/home/tono/ICONProject/data_analysis/'

inPath = os.path.join(currPath, "output")

resultPath = os.path.join(inPath, "icon_tracker")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

dataPath = os.path.join(resultPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ wallet ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Creating Wallet if does not exist (only done for the first time)
tester_wallet = os.path.join(walletPath, "test_keystore_1")

if os.path.exists(tester_wallet):
    wallet = KeyWallet.load(tester_wallet, "abcd1234*")
else:
    wallet = KeyWallet.create()
    wallet.get_address()
    wallet.get_private_key()
    wallet.store(tester_wallet, "abcd1234*")

tester_address = wallet.get_address()

SYSTEM_ADDRESS = "cx0000000000000000000000000000000000000000"

# using solidwallet
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))

# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return(icx)

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

def hex_to_int(val: str) -> Union[int, float]:
    """
    Attempts to convert a string based hex into int
        Parameters:
            val (str): Value in hex
        Returns:
            (Union[int, float]): Returns the value as an int if successful, otherwise a float NAN if not
    """
    try:
        return int(val, 0)
    except ValueError:
        print(f"failed to convert {val} to int")
        return float("NAN")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ iiss info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# get iiss info
def get_iiss_info():
    call = CallBuilder().from_(tester_address) \
        .to(SYSTEM_ADDRESS) \
        .method("getIISSInfo") \
        .build()
    result = icon_service.call(call)['variable']

    df = {'Icps': hex_to_int(result['Icps']), 'Iglobal': parse_icx(result['Iglobal']),
          'Iprep': hex_to_int(result['Iprep']),
          'Irelay': hex_to_int(result['Irelay']), 'Ivoter': hex_to_int(result['Ivoter'])}

    return df

iglobal = get_iiss_info()['Iglobal']
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#



## value extraction function
def extract_values(obj, key):
  # Pull all values of specified key from nested JSON
  arr = []

  def extract(obj, arr, key):

    # Recursively search for values of key in JSON tree
    if isinstance(obj, dict):
      for k, v in obj.items():
        if isinstance(v, (dict, list)):
          extract(v, arr, key)
        elif k == key:
          arr.append(v)
    elif isinstance(obj, list):
      for item in obj:
        extract(item, arr, key)
    return arr

  results = extract(obj, arr, key)
  return results


def request_sleep_repeat(url, repeat=3):
    for i in range(0,repeat):
        print(f"Trying {str(i)}...")
        try:
            # this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
            known_address_url = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            random_sleep_except = random.uniform(3,6)
            print("Just pausing for " + str(random_sleep_except) + " seconds and try again \n")
            sleep(random_sleep_except)
            
        except:
            random_sleep_except = random.uniform(30,60)
            print("I've encountered an error! I'll pause for " + str(random_sleep_except) + " seconds and try again \n")
            sleep(random_sleep_except)
    return known_address_url
    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX tracker Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# tracker_url = Request('https://tracker.icon.foundation/v3/main/mainInfo', headers={'User-Agent': 'Mozilla/5.0'})
tracker_url = request_sleep_repeat(url = 'https://tracker.icon.foundation/v3/main/mainInfo', repeat=3)
jtracker_url = json.load(urlopen(tracker_url))

def extract_json(json_dict):
    marketCap = extract_values(json_dict, 'marketCap')
    icxSupply = extract_values(json_dict, 'icxSupply')
    icxCirculationy = extract_values(json_dict, 'icxCirculationy')
    publicTreasury = extract_values(json_dict, 'publicTreasury')

    # combining strings into list
    icxdata = {'marketCap': marketCap,
         'icxSupply': icxSupply,
         'icxCirculation': icxCirculationy,
         'publicTreasury': publicTreasury}

    # convert into dataframe
    return(pd.DataFrame(data=icxdata))



icx_df = extract_json(jtracker_url)
icx_df['iglobal'] = iglobal

# today date
# today = date.today()
today = datetime.utcnow()
day_today = today.strftime("%Y-%m-%d")
day_excel = today.strftime("%d/%m/%Y")

icx_df.insert(loc=0, column='date', value=day_excel)

# =============================================================================
# # save as csv
# =============================================================================
icx_df.to_csv(os.path.join(dataPath, 'basic_icx_stat_df_' + day_today + '.csv'), index=False)

listData = glob.glob(os.path.join(dataPath, "basic*.csv"))


all_df =[]
for k in range(len(listData)):
    df = pd.read_csv(listData[k]).head(1)
    all_df.append(df)

df = pd.concat(all_df)
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

def interploate_data(df):
    df.index = pd.DatetimeIndex(df['date']).floor('D')
    all_days = pd.date_range(df.index.min(), df.index.max(), freq='D')
    df = df.reindex(all_days)
    df.reset_index(inplace=True)
    df = df.drop(columns='date').rename(columns={'index':'date'})
    df['date'] = df['date'].dt.date
    df.set_index('date', inplace=True)

    # interpolate
    df = df.interpolate()

    # putting back the date
    df = df.reset_index()
    return df

df = interploate_data(df)


# =============================================================================
# # save
# =============================================================================
df.to_csv(os.path.join(resultPath, 'icx_tracker_compiled.csv'), index=False)


old_df = pd.read_csv(os.path.join(inPath, 'icon_community/icx_stat_compiled.csv'))
old_df = old_df.drop(columns=['Total Staked \xa0','Circulation Staked \xa0','Total Voted \xa0','total_staked_ICX']).rename(columns={'Market Cap (USD)':'marketCap', 'Total Supply': 'icxSupply', 'Circulating Supply':'icxCirculation', 'Public Treasury \xa0': 'publicTreasury'})
new_df = old_df.append(df).reset_index(drop=True)
new_df = new_df[(np.abs(stats.zscore(new_df['marketCap'])) < 3)]
new_df = interploate_data(new_df)

# =============================================================================
# # save
# =============================================================================
new_df.to_csv(os.path.join(resultPath, 'icx_tracker_compiled_all.csv'), index=False)
