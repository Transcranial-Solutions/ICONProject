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
import math
import requests
from bs4 import BeautifulSoup


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# path to save
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")

resultPath = os.path.join(inPath, "prep_info")
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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX tracker Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


req = requests.get('https://tracker.icon.community/api/v1/preps')
# jtracker_url = BeautifulSoup(req.text, 'html.parser')
jtracker_url = json.loads(req.text)

# tracker_url = Request('https://tracker.icon.community/api/v1/preps', headers={'User-Agent': 'Mozilla/5.0'})
# jtracker_url = json.load(urlopen(tracker_url))


icx_df = pd.DataFrame(jtracker_url)
icx_df['iglobal'] = iglobal

first_column = icx_df.pop('name')
icx_df.insert(0, 'name', first_column)



# bond and power
icx_df['bonded'] = icx_df['bonded'].astype(float)
icx_df['bonded'] = loop_to_icx(icx_df['bonded'])

icx_df['power'] = icx_df['power'].astype(float)
icx_df['power'] = loop_to_icx(icx_df['power'])

icx_df['bond_percentage'] = icx_df['bonded'] / icx_df['delegated']
icx_df['bond_percentage'] = icx_df['bond_percentage'].apply('{:,.2%}'.format)

icx_df = icx_df.sort_values(by=['delegated', 'bonded', 'name'], ascending=False)
icx_df['rank_by_delegation'] = np.arange(len(icx_df)) + 1

icx_df = icx_df.sort_values(by=['bonded', 'delegated'], ascending=False)
icx_df['rank_by_bond'] = np.arange(len(icx_df)) + 1

icx_df = icx_df.reset_index(drop=True)


# today date
# today = date.today()
today = datetime.utcnow()
day_today = today.strftime("%Y-%m-%d")
day_excel = today.strftime("%d/%m/%Y")

icx_df.insert(loc=0, column='date', value=day_excel)

# save as csv
icx_df.to_csv(os.path.join(dataPath, 'prep_info_' + day_today + '.csv'), index=False)

print(' ### Done extracting p-rep info from geometry tracker. ### ')