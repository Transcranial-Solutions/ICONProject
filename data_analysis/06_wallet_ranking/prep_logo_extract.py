#########################################################################
## Project: P-REP logo extraction                                      ##
## Date: May 2021                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

import pandas as pd
import os

from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from iconsdk.builder.call_builder import CallBuilder
from urllib.request import Request, urlopen
import json

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

currPath = os.getcwd()


walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# using solidwallet
default_ctz_api = "https://ctz.solidwallet.io/api/v3"
icon_service = IconService(HTTPProvider(default_ctz_api))


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


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# extracting each p-rep json data -- need to feed in address

# get data function
def get_my_values(method, address):
    call = CallBuilder().from_(tester_address) \
        .to('cx0000000000000000000000000000000000000000') \
        .params({"address": address}) \
        .method(method) \
        .build()
    result = icon_service.call(call)

    json_details = extract_values(result, 'details')[0]
    page = Request(json_details, headers={'User-Agent': 'Mozilla/5.0'})
    jpage = json.load(urlopen(page))

    try:
        logo_link = extract_values(jpage, 'logo_256')[0]
    except:
        print(json_details + ': Perhaps No logo uploaded by the P-Rep')

    print(address + ' is done.')

    return(logo_link)


# example
get_my_values(method='getPRep', address='hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b')