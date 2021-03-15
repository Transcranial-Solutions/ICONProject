#########################################################################
## Project: P-REP API extraction                                       ##
## Date: March 2021                                                    ##
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
projectPath = os.path.join(currPath, "06_wallet_ranking")
if not os.path.exists(projectPath):
    os.mkdir(projectPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# using solidwallet
icon_service = IconService(HTTPProvider("https://ctz.solidwallet.io/api/v3"))


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
# loop to icx converter
def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return (icx)
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



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# get preps (1 to 22)
call = CallBuilder().from_(tester_address) \
    .to('cx0000000000000000000000000000000000000000') \
    .params({"startRanking": hex(1), "endRanking": hex(22)}) \
    .method('getPReps') \
    .build()
result = icon_service.call(call)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# extracting each p-rep json data -- this takes awhile

json_details = extract_values(result, 'details')

all_json = []
for k in json_details:
    URL = k
    page = Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        jpage = json.load(urlopen(page))
    except:
        print(k + ': Perhaps HTTP Error')
    try:
        api_endpoint = extract_values(jpage, 'api_endpoint')[0]
    except:
        print(k + "'s API does not exist!")
    all_json.append(api_endpoint)
    print(k + ' is done.')

# adding https:// if doesn't exist
all_json_mod = ['http://' + x if x[0] not in 'https' else x for x in all_json]



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# testing #
test_prep = all_json_mod[0] # try different address here
icon_service = IconService(HTTPProvider(test_prep, 3))


# icon foundation wallet

# get data function
def get_my_values(method, address, output):
    call = CallBuilder().from_(tester_address) \
        .to('cx0000000000000000000000000000000000000000') \
        .params({"address": address}) \
        .method(method) \
        .build()
    result = icon_service.call(call)
    try:
        temp_output = loop_to_icx(int(result[output], 0))
    except:
        temp_output = float("NAN")
    df = {'address': address, output: temp_output}
    return (df)


print(get_my_values("queryIScore", "hx0b047c751658f7ce1b2595da34d57a0e7dad357d", "estimatedICX"))

