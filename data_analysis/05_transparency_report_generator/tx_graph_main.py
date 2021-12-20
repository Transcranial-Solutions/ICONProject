
#########################################################################
## Project: Network Graph on ICON Network                              ##
## Date: March 2021                                                    ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Webscraping

# This file extracts information from Blockmove's Iconwatch website (https://iconwat.ch/address/).

# Extract
# Data put together in a DataFrame, saved separately in CSV format.

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
from datetime import date, datetime, timedelta
from time import time
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "output")
if not os.path.exists(outPath):
    os.mkdir(outPath)

# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(1)
    if string:
        return yesterday.strftime('%Y_%m_%d')
    return yesterday

def day_to_text(day):
    return day.replace("_", "-")

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


# today's date
date_is_range = 1 # if date is range (1) or is one  date (0)
use_specified_date = 0 # yes(1) no(0)

# date is range
if date_is_range == 1:
    day_1 = "2021_12_01"; day_2 = "2021_12_18"
    day_1_text = day_to_text(day_1); day_2_text = day_to_text(day_2)
    date_of_interest = pd.date_range(start=day_1_text, end=day_2_text, freq='D').strftime("%Y-%m-%d").to_list()

# specified date
if date_is_range == 0 and use_specified_date == 1:
   day_1 = "2021_06_14"
   date_of_interest = [day_to_text(day_1)]

# today
if date_is_range == 0 and use_specified_date == 0:
   today = datetime.utcnow()
   day_1 = today.strftime("%Y_%m_%d")
   date_of_interest = [day_to_text(day_1)]


api_endpoint = "http://icon.everstake.one:9000"
if 'http' not in api_endpoint:
    api_endpoint_mod = 'http://' + api_endpoint
elif 'https' not in api_endpoint:
    api_endpoint_mod = 'https://' + api_endpoint

# wallet of interest
this_address = 'hxc4193cda4a75526bf50896ec242d6713bb6b02a3' # Binance Hot
# this_address = 'hx1729b35b690d51e9944b2e94075acff986ea0675' # Binance cold
# this_address = 'hx54d6f19c3d16b2ef23c09c885ca1ba776aaa80e2' #ubik
# this_address = 'hxd0d9b0fee857de26fd1e8b15209ca15b14b851b2' #velic
# this_address = 'hxa224bb59e9ba930f3919b57feef7656f1139d24b' # catalyst
# this_address = 'hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b'# transcranial sol
# this_address = 'hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310' # upbit 1

# this_address = 'cx0000000000000000000000000000000000000000' # governance
# this_address = 'hxf1ea3eb337432bddb4e01e0b926c671eff297af9'
# this_address = 'hx81d4f834b91569b43cde903ec241eb1fce64a171'
# this_address = 'cx14002628a4599380f391f708843044bc93dce27d' # iAM
# this_address = 'hxe701834d9a55b1e9de0e1d2ee349bee77e50025a'
# this_address = 'hxff1c8ebad1a3ce1ac192abe49013e75db49057f8' #velic_stav
# this_address = 'hxa390d24fdcba68515b492ffc553192802706a121'

# number of wallets to determined exchange wallet (per 100 page, per date)
NW_EW = 15
rm_exch = 0
rm_known = 0
# from wallet or into the wallet
tx_flow = 'both' # 'in', 'out', 'both'
tx_type = 'normal' # 'normal', 'internal', 'contract', 'token (individual wallet)', 'token_txlist (token that has been xferred), 'token_list'

# does not work with range
first_degree = 1 # this is for getting only 1 interaction (WOI <-> wallet_x)
further_degree = 0 # this is for the next and beyond (so if it's 1, it means 2 in total)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX Address Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
known_address_url = Request('https://iconwat.ch/data/thes', headers={'User-Agent': 'Mozilla/5.0'})
jknown_address = json.load(urlopen(known_address_url))

def add_dict_if_noexist(key, d, value):
    if key not in d:
        d[key] = value

# add any known addresses here manually (if not exist)
add_dict_if_noexist('hx02dd8846baddc171302fb88b896f79899c926a5a', jknown_address, 'ICON_Vote_Monitor')
add_dict_if_noexist('hxa527f96d8b988a31083167f368105fc0f2ab1143', jknown_address, 'binance_us')

add_dict_if_noexist('hx6332c8a8ce376a5fc7f976d1bc4805a5d8bf1310', jknown_address, 'upbit_hot1')
add_dict_if_noexist('hxfdb57e23c32f9273639d6dda45068d85ee43fe08', jknown_address, 'upbit_hot2')
add_dict_if_noexist('hx4a01996877ac535a63e0107c926fb60f1d33c532', jknown_address, 'upbit_hot3')
add_dict_if_noexist('hx8d28bc4d785d331eb4e577135701eb388e9a469d', jknown_address, 'upbit_hot4')
add_dict_if_noexist('hxf2b4e7eab4f14f49e5dce378c2a0389c379ac628', jknown_address, 'upbit_hot5')

add_dict_if_noexist('hx6eb81220f547087b82e5a3de175a5dc0d854a3cd', jknown_address, 'bithumb_1')
add_dict_if_noexist('hx0cdf40498ef03e6a48329836c604aa4cea48c454', jknown_address, 'bithumb_2')
add_dict_if_noexist('hx6d14b2b77a9e73c5d5804d43c7e3c3416648ae3d', jknown_address, 'bithumb_3')

add_dict_if_noexist('hxa390d24fdcba68515b492ffc553192802706a121', jknown_address, 'bitvavo_hot')
add_dict_if_noexist('hxa390d24fdcba68515b492ffc553192802706a121', jknown_address, 'bitvavo_cold')

add_dict_if_noexist('hx85532472e789802a943bd34a8aeb86668bc23265', jknown_address, 'unkEx_c1')
add_dict_if_noexist('hx94a7cd360a40cbf39e92ac91195c2ee3c81940a6', jknown_address, 'unkEx_c2')

add_dict_if_noexist('hxe5327aade005b19cb18bc993513c5cfcacd159e9', jknown_address, 'unkEx_d1')

add_dict_if_noexist('hxddec6fb21f9618b537e930eaefd7eda5682d9dc8', jknown_address, 'ex_intermediary')

add_dict_if_noexist('hx294c5d0699615fc8d92abfe464a2601612d11bf7', jknown_address, 'funnel_1')
add_dict_if_noexist('hx44c0d5fab0c81fe01a052f5ffb83fd152e505202', jknown_address, 'facilitator_1')


# add_dict_if_noexist('hx7a649b6b2d431849fd8e3db2d4ed371378eacf78', jknown_address, 'icf_related1')
# add_dict_if_noexist('hx63862927a9c1389e277cd20a6168e51bd50af13e', jknown_address, 'icf_related2')
# add_dict_if_noexist('hx294c5d0699615fc8d92abfe464a2601612d11bf7', jknown_address, 'funnel_a1')
# add_dict_if_noexist('hxc8377a960d4eb484a3b8a733012995583dda0813', jknown_address, 'easy_crypto')


add_dict_if_noexist('hx9d9ad1bc19319bd5cdb5516773c0e376db83b644', jknown_address, 'icf_delegate_1')
add_dict_if_noexist('hx0cc3a3d55ed55df7c8eee926a4fafb5412d0cca4', jknown_address, 'icf_delegate_2')
add_dict_if_noexist('hxa9c54005bfa47bb8c3ff0d8adb5ddaac141556a3', jknown_address, 'icf_delegate_3')
add_dict_if_noexist('hxc1481b2459afdbbde302ab528665b8603f7014dc', jknown_address, 'icf_delegate_4')

add_dict_if_noexist('hx02ada5d31e0f3eafa7fdf069caebb8d9ac7272f7', jknown_address, 'icf_delegate_5')
add_dict_if_noexist('hxf936493f53a45cfcf3bc1d35643dfda051f9534f', jknown_address, 'icf_delegate_6')
add_dict_if_noexist('hx2d84173c2cfb0e5c4ca31fbd8e5df252fcad0992', jknown_address, 'icf_delegate_7')
add_dict_if_noexist('hx5620c34cc9bc6e56f4feaf4d81cbe69535221237', jknown_address, 'icf_delegate_8')

# binance sweepers
add_dict_if_noexist('hx8a50805989ceddee4341016722290f13e471281e', jknown_address, 'binance\nsweeper_01')
add_dict_if_noexist('hx58b2592941f61f97c7a8bed9f84c543f12099239', jknown_address, 'binance\nsweeper_02')
add_dict_if_noexist('hx49c5c7eead084999342dd6b0656bc98fa103b185', jknown_address, 'binance\nsweeper_03')
add_dict_if_noexist('hx56ef2fa4ebd736c5565967197194da14d3af88ca', jknown_address, 'binance\nsweeper_04')
add_dict_if_noexist('hxe295a8dc5d6c29109bc402e59394d94cf600562e', jknown_address, 'binance\nsweeper_05')
add_dict_if_noexist('hxa479f2cb6c201f7a63031076601bbb75ddf78670', jknown_address, 'binance\nsweeper_06')
add_dict_if_noexist('hx538de7e0fc0d312aa82549aa9e4daecc7fabcce9', jknown_address, 'binance\nsweeper_07')
add_dict_if_noexist('hxc20e598dbc78c2bfe149d3deddabe77a72412c92', jknown_address, 'binance\nsweeper_08')
add_dict_if_noexist('hx5fd21034a8b54679d636f3bbff328df888f0fe28', jknown_address, 'binance\nsweeper_09')
add_dict_if_noexist('hxa94e9aba8830f7ee2bace391afd464417284c430', jknown_address, 'binance\nsweeper_10')
add_dict_if_noexist('hxa3453ab17ec5444754cdc5d928be8a49ecf65b22', jknown_address, 'binance\nsweeper_11')
add_dict_if_noexist('hx8c0a2f8ca8df29f9ce172a63bf5fd8106c610f42', jknown_address, 'binance\nsweeper_12')
add_dict_if_noexist('hx663ff2514ece0de8c3ecd76f003a9682fdc1fb00', jknown_address, 'binance\nsweeper_13')
add_dict_if_noexist('hx0232d71f68846848b00b4008771be7b0527fbb39', jknown_address, 'binance\nsweeper_14')
add_dict_if_noexist('hx561485c5ee93cf521332b520bb5d10d9389c8bab', jknown_address, 'binance\nsweeper_15')
add_dict_if_noexist('hx0b75cf8b1cdb81d514e64cacd82ed14674513e6b', jknown_address, 'binance\nsweeper_16')
add_dict_if_noexist('hx02ebb44247de11ab80ace2e3c25ebfbcffe4fa68', jknown_address, 'binance\nsweeper_17')
add_dict_if_noexist('hxc000d92a7d9d316c6acf11a23a4a20030d414ef2', jknown_address, 'binance\nsweeper_18')
add_dict_if_noexist('hx7135ddaeaf43d87ea73cbdd22ba202b13a2caf6a', jknown_address, 'binance\nsweeper_19')
add_dict_if_noexist('hxb2d0da403832f9f94617f5037808fe655434e5b7', jknown_address, 'binance\nsweeper_20')
add_dict_if_noexist('hx387f3016ee2e5fb95f2feb5ba36b0578d5a4b8cf', jknown_address, 'binance\nsweeper_21')
add_dict_if_noexist('hx69221e58dfa8e3688fa8e2ad368d78bfa0fad104', jknown_address, 'binance\nsweeper_22')




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Contract Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# functions to get transactions and the page count needed for webscraping
def get_tx_via_url(tx_type=tx_type,
                   this_address='hx0b047c751658f7ce1b2595da34d57a0e7dad357d',
                   page_count=1,
                   tx_count=1):
    """ This function is used to extract json information from icon page (note that total number of
    tx from the website is 500k max, so we can't go back too much """

    # getting transaction information from icon transaction page
    if tx_type == 'normal':
        link_text = 'txList'
    elif tx_type == 'internal':
        link_text = 'internalTxList'

    # this is token transfer within a wallet
    elif tx_type == 'token':
        link_text = 'tokenTxList'

    if tx_type in ['normal','internal','token']:
        # tx url
        tx_url = Request('https://tracker.icon.foundation/v3/address/'+ link_text + '?page=' + str(page_count) + '&count=' + str(tx_count) + '&address=' + this_address,
                         headers={'User-Agent': 'Mozilla/5.0'})

    # contract information
    elif tx_type in ['contract']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/contract/list?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # list of tokens
    elif tx_type in ['token_list']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/token/list?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # all token transfer (different from main token tx which is on its own)
    elif tx_type in ['token_txlist']:
        tx_url = Request(
            'https://tracker.icon.foundation/v3/token/txList?page=' + str(page_count) + '&count=' + str(tx_count),
            headers={'User-Agent': 'Mozilla/5.0'})

    # extracting info into json
    jtx_url = json.load(urlopen(tx_url))

    return jtx_url

def get_page_tx_count(tx_type=tx_type, this_address=this_address):
    """ This function is to get the number of elements per page and number of page to be extracted """

    if tx_type in ['normal', 'internal']:
        jtx_url = get_tx_via_url(tx_type=tx_type, this_address=this_address, page_count=1, tx_count=1)
        totalSize = extract_values(jtx_url, 'totalSize')

    elif tx_type in ['contract','token_txlist']:
        jtx_url = get_tx_via_url(tx_type=tx_type, page_count=1, tx_count=1)
        totalSize = extract_values(jtx_url, 'listSize')

    # to get the tx count (loading how many elements in a page, and how many pages there are) for web scraping
    if totalSize[0] != 0:

        # get page count to loop over
        if totalSize[0] > 100:
            tx_count = 100
            page_count = round((totalSize[0] / tx_count) + 0.5)
        else:
            tx_count = totalSize[0]
            page_count = 1

    elif totalSize[0] == 0:
        page_count=0
        tx_count=0
        print("No records found!")

    return page_count, tx_count


# getting contract information from icon transaction page
page_count = []
tx_count = []
page_count, tx_count = get_page_tx_count(tx_type='contract')


# collecting contact info using multithreading
start = time()

known_contract_all = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(0, page_count):
        known_contract_all.append(executor.submit(get_tx_via_url, tx_type='contract', page_count=k+1, tx_count=tx_count))

temp_df = []
for task in as_completed(known_contract_all):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')

# extracting information by labels
contract_address = extract_values(temp_df, 'address')
contract_name = extract_values(temp_df, 'contractName')

# converting list into dictionary
def Convert(lst1, lst2):
    res_dct = {lst1[i]: lst2[i] for i in range(0, len(lst1), 1)}
    return res_dct

contract_d = Convert(contract_address, contract_name)

# updating known address with other contract addresses
jknown_address.update(contract_d)

# updating contact address
jknown_address['cxb0b6f777fba13d62961ad8ce11be7ef6c4b2bcc6'] = 'ICONbet \nDAOdice (new)'
jknown_address['cx38fd2687b202caf4bd1bda55223578f39dbb6561'] = 'ICONbet \nDAOlette (new)'
jknown_address['cx5d6e1482434085f30660efe30573304d629270e5'] = 'ICONbet \nBaccarat'
jknown_address['cx3b9955d507ace8ac27080ed64948e89783a62ab1'] = 'ICONbet \nReward'
jknown_address['cx1b97c1abfd001d5cd0b5a3f93f22cccfea77e34e'] = 'ICONbet \nGame Contract'
jknown_address['cxc6bb033f9d0b2d921887040b0674e7ceec1b769c'] = 'Lossless Lottery'
jknown_address['cx14002628a4599380f391f708843044bc93dce27d'] = 'iAM Div'
jknown_address['cx75e584ffe40cf361b3daa00fa6593198d47505d5'] = 'TAP Div'
jknown_address['cxff66ea114d20f6518e89f1269b4a31d3620b9331'] = 'PGT Distro'
jknown_address['cx953260a551584681e1f0492dce29e07d323ed5a6'] = 'ICONPOOL'

# making same table but with different column names
known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['dest_address', 'dest_def'])
known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from_address', 'from_def'])

known_address_exception = known_address_details_from[~known_address_details_from['from_def'].str.startswith('binance\nsweeper', na=False)]




#~~~~~~~~~~~~~~~~~~~~~~~~~~ Collecting data that meets date criteria Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# collects data that meets date criteria
def data_meeting_date_criteria(tx_type=tx_type, date_of_interest=date_of_interest, this_address=this_address,
                               page_count=page_count, tx_count=tx_count):
    """
    :param date_of_interest: date could be range or a specific date
    :return: appended data that meets the criteria -- requires loop
    """
    if tx_type in ['normal', 'internal']:
        jtx_url = get_tx_via_url(tx_type=tx_type, this_address=this_address, page_count=page_count, tx_count=tx_count)
        createDateTime = 'createDateTime'
        totalSize = 'totalSize'

    elif tx_type in ['token_txlist']:
        jtx_url = get_tx_via_url(tx_type=tx_type, page_count=page_count, tx_count=tx_count)
        createDateTime = 'age'
        totalSize = 'listSize'

    if jtx_url[totalSize] != 0:
        # changing datetime to date
        for d in jtx_url['data']:
            if tx_type in ['token_txlist']:
                d['createDateTime'] = d[createDateTime]
                d['createDate'] = d['createDateTime']
            elif tx_type in ['normal', 'internal']:
                d['createDateTime'] = d['createDate']

            if d['createDate']:
                date = d['createDate'].split('T', 1)[0]
                d['createDate'] = date

        first_createDate = jtx_url['data'][0]['createDate']
        # print(first_createDate)

        # collecting data that meets criteria -- given date/range and state == 1 (successful tx)
        my_item = []
        for item in jtx_url['data']:
            if item['createDate'] in date_of_interest and item['state'] == 1:
                my_item.append(item)
        return first_createDate, my_item

# collecting data
def collect_data(tx_type=tx_type, date_of_interest=date_of_interest, this_address=this_address):
    """ collects data and breaks loop if the date is before the date of interest (so it doesn't loop over all the data available) """

    page_count, tx_count = get_page_tx_count(tx_type=tx_type, this_address=this_address)
    if page_count != 0:
        tx_all = []
        for k in range(0, page_count):
            first_createDate, my_item = data_meeting_date_criteria(tx_type=tx_type,
                                                                    date_of_interest=date_of_interest,
                                                                    this_address=this_address,
                                                                    page_count=k+1,
                                                                    tx_count=tx_count)

            ## overall collection as it loops
            # if there's data then add to tx_all
            # if len(my_item) != 0:
            if any(my_item):

                tx_all.append(my_item)

            # if first date in the page is after the date of interest, then break of loop
            if date_of_interest[0] > first_createDate:
                break

        # in case there is no data then it'll skip all the processing
        if len(tx_all) != 0:

            # if normal of internal transactions
            if tx_type in ['normal', 'internal']:
                # extracting transaction information by labels
                tx_from_address = extract_values(tx_all, 'fromAddr')
                tx_to_address = extract_values(tx_all, 'toAddr')
                tx_date = extract_values(tx_all, 'createDate')
                tx_datetime = extract_values(tx_all, 'createDateTime')
                tx_amount = extract_values(tx_all, 'amount')
                # tx_fee = extract_values(tx_all, 'fee')
                tx_state = extract_values(tx_all, 'state')

                # combining lists
                combined = {'from_address': tx_from_address,
                            'dest_address': tx_to_address,
                            'date': tx_date,
                            'datetime': tx_datetime,
                            'amount': tx_amount,
                            # 'fee': tx_fee,
                            'state': tx_state}

            # if token tx
            if tx_type in ['token_txlist']:
                # extracting transaction information by labels
                tx_from_address = extract_values(tx_all, 'fromAddr')
                tx_to_address = extract_values(tx_all, 'toAddr')
                tx_date = extract_values(tx_all, 'createDate')
                tx_datetime = extract_values(tx_all, 'createDateTime')
                tx_amount = extract_values(tx_all, 'quantity')
                tx_fee = extract_values(tx_all, 'fee')
                symbol = extract_values(tx_all, 'symbol')
                token_name = extract_values(tx_all, 'tokenName')
                tx_state = extract_values(tx_all, 'state')

                # combining lists
                combined = {'from_address': tx_from_address,
                            'dest_address': tx_to_address,
                            'date': tx_date,
                            'datetime': tx_datetime,
                            'amount': tx_amount,
                            'fee': tx_fee,
                            'symbol': symbol,
                            'token_name': token_name,
                            'state': tx_state}

            # convert into dataframe
            combined_df = pd.DataFrame(data=combined)

            # removing failed transactions & drop 'state (state for transaction success)'
            combined_df = combined_df[combined_df.state != 0].drop(columns='state')

            return combined_df

# attach known wallet info to dataframe
def get_wallet_info(df,
                    known_address_details_from=known_address_details_from,
                    known_address_details_to=known_address_details_to):
    # if len(df) != 0:
    if any(df):
        """ attaching known address information to the data frame"""
        # attaching address info (definition)
        temp_df = pd.merge(df, known_address_details_from, how='left', on='from_address')
        temp_df = pd.merge(temp_df, known_address_details_to, how='left', on='dest_address')
        return temp_df

# getting unique address from the input data
def get_unique_addresses(df, tx_flow=tx_flow,
                         known_address_exception=known_address_exception,
                         this_address=this_address):
    if any(df):
        temp_df = get_wallet_info(df)

        # this is for in and out
        if tx_flow == 'both':
            u_address = pd.Series(temp_df['dest_address'].append(temp_df['from_address']).unique())

        # this is for out
        elif tx_flow == 'out':
            temp_df = temp_df[temp_df.dest_address != this_address]
            u_address = pd.Series(temp_df['dest_address'].unique())

        # this is for in
        elif tx_flow == 'in':
            temp_df = temp_df[temp_df.from_address != this_address]
            u_address = pd.Series(temp_df['from_address'].unique())

        u_address = u_address.unique().tolist()

        # removing known address from the list -- putting back current address in case its one of the known list
        # known_address = known_address_exception['from_address'].to_list()
        # u_address = u_address[~u_address.isin(known_address)].append(pd.Series([this_address])).unique().tolist()
        return(u_address)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Token transfer Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# getting token_txlist information from icon transaction page
# page_count = []
# tx_count = []
# page_count, tx_count = get_page_tx_count(tx_type='token_txlist')
# extract data within the date of interest

if tx_type in ['token_txlist']:
    token_xfer_df = collect_data(tx_type='token_txlist', date_of_interest=date_of_interest)
    after_destination_all = get_wallet_info(token_xfer_df)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Transaction Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# this is for regular tx and interal tx
# after_destination_all_tx = []
# after_destination_normal_tx = []
# after_destination_internal_tx = []
#
#

# tx_type = 'normal' #normal, internal
# tx_flow = 'out' #both, out, in

# if tx type is not token
if tx_type in ['normal', 'internal']:

    # getting transaction information from icon transaction page
    page_count = []
    tx_count = []
    page_count, tx_count = get_page_tx_count(tx_type=tx_type, this_address=this_address)

    # extract data within the date of interest
    collected_df = collect_data(tx_type=tx_type,
                               date_of_interest=date_of_interest,
                               this_address=this_address)


    # add condition here if exist collected df
    if collected_df is not None:

        if first_degree == 0:

            # get exchange wallets
            def get_exchange_wallets(df, NW_EW=NW_EW, from_to_address='from_address'):
                """ used for outputting potential wallet exchanges -- number of wallets transacted set by NW_EW """
                if any(df):
                    df = df[df['from_address'].str.startswith('hx')].reset_index(drop=True)
                    df = df[df['dest_address'].str.startswith('hx')].reset_index(drop=True)
                    wallet_exch = df.groupby([from_to_address, 'date']).count().reset_index()
                    wallet_exch = wallet_exch[wallet_exch['amount'] > NW_EW]
                    wallet_exch = wallet_exch[from_to_address].to_list()
                    return wallet_exch

            # FIRST running address
            # unique addresses found in the initial data -- this would be 'first degree' data
            running_addresses = get_unique_addresses(df=collected_df, tx_flow=tx_flow)
            # option to add known addresses
            if rm_known == 1:
                known_address = known_address_exception['from_address'].to_list()
                running_addresses = list(set(running_addresses) - set(known_address))

            from_wallet_exch = get_exchange_wallets(collected_df, NW_EW, from_to_address='from_address')
            to_wallet_exch = get_exchange_wallets(collected_df, NW_EW, from_to_address='dest_address')
            possible_exchange_wallets = sorted(np.unique(from_wallet_exch + to_wallet_exch))

            if rm_exch == 1:
                # remove those addresses that look like exchanges
                running_addresses = list(set(running_addresses) - set(possible_exchange_wallets))

            collected_df = []

            # get wallet address to loop
            def get_wallet_addresses_to_loop(df, tx_type=tx_type, tx_flow=tx_flow, NW_EW=NW_EW, running_addresses=running_addresses, rm_exch=0, rm_known=0):
                """ if unknown address, it will find those addresses so that they can be added to extract data """
                # if len(df) != 0:
                if any(df):

                    # attaching address info (definition)
                    temp_df = get_wallet_info(df)

                    if tx_type == tx_type:
                        # getting possible exchange wallet addresses (NW_EW (number of wallets to determined exchange wallet) diff address in 100 pages (per day))
                        from_wallet_exch = get_exchange_wallets(temp_df, NW_EW, from_to_address='from_address')
                        to_wallet_exch = get_exchange_wallets(temp_df, NW_EW, from_to_address='dest_address')

                        possible_exchange_wallets = sorted(np.unique(from_wallet_exch + to_wallet_exch))

                    # removing data depending on in or out flow
                    # nan (or binance sweeper/exception) would continue to loop and add to the list
                    # this is for in and out
                    if tx_flow == 'both':
                        is_nan_in = temp_df['from_def'].isna() | \
                                 temp_df['from_def'].str.startswith(('binance\nsweeper','funnel','facilitator'), na=False)
                        is_nan_out = temp_df['dest_def'].isna() | \
                                 temp_df['dest_def'].str.startswith(('binance\nsweeper','funnel','facilitator'), na=False)
                        add_to_loop = pd.Series(temp_df[is_nan_in].from_address.append(temp_df[is_nan_out].dest_address).unique())

                    # THESE NEED TO BE FIXED -- also need to be dependent on previous running addresses
                    if tx_flow == 'out':
                        temp_df = temp_df[temp_df.dest_address != this_address]
                        is_nan = (temp_df['dest_def'].isna() | temp_df['dest_def'].str.startswith(
                            ('binance\nsweeper','funnel','facilitator'), na=False))
                        add_to_loop = pd.Series(temp_df[is_nan].dest_address.unique())

                    if tx_flow == 'in':
                        temp_df = temp_df[temp_df.from_address != this_address]
                        is_nan = (temp_df['from_def'].isna() | temp_df['from_def'].str.startswith(
                            ('binance\nsweeper','funnel','facilitator'), na=False))# & ~df_after_destination['from_address'].isin([this_address])
                        add_to_loop = pd.Series(temp_df[is_nan].from_address.unique())

                    # adding possible exchange to the addresses we loop through
                    running_addresses.extend(possible_exchange_wallets)
                    running_addresses = list(set(running_addresses))

                    if rm_exch == 1:
                        # remove those addresses that look like exchanges
                        running_addresses = list(set(running_addresses) - set(possible_exchange_wallets))
                    if rm_known == 1:
                        known_address = known_address_exception['from_address'].to_list()
                        running_addresses = list(set(running_addresses) - set(known_address))

                    add_to_loop = add_to_loop[~add_to_loop.isin(running_addresses)].to_list()     # if already exists then do not add

                    return possible_exchange_wallets, add_to_loop

            def collect_data_via_multithreading(running_addresses=running_addresses, tx_type='normal', date_of_interest=date_of_interest):

                # collecting data within date of interest using multithreading
                start = time()
                tx_all = []
                with ThreadPoolExecutor(max_workers=5) as executor:
                    for k in tqdm(running_addresses, desc="workers"):
                        tx_all.append(executor.submit(collect_data, tx_type=tx_type,date_of_interest=date_of_interest, this_address=k))
                print("workers complete")

                temp_df = []
                for task in tqdm(as_completed(tx_all), desc='collecting data...', total=len(running_addresses)):
                    temp_df.append(task.result())
                df = pd.concat(temp_df).drop_duplicates().reset_index(drop=True)
                print(f'Time taken: {time() - start}')

                possible_exchange_wallets, add_to_loop = get_wallet_addresses_to_loop(df, rm_exch=rm_exch, rm_known=rm_known)

                return possible_exchange_wallets, add_to_loop, df

            all_df=[]
            all_possible_exchange_wallets=[]

            running_address_history = running_addresses.copy()
            add_to_loop = running_addresses.copy()

            # adding additional degree
            i = 0
            # while loop to stop collecting when there is no more address to add in loops
            while len(add_to_loop) != 0:
                i = i + 1
                possible_exchange_wallets, add_to_loop, df = collect_data_via_multithreading(running_addresses=add_to_loop,
                                                                                             tx_type='normal', date_of_interest=date_of_interest)
                # adding wallets that have not been found
                add_to_loop = list(set(add_to_loop).difference(running_address_history))

                running_address_history.extend(add_to_loop)
                running_address_history = list(set(running_address_history))
                all_possible_exchange_wallets.extend(possible_exchange_wallets)
                all_df.append(df)

                if further_degree != 0 and i == further_degree:
                    print(i)
                    print(further_degree)
                    break

            after_destination_all = pd.concat(all_df)
            after_destination_all = get_wallet_info(after_destination_all)
            after_destination_all = after_destination_all.drop_duplicates()
            after_destination_all = after_destination_all.sort_values(by=['datetime','from_address','dest_address'])


        # if first degree == 1
        else:
            after_destination_all = get_wallet_info(collected_df)
            all_possible_exchange_wallets=[]
            # this is for in and out
            if tx_flow == 'both':
                after_destination_all = after_destination_all

            # this is for out
            elif tx_flow == 'out':
                after_destination_all = after_destination_all[after_destination_all.dest_address != this_address]

            # this is for in
            elif tx_flow == 'in':
                after_destination_all = after_destination_all[after_destination_all.from_address != this_address]



# if tt == 'normal' and len(after_destination_all) != 0:
#     after_destination_normal_tx = after_destination_all.copy()
#
# if tt == 'internal' and len(after_destination_all) != 0:
#     after_destination_internal_tx = after_destination_all.copy()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Concatenate data together  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# concat_df = []
#
# if len(after_destination_all_tx) != 0:
#     # this needs to be here because it can append two empty dataframes
#     after_destination_all_tx = pd.concat(after_destination_all_tx)
#
# if data_type == 'normal' and len(after_destination_normal_tx) != 0:
#     concat_df = after_destination_normal_tx.copy()
#
# if data_type == 'internal' and len(after_destination_internal_tx) != 0:
#     concat_df = after_destination_internal_tx.copy()
#
# if data_type == 'both' and len(after_destination_all_tx) != 0:
#     concat_df = after_destination_all_tx.copy()

#
#
#
#
# if len(concat_df) != 0:
#
#     if first_degree == 1 and tx_flow == 'out':
#         concat_df = concat_df[concat_df['from_address'] == this_address]
#     elif first_degree == 1 and tx_flow == 'in':
#         concat_df = concat_df[concat_df['dest_address'] == this_address]
#     else:
#         pass
#
#     concat_df = concat_df.drop_duplicates().reset_index(drop=True)

try:
    concat_df = after_destination_all.copy()

    #~~~~~~~~~~ Assign unknown wallet a name (e.g. wallet_1, wallet_2) by appeared date ~~~~~~~~~#
    # get unique address for both from and to transactions
    address_1 = concat_df.rename(columns={"from_address": "address"}).sort_values(by=['address', 'datetime']).\
        groupby(['address']).first().reset_index()[['address', 'datetime']]
    address_2 = concat_df.rename(columns={"dest_address": "address"}).sort_values(by=['address', 'datetime']).\
        groupby(['address']).first().reset_index()[['address', 'datetime']]

    # concatenate & remove duplicates, attach wallet info from iconwatch
    concat_address = pd.concat([address_1, address_2]).sort_values(by=['address', 'datetime']).\
        groupby('address').first().reset_index().sort_values(by='datetime')

    wallet_info = []
    wallet_info = pd.merge(concat_address, known_address_details_from, left_on='address', right_on='from_address', how='left').\
        drop(columns='from_address').rename(columns={"from_def": "wallet_def"})

    # re-ordering to get the wallet of interest to the top
    wallet_info['this_order'] = np.where(wallet_info['address'] == this_address, 1, 2)
    wallet_info = wallet_info.sort_values(by=['this_order', 'datetime']).drop(columns='this_order')

    # if nan, then assign w_001, w_002 etc
    wallet_info['wallet_count'] = 'w_' + wallet_info.groupby(['wallet_def']).cumcount().add(1).astype(str).apply(lambda x: x.zfill(3))
    wallet_info['wallet_def'].fillna(wallet_info.wallet_count, inplace=True)
    wallet_info = wallet_info.drop(columns=['datetime', 'wallet_count'])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # if wallet is known, then use the name, if not, call it WOI (Wallet Of Interest)
    known_address_name = known_address_details_from[known_address_details_from['from_address'] == this_address]
    if len(known_address_name) != 0:
        name_this_address = known_address_name.pop('from_def').iloc[0]
    else:
        name_this_address = 'WOI'
        wallet_info['wallet_def'] = np.where(wallet_info['wallet_def'] == 'w_001', name_this_address, wallet_info['wallet_def'])


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # Add it back to data to be analysed
    concat_df = pd.merge(concat_df, wallet_info, left_on='from_address', right_on='address', how='left').\
        drop(columns=['address', 'from_def']).rename(columns={'wallet_def': 'from_def'})

    concat_df = pd.merge(concat_df, wallet_info, left_on='dest_address', right_on='address', how='left').\
        drop(columns=['address', 'dest_def']).rename(columns={'wallet_def': 'dest_def'})

    concat_df[['amount']] = concat_df[['amount']].apply(pd.to_numeric, errors='coerce', axis=1)

    # edges (for gephi?)
    edges = concat_df.groupby(['from_def', 'dest_def']).from_address.agg('count').reset_index().rename(columns={'from_address': 'weight'})


    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # FIGURE

    import networkx as nx
    from networkx.drawing.nx_agraph import graphviz_layout
    import matplotlib.pyplot as plt
    from matplotlib.pyplot import figure, text
    from matplotlib.offsetbox import AnchoredText


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    if this_address.startswith('hx'):
        wallet_or_contact = ' wallet '
    else:
        wallet_or_contact = ' contract '

    # nodesize_ratio = 5
    # nodesize_ratio = 100

    edge_width_ratio = 5
    # summarising df
    temp_df = concat_df[['from_def', 'dest_def', 'amount']].groupby(['from_def', 'dest_def']).amount.agg(['sum', 'count']).reset_index()
    temp_df = temp_df.rename(columns={'sum': 'amount', 'count': 'weight'})
    temp_df['weight_ratio'] = temp_df['weight'] / edge_width_ratio  # to change width of edges based on number of tx
    temp_df['color'] = np.where(temp_df['from_def'] == name_this_address, 'r', 'g')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # node size
    nodesize_from = concat_df.rename(columns={'from_def': 'def'}).groupby(['def'])['amount'].sum()
    nodesize_dest = concat_df.rename(columns={'dest_def': 'def'}).groupby(['def'])['amount'].sum()
    nodesize = pd.merge(nodesize_from, nodesize_dest, on='def', how='outer').fillna(0)

    nodesize = nodesize.reset_index()

    if tx_flow == 'both':
        nodesize['total'] = nodesize['amount_y'] - nodesize['amount_x']
        nodesize = nodesize[['def', 'total']]
        wallet_001_amount = nodesize.reset_index(drop=True)
        wallet_001_amount = wallet_001_amount[wallet_001_amount['def'] == name_this_address]
        wallet_001_amount['text'] = wallet_001_amount['total']

    elif tx_flow == 'out':
        nodesize['total'] = nodesize['amount_y']
        wallet_001_amount = nodesize.reset_index(drop=True)
        wallet_001_amount = wallet_001_amount[wallet_001_amount['def'] == name_this_address]
        wallet_001_amount['text'] = wallet_001_amount['amount_x']
        wallet_001_amount = wallet_001_amount.drop(columns=['total','amount_y']).rename(columns={'amount_x':'total'})
        nodesize = nodesize[['def', 'total']]

    elif tx_flow == 'in':
        nodesize['total'] = nodesize['amount_x']
        wallet_001_amount = nodesize.reset_index(drop=True)
        wallet_001_amount = wallet_001_amount[wallet_001_amount['def'] == name_this_address]
        wallet_001_amount['text'] = wallet_001_amount['amount_y']
        wallet_001_amount = wallet_001_amount.drop(columns=['total','amount_x']).rename(columns={'amount_y':'total'})
        nodesize = nodesize[['def', 'total']]


    # for largest node label
    largest_nodesize = nodesize[['def','total']].round(0).sort_values(by=['total','def'], ascending=[False,True]).reset_index(drop=True).head(1)

    # ratio is the 0.01% of the largest node
    nodesize_ratio = largest_nodesize.iloc[0]['total'] * 0.0001


    largest_nodesize['text'] = largest_nodesize['total']
    largest_nodesize['text'] = largest_nodesize['def'] + ': \n' + largest_nodesize['text'].round(0).astype(int).apply(
        '{:,}'.format).astype(str) + ' ICX'
    largest_nodesize = largest_nodesize.drop(columns=['total'])


    # * -1 to get the amount right for visualisation when it's going out of wallet of interest (and not both, not the node size)
    if tx_flow in ['out']:
        if wallet_001_amount.iloc[0]['total'] != 0:
            wallet_001_amount['text'] = wallet_001_amount['text'] * -1

    if wallet_001_amount.iloc[0]['text'] > 0:
        wallet_001_sign = '+'
    elif wallet_001_amount.iloc[0]['text'] <= 0:
        wallet_001_sign = ''

    wallet_001_amount['text'] = name_this_address + ': \n' + wallet_001_sign + wallet_001_amount['text'].round(0).astype(int).apply('{:,}'.format).astype(str) + ' ICX'
    wallet_001_amount = wallet_001_amount[['def', 'text']]

    # appending (remove dups)
    wallet_001_amount = wallet_001_amount.append(largest_nodesize).drop_duplicates().reset_index(drop=True)

    # node label final
    node_label = []
    node_label = nodesize.drop(columns=['total'])
    node_label = pd.merge(node_label, wallet_001_amount, how='left', on='def')
    node_label['text'] = np.where(node_label['text'].isna(), node_label['def'], node_label['text'])

    # edge width
    if len(node_label) < 50:
        edge_width = 2
    else:
        edge_width = 1

    if len(node_label) > 1000:
        edge_width = 0.5

    # node label attributes
    node_label_attr = []
    node_label_attr = node_label.copy()

    node_label_attr['color'] = np.where(node_label_attr['def'].isin(known_address_exception['from_def']), 'aqua', 'azure')
    node_label_attr['color'] = np.where(node_label_attr['def'] == name_this_address, 'deeppink', node_label_attr['color'])
    node_label_attr['color'] = np.where(node_label_attr['def'] == largest_nodesize.iloc[0,0], 'salmon', node_label_attr['color'])
    node_label_attr['weight'] = np.where(node_label_attr['def'] == name_this_address, 'heavy', 'normal')
    node_label_attr['weight'] = np.where(node_label_attr['def'].isin(largest_nodesize['def']), 'heavy', node_label_attr['weight'])

    # making the size of font 0 if more than 100 nodes
    node_label_attr['fontsize'] = np.where(node_label_attr['def'].str.startswith(('w_', 'binance\nsweeper','funnel','facilitator')), 6, 9)
    node_label_attr['fontsize'] = np.where(node_label_attr['def'].isin(largest_nodesize['def']), 9,
                                           node_label_attr['fontsize'])
    if len(node_label) > 100:
            node_label_attr['fontsize'] = np.where(node_label_attr['text'].str.startswith('w_') & ~node_label['def'].isin(largest_nodesize['def']), 5, node_label_attr['fontsize'])
    if len(node_label) > 500:
            node_label_attr['fontsize'] = np.where(node_label_attr['text'].str.startswith('w_') & ~node_label['def'].isin(largest_nodesize['def']), 0, node_label_attr['fontsize'])


    # node & edge label into dict
    node_labeldict = node_label.set_index('def')['text'].to_dict()





    edge_labeldict = temp_df.drop(columns='weight')
    edge_labeldict['amount'] = edge_labeldict['amount'].round(0).astype(int).apply('{:,}'.format).astype(str) + '\n ICX'

    # going out balance
    edge_labeldict_r = edge_labeldict[edge_labeldict['color'] == 'r']
    edge_labeldict_r = edge_labeldict_r.set_index(['from_def','dest_def']).pop("amount").to_dict()

    # coming in balance
    edge_labeldict_g = edge_labeldict[edge_labeldict['color'] == 'g']
    edge_labeldict_g = edge_labeldict_g.set_index(['from_def','dest_def']).pop("amount").to_dict()

    edge_labeldict = edge_labeldict.set_index(['from_def','dest_def']).pop("amount").to_dict()

    # adjusting text
    vmax_val = 0
    leftright = 100
    updown = -220

    ## figure
    plt.style.use(['dark_background'])
    fig = plt.figure(figsize=(12,8))
    ax = plt.gca()
    # ax.set_title('$ICX flow from ' + name_this_address + ' wallet (' + day_today_text + ')')

    # title based on range or not
    if date_is_range == 1:
        title_date = date_of_interest[0] + ' ~ ' + date_of_interest[-1]
    else:
        title_date = date_of_interest[0]

    if tx_flow == 'both':
        in_out_text = 'transacted with '
    elif tx_flow == 'out':
        in_out_text = 'flow from '
    elif tx_flow == 'in':
        in_out_text = 'flow into '

    # to give title different colour
    plt.figtext(0.25, 0.96, "$ICX", fontsize='large', weight='bold', color='cyan', ha='right')
    plt.figtext(0.255, 0.96, in_out_text + name_this_address + wallet_or_contact + '(' + title_date + ')', fontsize='large', color='w', ha='left')
    plt.figtext(0.25, 0.93, '(' + this_address + ')', fontsize='large', color='deeppink', ha='left')

    G = nx.from_pandas_edgelist(temp_df, 'from_def', 'dest_def', create_using=nx.MultiDiGraph())
    pos = nx.nx_pydot.graphviz_layout(G)
    # pos = nx.spring_layout(G)

    # node size
    nodesize_ratio = nodesize_ratio
    nodesize = nodesize.set_index('def').squeeze().loc[list(G.nodes)]  # to align the data with G.nodes

    # node text color & weight
    node_label_color = node_label_attr[['def','text','color']].set_index('def').loc[list(G.nodes)].set_index('text').squeeze() # to align the data with G.nodes
    node_label_weight = node_label_attr[['def','text','weight']].set_index('def').loc[list(G.nodes)].set_index('text').squeeze() # to align the data with G.nodes
    node_label_fontsize = node_label_attr[['def','text','fontsize']].set_index('def').loc[list(G.nodes)].set_index('text').squeeze() # to align the data with G.nodes

    edgelist = list(G.edges)
    edgelist = [el[:2] for el in edgelist]

    if len(edgelist) == 1:
        temp_df = temp_df.set_index(['from_def','dest_def']).loc[edgelist]
    else:
        temp_df = temp_df.set_index(['from_def','dest_def']).squeeze().loc[edgelist]

    color_map_1 = []
    for node in G:
        if node == name_this_address:
            color_map_1.append('brown')
        else:
            color_map_1.append('steelblue')

    colors = temp_df['weight'].values
    # cmap = plt.cm.YlGn
    # cmap = plt.cm.coolwarm
    cmap = plt.cm.viridis
    vmin = 0

    if vmax_val == 0:
        vmax = max(temp_df['weight'].values)
    else:
        vmax = vmax_val

    g = nx.draw(G, node_color=color_map_1, pos=pos,
                with_labels=False, connectionstyle='arc3, rad = 0.15', #labels=node_labeldict,
                node_size=nodesize / nodesize_ratio, alpha=0.8, arrows=True,
                font_size=8, font_color='azure', font_weight='normal',
                edge_color=colors,
                edge_cmap=cmap,
                vmin=vmin, vmax=vmax, width=edge_width)
                #width=temp_df['weight_ratio'])  # fontweight='bold',


    # rename pos for layout (renaming labels)
    def rekey(inp_dict, keys_replace):
        return {keys_replace.get(k, k): v for k, v in inp_dict.items()}
    pos = rekey(pos, node_labeldict)

    for node, (x, y) in pos.items():
        text(x, y, node,
             color=node_label_color[node],
             weight=node_label_weight[node],
             fontsize=node_label_fontsize[node],
             ha='center', va='center')

    # nx.draw_networkx_edge_labels(G, pos, font_size=6,
    #                              edge_labels=edge_labeldict_r,
    #                              label_pos=0.6,
    #                              font_color='r', bbox=dict(alpha=0))
    #
    # nx.draw_networkx_edge_labels(G, pos, font_size=6,
    #                              edge_labels=edge_labeldict_g,
    #                              label_pos=0.6,
    #                              font_color='g', bbox=dict(alpha=0))


    box_text = ' (1) Circle size represents amount at the destination \n (2) Arrow thickness/colour represents number of transactions'
    text_box = AnchoredText(box_text, frameon=False, loc=4, pad=0.5)
    plt.setp(text_box.patch, facecolor='black', alpha=0.5)
    ax.add_artist(text_box)
    plt.axis('off')
    plt.tight_layout()
    # ax.text(leftright, updown,' (1) Circle size represents amount transacted \n (2) Arrow thickness/colour represents number of transactions')


    # # Set Up Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm._A = []
    clb = plt.colorbar(sm, shrink=0.5)
    clb.ax.set_title('Number of TX')

    fig.set_facecolor('black')




    if len(all_possible_exchange_wallets) != 0:
        # possible exchange wallets
        all_possible_exchange_wallets = sorted(np.unique(all_possible_exchange_wallets))
        all_possible_exchange_wallets = pd.DataFrame({'from_address': all_possible_exchange_wallets})
        all_possible_exchange_wallets = pd.merge(all_possible_exchange_wallets, wallet_info, left_on='from_address', right_on='address', how='left')
        all_possible_exchange_wallets = all_possible_exchange_wallets.drop(columns='from_address').rename(columns={'address':'possible_Ex_add',
                                                                              'wallet_def':'possible_Ex_name'})

        print(all_possible_exchange_wallets)



    this_title = 'TAP Div Breakdown'
    # this_title = 'iAM Div Breakdown'

    # just for iAM
    table = concat_df.copy()

    table['TX count'] = table.groupby('dest_address')['dest_address'].transform('count')
    table['amount'] = table['amount'].astype(float)
    table = table.rename(columns={'dest_address': 'Address count', 'amount': 'Amount (ICX)'})
    table = table.groupby("TX count").agg({"Amount (ICX)": np.sum, "Address count": pd.Series.nunique})
    table['Address count'] = table['Address count'].astype(int).apply('{:,}'.format).astype(str)
    table['Amount (ICX)'] = table['Amount (ICX)'].astype(float).round(2).astype(int).apply('{:,}'.format).astype(str)
    table = table.reset_index()

    import six

    def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=12,
                         header_color='#40466e', row_colors=['black', 'black'], edge_color='w',
                         bbox=[0, 0, 1, 1], header_columns=0,
                         ax=None, **kwargs):
        if ax is None:
            size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
            fig, ax = plt.subplots(figsize=size)
            ax.axis('off')
            ax.set_title(this_title, fontsize=15,
                         weight='bold', pad=30)
            plt.tight_layout()

        mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(font_size)

        for k, cell in  six.iteritems(mpl_table._cells):
            cell.set_edgecolor(edge_color)
            if k[0] == 0 or k[1] < header_columns:
                cell.set_text_props(weight='bold', color='w')
                cell.set_facecolor(header_color)
            else:
                cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
        return ax


    render_mpl_table(table)

except NameError:
    print('There was no transaction in the given period')