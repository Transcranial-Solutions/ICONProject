#########################################################################
## Project: Transaction per day                                        ##
## Date: August 2021                                                   ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Extract

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
import os
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from iconsdk.wallet.wallet import KeyWallet
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from time import time
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

currPath = os.getcwd()
projectPath = os.path.join(currPath, "08_transaction_data")
if not os.path.exists(projectPath):
    os.mkdir(projectPath)

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)


# get yesterday function
def yesterday(string=False):
    yesterday = datetime.utcnow() - timedelta(1)
    if string:
        return yesterday.strftime('%Y-%m-%d')
    return yesterday


# today's date
# today = date.today()
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")

# to use specific date (1), otherwise use yesterday (0)
use_specific_prev_date = 2
date_prev = "2021-07-21"

if use_specific_prev_date == 1:
    date_of_interest = date_prev
elif use_specific_prev_date == 0:
    date_of_interest = yesterday(today)
elif use_specific_prev_date == 2:
    # for loop between dates
    day_1 = "2021-07-21"; day_2 = "2021-07-31"
    date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()
else:
    date_of_interest=[]
    print('No date selected.')

print(date_of_interest)


for date_prev in date_of_interest:

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ load  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # data loading
    # windows_path = "E:/GitHub/ICONProject/data_analysis/08_transaction_data/data/"
    tx_df = pd.read_csv(os.path.join(dataPath, 'tx_final_' + date_prev + '.csv'), low_memory=False)

    def clean_tx_df(tx_df, from_this='from', to_this='to'):
        tx_df[from_this] = np.where(tx_df[from_this].isnull(), 'System', tx_df[from_this])
        tx_df[to_this] = np.where(tx_df[to_this].isnull(), 'System', tx_df[to_this])
        tx_df['intEvtOthCount'] = tx_df['intEvtCount'] - tx_df['intTxCount']
        return tx_df

    tx_df = clean_tx_df(tx_df, from_this='from', to_this='to')



    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX Address Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

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


    # this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
    known_address_url = Request('https://iconwat.ch/data/thes', headers={'User-Agent': 'Mozilla/5.0'})
    jknown_address = json.load(urlopen(known_address_url))


    def add_dict_if_noexist(key, d, value):
        if key not in d:
            d[key] = value


    def add_know_addresses():
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

        add_dict_if_noexist('hxddec6fb21f9618b537e930eaefd7eda5682d9dc8', jknown_address, 'unkEx_e1')

        add_dict_if_noexist('hx294c5d0699615fc8d92abfe464a2601612d11bf7', jknown_address, 'funnel_1')
        add_dict_if_noexist('hx44c0d5fab0c81fe01a052f5ffb83fd152e505202', jknown_address, 'facilitator_1')

        add_dict_if_noexist('hx8f0c9200f58c995fb28029d83adcf4521ff5cb2f', jknown_address, 'LDX Distro')

        add_dict_if_noexist('hxbdd5ba518b70408acd023a18e4d6b438c7f11655', jknown_address, 'Somesing Exchange')

        add_dict_if_noexist('hx037c73025819e490e9a01a7e954f9b46d89b0245', jknown_address, 'MyID_related')
        add_dict_if_noexist('hx522bff55a62e0c75a1b51855b0802cfec6a92e84', jknown_address, '3-min_tx_bot_out')
        add_dict_if_noexist('hx11de4e28be4845de3ea392fd8d758655bf766ca7', jknown_address, '3-min_tx_bot_in')

        # add_dict_if_noexist('hx7a649b6b2d431849fd8e3db2d4ed371378eacf78', jknown_address, 'icf_related1')
        # add_dict_if_noexist('hx63862927a9c1389e277cd20a6168e51bd50af13e', jknown_address, 'icf_related2')
        # add_dict_if_noexist('hxc8377a960d4eb484a3b8a733012995583dda0813', jknown_address, 'easy_crypto')

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

        add_dict_if_noexist('hx1a5f0ce1d0d49054379a554f644f39a66a979b04', jknown_address, 'circle_arb_1_related')
        add_dict_if_noexist('hx5b802623eb53c6a90df9f29e5808596f3c2bf63e', jknown_address, 'circle_arb_2_related')

    add_know_addresses()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Contract Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    tx_type = 'contract'


    # functions to get transactions and the page count needed for webscraping
    def get_tx_via_url(tx_type=tx_type,
                       page_count=1,
                       tx_count=1):
        """ This function is used to extract json information from icon page (note that total number of
        tx from the website is 500k max, so we can't go back too much """

        # getting transaction information from icon transaction page
        # contract information
        if tx_type in ['contract']:
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


    def get_page_tx_count(tx_type=tx_type):
        """ This function is to get the number of elements per page and number of page to be extracted """

        if tx_type in ['contract', 'token_txlist']:
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
            page_count = 0
            tx_count = 0
            print("No records found!")

        return page_count, tx_count


    # getting contract information from icon transaction page
    page_count = []
    tx_count = []
    page_count, tx_count = get_page_tx_count(tx_type='contract')


    def get_contract_info():
        # collecting contact info using multithreading
        start = time()
        known_contract_all = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for k in range(0, page_count):
                known_contract_all.append(
                    executor.submit(get_tx_via_url, tx_type='contract', page_count=k + 1, tx_count=tx_count))

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
        jknown_address['cx1c06cf597921e343dfca2883f699265fbec4d578'] = 'ICONbet \nLottery (new)'
        jknown_address['cx5d6e1482434085f30660efe30573304d629270e5'] = 'ICONbet \nBaccarat'
        jknown_address['cx38fd2687b202caf4bd1bda55223578f39dbb6561'] = 'ICONbet \nMini Roulette (new)'
        jknown_address['cx6cdbc291c73faf79366d35b1491b89217fdc6638'] = 'ICONbet \nWar'
        jknown_address['cx8f9683da09e251cc2b67e4b479e016550f154bd6'] = 'ICONbet \nHi-Lo'
        jknown_address['cxd47f7d943ad76a0403210501dab03d4daf1f6864'] = 'ICONbet \nBlackjack'
        jknown_address['cx299d88908ab371d586c8dfe0ed42899a899e6e5b'] = 'ICONbet \nLevels'
        jknown_address['cxca5df10ab4f46df979aa2d38b370be85076e6117'] = 'ICONbet \nColors'
        jknown_address['cx03c76787861eec166b25e744e52a82af963670eb'] = 'ICONbet \nPlinko'
        jknown_address['cx26b5b9990e78c6afe4f9d30776a43b1c19f7d85a'] = 'ICONbet \nSic Bo'
        jknown_address['cx9fda786d3e7965ed9dc01321c85026653d6a5ff4'] = 'ICONbet \nJungle Jackpot'
        jknown_address['cx3b9955d507ace8ac27080ed64948e89783a62ab1'] = 'ICONbet \nReward'
        jknown_address['cx1b97c1abfd001d5cd0b5a3f93f22cccfea77e34e'] = 'ICONbet \nGame Contract'
        jknown_address['cxc6bb033f9d0b2d921887040b0674e7ceec1b769c'] = 'Lossless Lottery'
        jknown_address['cx14002628a4599380f391f708843044bc93dce27d'] = 'iAM Div'
        jknown_address['cx75e584ffe40cf361b3daa00fa6593198d47505d5'] = 'TAP Div'
        jknown_address['cxff66ea114d20f6518e89f1269b4a31d3620b9331'] = 'PGT Distro'
        jknown_address['cx953260a551584681e1f0492dce29e07d323ed5a6'] = 'ICONPOOL'
        jknown_address['cx087b4164a87fdfb7b714f3bafe9dfb050fd6b132'] = 'Relay_1'
        jknown_address['cx2ccc0c98ab5c2709cfc2c1512345baa99ea4106a'] = 'Relay_2'
        jknown_address['cx735704cf28098ea43cae2a8325c35a3e7f2a5d1c'] = 'Relay_3'
        jknown_address['cx9e3cadcc1a4be3323ea23371b84575abb32703ae'] = 'MyID_1'
        jknown_address['cx694e8c9f1a05c8c3719f30d46b97697960e4289e'] = 'MyID_2'
        jknown_address['cxba62bb61baf8dd8b6a04633fe33806547785a00c'] = 'MyID_3'
        jknown_address['cxcaef4255ec5cb784594655fa5ff62ce09a4f8dfa'] = 'w3id'
        jknown_address['cx636caea5cf5a336d33985ae12ae1839821a175a4'] = 'SEED_1'
        jknown_address['cx2e138bde7e4cb3706c7ac3c881fbd165dce49828'] = 'SEED_2'
        jknown_address['cx3c08892673803db95c617fb9803c3653f4dcd4ac'] = 'SEED_3'
        jknown_address['cx32b06547643fead9048ea912ba4c03419ee97052'] = 'FutureICX'
        jknown_address['cx9c4698411c6d9a780f605685153431dcda04609f'] = 'Auction'
        jknown_address['cx334beb9a6cde3bf1df045869447488e0de31df4c'] = 'circle_arb_1'
        jknown_address['cx9df59cf2c7dc7ae2dbdec4a10b295212595f2378'] = 'circle_arb_2'
        jknown_address['cx05874afb081257373c89491d6dc63faefb428bb9'] = 'circle_arb_3'
        jknown_address['cxcc711062b732ed14954008da8a5b5193b4d48618'] = 'peek_1'
        jknown_address['cxa89982990826b66d86ef31275e93275dfddabfde'] = 'peek_2'
        jknown_address['cxcaef4255ec5cb784594655fa5ff62ce09a4f8dfa'] = 'peek_3'
        jknown_address['cxfb832c213401d824b9725b5cca8d75b734fd830b'] = 'rev_share'
        jknown_address['cx40d59439571299bca40362db2a7d8cae5b0b30b0'] = 'balanced_check_1'
        jknown_address['cx624af53e8954abed2acf18e6f8c9f35eae918244'] = 'balanced_check_2'

        for k, v in jknown_address.items():
            if v == "-":
                jknown_address[k] = "unknown_cx"

        # making same table but with different column names
        known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['to', 'to_def'])
        known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from', 'from_def'])

        known_address_exception = known_address_details_from[
            ~known_address_details_from['from'].str.startswith('binance\nsweeper', na=False)]

        return known_address_details_to, known_address_details_from, known_address_exception


    known_address_details_to, known_address_details_from, known_address_exception = get_contract_info()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Analysis ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    tx_df = pd.merge(tx_df, known_address_details_from, on='from', how='left')
    tx_df = pd.merge(tx_df, known_address_details_to, on='to', how='left')


    def bool_address_type(df=tx_df, direction='from', type='hx') -> bool:
        return df[direction].str.startswith(type, na=False) & df[direction+'_def'].isnull()

    tx_df['from_def'] = np.where(bool_address_type(tx_df, 'from', 'hx'), 'unknown_hx', tx_df['from_def'])
    tx_df['from_def'] = np.where(bool_address_type(tx_df, 'from', 'cx'), 'unknown_cx', tx_df['from_def'])

    tx_df['to_def'] = np.where(bool_address_type(tx_df, 'to', 'hx'), 'unknown_hx', tx_df['to_def'])
    tx_df['to_def'] = np.where(bool_address_type(tx_df, 'to', 'cx'), 'unknown_cx', tx_df['to_def'])

    tx_df = clean_tx_df(tx_df, from_this='from_def', to_this='to_def')

    # defs
    from_def = tx_df[['from','from_def']].drop_duplicates().sort_values(by='from')
    to_def = tx_df[['to','to_def']].drop_duplicates().sort_values(by='to')


    # check_from = tx_df.groupby(['from'])['tx_fees']\
    #     .agg({'count','sum'})\
    #     .sort_values(by='count', ascending=False)\
    #     .rename(columns={'count':'tx_count','sum':'fees_burned'})\
    #     .reset_index()\
    #     .merge(from_def, how='left', on='from')


    check_to = tx_df.groupby(['to'])['tx_fees']\
        .agg({'count','sum'})\
        .sort_values(by='count', ascending=False)\
        .rename(columns={'count':'Regular Tx','sum':'Fees burned'})\
        .reset_index()\
        .merge(to_def, how='left', on='to')

    check_to_intTx = tx_df.groupby(['to'])['intTxCount']\
        .agg({'sum'})\
        .sort_values(by='sum', ascending=False)\
        .rename(columns={'sum':'Internal Tx'})\
        .reset_index()\
        .merge(to_def, how='left', on='to')

    check_to_intEvt = tx_df.groupby(['to'])['intEvtCount']\
        .agg({'sum'})\
        .sort_values(by='sum', ascending=False)\
        .rename(columns={'sum':'Internal Event'})\
        .reset_index()\
        .merge(to_def, how='left', on='to')

    check_to_intEvtOth = tx_df.groupby(['to'])['intEvtOthCount']\
        .agg({'sum'})\
        .sort_values(by='sum', ascending=False)\
        .rename(columns={'sum':'Internal Event (excluding Tx)'})\
        .reset_index()\
        .merge(to_def, how='left', on='to')

    def grouping_wrapper(df, in_col):
        def wallet_grouping(df, in_col, in_name, else_name):
            df['group'] = np.where(df[in_col].str.contains(in_name, case=False, regex=True), in_name, else_name)
            return df

        these_incols = ['bithumb', 'upbit','velic','bitvavo','unkEx_c','unkEx_d','kraken',
                        'circle_arb','ICONbet','Relay','MyID','Balance','Nebula','peek','Craft','SEED','iAM ']

        def group_wallet(df, in_col='to_def'):
            df = wallet_grouping(df,  in_col, in_name='binance', else_name=df[in_col])

            for i in these_incols:
                df = wallet_grouping(df, in_col, i, df['group'])
            return df

        df = group_wallet(df, in_col)

        # for unified protocol
        unified_protocol = (df[in_col].str[1].str.isupper()) & (df[in_col].str[0] == 'u')
        df['group'] = np.where(unified_protocol, 'UP', df['group'])

        def manual_grouping(df):

            #balanced
            df['group'] = np.where(df['group'] == 'Balance', 'Balanced', df['group'])
            df['group'] = np.where(df['to'] == 'cx43e2eec79eb76293c298f2b17aec06097be606e0', 'Balanced', df['group'])
            df['group'] = np.where(df['to'] == 'cxaf244cf3c7164fe6f996f398a9d99c4d4a85cf15', 'Balanced', df['group'])

            #iconbet
            df['group'] = np.where(df['group'] == 'TAPToken', 'ICONbet', df['group'])
            df['group'] = np.where(df['group'] == 'SicBo', 'ICONbet', df['group'])

            # futureicx
            df['group'] = np.where(df['group'] == 'FutureICX', 'FutureICX', df['group'])
            df['group'] = np.where(df['group'] == 'EpICX', 'FutureICX', df['group'])
            return df


        df = manual_grouping(df)

        return df


    # combine and add grouping
    tos = [check_to, check_to_intTx, check_to_intEvtOth]
    to_final = reduce(lambda left,right: pd.merge(left,right,on=['to','to_def']), tos)
    to_final_grouping = grouping_wrapper(to_final, in_col='to_def')

    to_final_grouping.to_csv(os.path.join(resultPath, 'tx_summary_' + date_prev + '.csv'), index=False)

    # to_group = to_final_grouping.groupby('group').agg('sum').sort_values(by='Regular Tx', ascending=False) #.reset_index()
    to_group = to_final_grouping.groupby('group').agg('sum').sort_values(by='Fees burned', ascending=False) #.reset_index()

    totals = to_group.agg('sum')

    all_tx = 'Total Transactions: ' + '{:,}'.format(totals['Regular Tx'].astype(int) + totals['Internal Tx'].astype(int)) + '\n' + \
             'Total Events (including Tx): ' + '{:,}'.format(totals['Regular Tx'].astype(int) + totals['Internal Event (excluding Tx)'].astype(int))

    totals = totals.astype(int).map("{:,}".format)

    to_group = to_group.rename(columns={'Fees burned': 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX)',
                             'Regular Tx': 'Regular Tx' + ' (' + totals['Regular Tx'] + ')',
                             'Internal Tx': 'Internal Tx' + ' (' + totals['Internal Tx'] + ')',
                             'Internal Event (excluding Tx)': 'Internal Event (excluding Tx: ' + totals['Internal Event (excluding Tx)'] + ')'})

    fees_burned = 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX)'

    plot_df = to_group.drop(columns=fees_burned)




    # stacked barplot


    import matplotlib.pyplot as plt # for improving our visualizations
    import matplotlib.lines as mlines
    import seaborn as sns

    sns.set(style="dark")
    plt.style.use("dark_background")
    ax1 = plot_df.plot(kind='bar', stacked=True, figsize=(14, 8))
    plt.title('Daily Transactions' + ' (' + date_prev + ')', fontsize=14, weight='bold', pad=10, loc='left')
    ax1.set_xlabel('Destination Contracts/Addresses')
    ax1.set_ylabel('Transactions')
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=90, ha="center")
    ax2 = ax1.twinx()
    lines = plt.plot(to_group[fees_burned])

    ax2.set_ylabel(fees_burned, labelpad=10)
    plt.setp(lines, 'color', 'r', 'linewidth', 2.0)
    # ax1.legend(loc='upper center', bbox_to_anchor=(0.4, 0.95),
    #           fancybox=True, shadow=True, ncol=5)
    color = 'tab:red'
    red_line = mlines.Line2D([], [], color=color, label=fees_burned, linewidth=3)
    plt.legend(handles=[red_line], loc='upper right', fontsize='small', bbox_to_anchor=(0.98, 0.99), frameon=False)
    plt.tight_layout(rect=[0,0,1,1])

    xmin, xmax = ax1.get_xlim()
    ymin, ymax = ax1.get_ylim()
    ax1.text(xmax*0.97, ymax*0.88, all_tx,
            horizontalalignment='right',
            verticalalignment='center',
            linespacing = 1.5,
            fontsize=12,
            weight='bold')

    handles, labels = ax1.get_legend_handles_labels()
    ax1.legend(handles, labels,
               loc='upper right', bbox_to_anchor=(1, 1.07),
               frameon=False, fancybox=True, shadow=True, ncol=3)


    plt.savefig(os.path.join(resultPath, 'tx_summary_' + date_prev + '.png'))

    print(date_prev + ' is done!')

def run_all():
    if __name__ == "__main__":
        run_all()