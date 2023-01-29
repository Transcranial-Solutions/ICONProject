#########################################################################
## Project: Transaction per week                                       ##
## Date: December 2021                                                 ##
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
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.wallet.wallet import KeyWallet
from typing import Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from time import time, sleep
from datetime import date, datetime, timedelta
from tqdm import tqdm
from functools import reduce
import re
import random
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
import seaborn as sns
import random
import requests

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


projectPath = os.getcwd()

dailyPath = '/home/tono/ICONProject/data_analysis/'
projectPath = '/home/tono/ICONProject/data_analysis/08_transaction_data'

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

weeklyresultPath = os.path.join(resultPath, "weekly")
if not os.path.exists(weeklyresultPath):
    os.mkdir(weeklyresultPath)

walletPath = os.path.join(projectPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)

# reading address info
walletaddressPath = os.path.join(dailyPath, "wallet_addresses")
with open(os.path.join(walletaddressPath, 'contract_addresses.json')) as f:
          contract_addresses = json.load(f)
with open(os.path.join(walletaddressPath, 'exchange_addresses.json')) as f:
          exchange_addresses = json.load(f)
with open(os.path.join(walletaddressPath, 'other_addresses.json')) as f:
          other_addresses = json.load(f)


basicstatPath = os.path.join(dailyPath,"output/icon_tracker/data/")
tokentransferPath = os.path.join(dailyPath,"10_token_transfer/results/")

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
    
# get yesterday function
# def yesterday(string=False):
#     yesterday = datetime.utcnow() - timedelta(1)
#     if string:
#         return yesterday.strftime('%Y-%m-%d')
#     return yesterday

def yesterday(doi = "2021-08-20"):
    yesterday = datetime.fromisoformat(doi) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def tomorrow(doi = "2021-08-20"):
    tomorrow = datetime.fromisoformat(doi) + timedelta(1)
    return tomorrow.strftime('%Y-%m-%d')

def one_week_ago(doi = "2021-08-20"):
    one_week_ago = datetime.fromisoformat(doi) - timedelta(6)
    return one_week_ago.strftime('%Y-%m-%d')

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


def clean_tx_df(tx_df, from_this='from', to_this='to'):
    tx_df[from_this] = np.where(tx_df[from_this].isnull(), 'System', tx_df[from_this])
    tx_df[to_this] = np.where(tx_df[to_this].isnull(), 'System', tx_df[to_this])
    tx_df['intEvtOthCount'] = tx_df['intEvtCount'] - tx_df['intTxCount']
    return tx_df

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


def request_sleep_repeat(url, repeat=3, verify=True):
    for i in range(0,repeat):
        print(f"Trying {str(i)}...")
        try:
            # this is from Blockmove's iconwatch -- get the destination address (known ones, like binance etc)
            known_address_url = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=verify)
            random_sleep_except = random.uniform(3,6)
            print("Just pausing for " + str(random_sleep_except) + " seconds and try again \n")
            sleep(random_sleep_except)
            
        except:
            random_sleep_except = random.uniform(30,60)
            print("I've encountered an error! I'll pause for " + str(random_sleep_except) + " seconds and try again \n")
            sleep(random_sleep_except)
    return known_address_url


def add_dict_if_noexist(key, d, value):
    if key not in d:
        d[key] = value

def replace_dict_if_unknown(key, d, value):
    if ("-" in d.values()) or (d.values() == None):
        d.update({key: value})

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Contract Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

def get_contract_info():
    def token_tx_using_community_tracker(total_pages=100):
        # functions to get transactions and the page count needed for webscraping
        def get_tx_via_icon_community_tracker(skip_pages):
            """ This function is used to extract json information from icon community site """
            req = requests.get('https://tracker.icon.community/api/v1/addresses/contracts?search=&skip=' + str(skip_pages) + '&limit=' + str(100),
                                 headers={'User-Agent': 'Mozilla/5.0'})
            try:
                jtracker = json.loads(req.text)
                jtracker_df = pd.DataFrame(jtracker)
            except:
                jtracker_df = pd.DataFrame()
            return jtracker_df
    
        skip = 100
        total_pages = total_pages
        last_page = total_pages * skip - skip
        page_count = range(0, last_page, skip)
    
        tx_all = []
        for k in tqdm(page_count):
            temp_df = get_tx_via_icon_community_tracker(k)
            tx_all.append(temp_df)             
    
        df_contract = pd.concat(tx_all)

        return df_contract

    df_contract = token_tx_using_community_tracker()
    POSSIBLE_NANS = ['', ' ', np.nan]
    df_contract['name'] = np.where(df_contract['name'].isin(POSSIBLE_NANS), '-', df_contract['name'])
    
    contract_d = dict(zip(df_contract.address, df_contract.name))

    # updating known address with other contract addresses
    jknown_address.update(contract_d)
    
    ## updating contract info
    {replace_dict_if_unknown(k, jknown_address, v) for k,v in contract_addresses.items()}


    for k, v in jknown_address.items():
        if v == "-":
            jknown_address[k] = "unknown_cx"

    # making same table but with different column names
    known_address_details_to = pd.DataFrame(jknown_address.items(), columns=['to', 'to_def'])
    known_address_details_from = pd.DataFrame(jknown_address.items(), columns=['from', 'from_def'])

    known_address_exception = known_address_details_from[
        ~known_address_details_from['from'].str.startswith('binance\nsweeper', na=False)]

    return known_address_details_to, known_address_details_from, known_address_exception


def bool_address_type(df, direction='from', type='hx') -> bool:
    return df[direction].str.startswith(type, na=False) & df[direction+'_def'].isnull()


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
        df['group'] = np.where(df['group'].str.contains('balanced_', case=False), 'Balanced', df['group'])
        df['group'] = np.where(df['group'].str.contains('baln', case=False), 'Balanced', df['group'])
        df['group'] = np.where(df['group'].str.contains('circle_arb', case=False), 'Balanced', df['group'])

        #omm
        df['group'] = np.where(df['group'].str.contains('omm', case=False), 'Omm', df['group'])
        # df['group'] = np.where(df['group'].str.contains('ICON USD', case=False), 'Omm', df['group'])

        #Optimus
        df['group'] = np.where(df['group'].str.contains('optimus', case=False), 'Optimus', df['group'])
        df['group'] = np.where(df['to_def'].str.contains('optimus', case=False), 'Optimus', df['group'])
        df['group'] = np.where(df['group'].str.contains('finance token', case=False), 'Optimus', df['group'])


        #Craft
        df['group'] = np.where(df['to'] == 'cx9c4698411c6d9a780f605685153431dcda04609f', 'Craft', df['group'])
        df['group'] = np.where(df['to'] == 'cx82c8c091b41413423579445032281bca5ac14fc0', 'Craft', df['group'])
        df['group'] = np.where(df['to'] == 'cx7ecb16e4c143b95e01d05933c17cb986cfe618e6', 'Craft', df['group'])
        df['group'] = np.where(df['to'] == 'cx5ce7d060eef6ebaf23fa8a8717d3a5c8f0a3fda9', 'Craft', df['group'])
        df['group'] = np.where(df['to'] == 'cx2d86ce51600803e187ce769129d1f6442bcefb5b', 'Craft', df['group'])
        
        #iAM
        df['group'] = np.where(df['to'] == 'cx210ded1e8e109a93c89e9e5a5d0dcbc48ef90394', 'iAM ', df['group'])

        #Bridge
        df['group'] = np.where(df['to'] == 'cxa82aa03dae9ca03e3537a8a1e2f045bcae86fd3f', 'Bridge', df['group'])
        df['group'] = np.where(df['to'] == 'cx0eb215b6303142e37c0c9123abd1377feb423f0e', 'Bridge', df['group'])


        #iconbet
        df['group'] = np.where(df['group'].str.contains('SicBo', case=False), 'ICONbet', df['group'])
        df['group'] = np.where(df['group'].str.contains('Jungle Jackpot', case=False), 'ICONbet', df['group'])
        df['group'] = np.where(df['group'] == 'TapToken', 'ICONbet', df['group'])

        #gangstabet
        df['group'] = np.where(df['group'].str.contains('gangstabet', case=False), 'GangstaBet', df['group'])
        df['group'] = np.where(df['group'].str.contains('crown', case=False), 'GangstaBet', df['group'])
        df['group'] = np.where(df['to'] == 'cx8683d50b9f53275081e13b64fba9d6a56b7c575d', 'GangstaBet', df['group'])
        
        # futureicx
        # df['group'] = np.where(df['group'] == 'FutureICX', 'FutureICX', df['group'])
        # df['group'] = np.where(df['group'] == 'EpICX', 'FutureICX', df['group'])
        df['group'] = np.where(df['group'] == 'FutureICX', 'EPX', df['group'])
        df['group'] = np.where(df['group'].str.contains('epx', case=False), 'EPX', df['group'])

        # UP
        df['group'] = np.where(df['to'] == 'cxc432c12e6c91f8a685ee6ff50a653c8a056875e4', 'UP', df['group'])

        # Inanis
        df['group'] = np.where(df['group'].str.contains('Inanis', case=False), 'Inanis', df['group'])

        # FRAMD
        df['group'] = np.where(df['to_def'].str.contains('Yetis', case=False), 'FRAMD', df['group'])
        df['group'] = np.where(df['group'].str.contains('FRAMD', case=False), 'FRAMD', df['group'])

        # BTP
        df['group'] = np.where(df['to_def'].str.lower().str.startswith('btp'), 'BTP', df['group'])


        return df

    df = manual_grouping(df)

    return df


def get_donut_df(df, col_num):
    df_out = df.iloc[:,col_num].sort_values(ascending=False).to_frame().reset_index()
    df_out['top_10'] = np.arange(df_out.shape[0])
    df_out['group'] = np.where(df_out['top_10'] > 9, 'Others', df_out['group'])
    df_out = df_out.groupby('group').sum().sort_values(by='top_10').drop(columns='top_10')
    return df_out

def plot_donut_df(df_col=3, title='Regular Tx', add_string=""):
    df_regular_tx = get_donut_df(plot_df_donut, df_col)

    df_regular_tx['percent'] = df_regular_tx / df_regular_tx.sum()
    df_regular_tx['percent'] = df_regular_tx['percent'].astype(float).map("{:.2%}".format)

    plt.style.use(['dark_background'])
    fig, ax = plt.subplots(figsize=(10, 6), subplot_kw=dict(aspect="equal"))
    fig.patch.set_facecolor('black')

    cmap = plt.get_cmap("Set3")
    these_colors = cmap(np.array(range(len(df_regular_tx[0:]))))

    my_circle = plt.Circle((0,0), 0.7, color='black')

    wedges, texts = plt.pie(df_regular_tx.reset_index().iloc[:,1],
                                    labels=df_regular_tx.reset_index().iloc[:,0],
                                    counterclock=False,
                                    startangle=90,
                                    colors=these_colors,
                                    textprops={'fontsize': 9, 'weight': 'bold'}, rotatelabels=True)

    for text, color in zip(texts, these_colors):
        text.set_color(color)

    # for plotting (legend)
    label_text = df_regular_tx.reset_index().iloc[:,0] \
                + ' (' + df_regular_tx.reset_index().iloc[:,1].astype(int).apply('{:,}'.format).astype(str) \
                + ' || ' + df_regular_tx.reset_index().iloc[:,2] + ')'

    if df_col in [0,2]:
        ax.text(0., 0., totals_reused.reset_index()[df_col:df_col+1]['index'][df_col] + ': ' + totals_reused.reset_index()[df_col:df_col+1][0][df_col] + add_string,
                horizontalalignment='center',
                verticalalignment='center',
                linespacing=2,
                fontsize=10,
                weight='bold')

    if df_col == 1:
        ax.text(0., 0., totals_reused.reset_index()[df_col:df_col+1]['index'][df_col] + ': \n' + totals_reused.reset_index()[df_col:df_col+1][0][df_col] + add_string,
                horizontalalignment='center',
                verticalalignment='center',
                linespacing=2,
                fontsize=10,
                weight='bold')

    if df_col == 3:
        ax.text(0., 0.05, totals_reused.reset_index()[df_col:df_col+1]['index'][df_col] + ': ' + totals_reused.reset_index()[df_col:df_col+1][0][df_col] + add_string,
            horizontalalignment='center',
            verticalalignment='center',
            linespacing=2,
            fontsize=10,
            weight='bold')

        ax.text(0., -0.05, '(' + system_tx.split('\n')[0] + ')',
                horizontalalignment='center',
                verticalalignment='center',
                linespacing=2,
                fontsize=9,
                weight='bold')

    plt.legend(wedges, label_text,
                # title="Number of P-Reps Voted (ICX)",
                loc="lower left",
                bbox_to_anchor=(1, 0, 0.5, 1),
                fontsize=10)

    ax.set_title(title + ' (' + start_date + ' to ' + end_date + ')', fontsize=14, weight='bold', x=0.99, y=1.15)

    p=plt.gcf()
    p.gca().add_artist(my_circle)
    plt.axis('equal')
    # plt.show()
    plt.tight_layout()

    plt.savefig(os.path.join(thisweekresultPath, title.replace(' ', '_') + '_' + start_date + '_to_' + end_date + '.png'))


iglobal = get_iiss_info()['Iglobal']
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# today's date
# today = date.today()
today = datetime.utcnow()
date_today = today.strftime("%Y-%m-%d")

start_date = one_week_ago(yesterday(date_today))
end_date = yesterday(date_today)

# manual
# start_date = '2022-02-04'
# end_date = '2022-02-10'

thisweekresultPath = os.path.join(weeklyresultPath, start_date + '_to_' + end_date)
if not os.path.exists(thisweekresultPath):
    os.mkdir(thisweekresultPath)

date_of_interest = pd.date_range(start=start_date, end=end_date, freq='D').strftime("%Y-%m-%d").to_list()

print(date_of_interest)


tx_df = []
basic_stat_df = []
total_transfer_value_list = []
for these_dates in date_of_interest:

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ load  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    # data loading
    data = pd.read_csv(os.path.join(dataPath, 'tx_final_' + these_dates + '.csv'), low_memory=False)
    tx_df.append(data)

    # for token transfer value
    date_prev_underscore = these_dates.replace("-", "_")
    tokentransfer_date_Path = os.path.join(tokentransferPath, date_prev_underscore) 
    token_transfer_df = pd.read_csv(os.path.join(tokentransfer_date_Path, 'IRC_token_transfer_' + date_prev_underscore + '.csv'), low_memory=False)
    
    token_transfer_value = token_transfer_df['Value Transferred in USD'].sum()

    try:
        icx_price = token_transfer_df[token_transfer_df['IRC Token'] == 'ICX']['Price in USD'].iloc[0]
        icx_transfer_value = data['value'].sum() * icx_price
    
        total_transfer_value = icx_transfer_value + token_transfer_value
        total_transfer_value_list.append(total_transfer_value)
    except:
        pass

    #iglobal
    # basic_stat = pd.read_csv(os.path.join(basicstatPath, 'basic_icx_stat_df_' + these_dates + '.csv'), low_memory=False)
    # basic_stat_df.append(basic_stat)

try:
    total_transfer_value_text = 'Total Value Transferred: ~' + '{:,}'.format(int(sum(total_transfer_value_list))) + ' USD'
except:
    total_transfer_value_text = 'Total Value Transferred: Not Available'
        


tx_df = pd.concat(tx_df).reset_index(drop=True)

# inflation
# basic_stat_df = pd.concat(basic_stat_df).reset_index(drop=True)
# basic_stat_df['daily_issuance'] = basic_stat_df['iglobal']*12/365
# weekly_issuance = basic_stat_df['daily_issuance'].sum()


iglobal = get_iiss_info()['Iglobal']

# iglobal = 3_000_000
daily_issuance = iglobal*12/365
weekly_issuance = daily_issuance*7



tx_df = clean_tx_df(tx_df, from_this='from', to_this='to')



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX Address Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# =============================================================================
# # run
# =============================================================================

# known_address_url = request_sleep_repeat(url = 'https://iconwat.ch/data/thes', repeat=3, verify=False)
known_address_url = request_sleep_repeat(url = 'http://iconwat.ch/data/thes', repeat=3, verify=False)
# jknown_address = json.load(urlopen(known_address_url))
jknown_address = known_address_url.json()

## updating address info
{add_dict_if_noexist(k, jknown_address, v) for k,v in exchange_addresses.items()}
{add_dict_if_noexist(k, jknown_address, v) for k,v in other_addresses.items()}

known_address_details_to, known_address_details_from, known_address_exception = get_contract_info()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Analysis ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

tx_df = pd.merge(tx_df, known_address_details_from, on='from', how='left')
tx_df = pd.merge(tx_df, known_address_details_to, on='to', how='left')


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


# combine and add grouping
tos = [check_to, check_to_intTx, check_to_intEvtOth]
to_final = reduce(lambda left,right: pd.merge(left,right,on=['to','to_def']), tos)
to_final_grouping = grouping_wrapper(to_final, in_col='to_def')



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SAVE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
to_final_grouping.to_csv(os.path.join(thisweekresultPath, 'tx_summary_' + start_date + '_to_' + end_date + '.csv'), index=False)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SAVE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# to_group = to_final_grouping.groupby('group').agg('sum').sort_values(by='Regular Tx', ascending=False) #.reset_index()
to_group = to_final_grouping.groupby('group').agg('sum').sort_values(by='Fees burned', ascending=False) #.reset_index()

system_tx = to_group[to_group.index == 'System'].agg('sum')

system_tx = 'System Transactions: ' + '{:,}'.format(system_tx['Regular Tx'].astype(int) + system_tx['Internal Tx'].astype(int)) + '\n' + \
        'System Events (including Tx): ' + '{:,}'.format(system_tx['Regular Tx'].astype(int) + system_tx['Internal Tx'].astype(int) + system_tx['Internal Event (excluding Tx)'].astype(int))

totals = to_group.agg('sum')

weekly_burned_percentage = "{:.2%}".format(totals['Fees burned']/weekly_issuance)

all_tx = 'Total Transactions: ' + '{:,}'.format(totals['Regular Tx'].astype(int) + totals['Internal Tx'].astype(int)) + '\n' + \
        'Total Events (including Tx): ' + '{:,}'.format(totals['Regular Tx'].astype(int) + totals['Internal Tx'].astype(int) + totals['Internal Event (excluding Tx)'].astype(int)) + '\n' + \
                total_transfer_value_text

totals = totals.astype(int).map("{:,}".format)

to_group = to_group.rename(columns={'Fees burned': 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX)',
                        'Regular Tx': 'Regular Tx' + ' (' + totals['Regular Tx'] + ')',
                        'Internal Tx': 'Internal Tx' + ' (' + totals['Internal Tx'] + ')',
                        'Internal Event (excluding Tx)': 'Internal Event (excluding Tx: ' + totals['Internal Event (excluding Tx)'] + ')'})

fees_burned = 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX)'
fees_burned_label = 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX' + ' / ' + weekly_burned_percentage + ' of weekly inflation)'


plot_df = to_group.drop(columns=fees_burned)
plot_df = plot_df[plot_df.index !="System"] # getting rid of system from the graph


# stacked barplot
sns.set(style="dark")
plt.style.use("dark_background")
ax1 = plot_df.plot(kind='bar', stacked=True, figsize=(14, 8))
plt.title('Weekly Transactions' + ' (' + start_date + ' to ' + end_date + ')', fontsize=14, weight='bold', pad=25, loc='left')
ax1.set_xlabel('Destination Contracts/Addresses')
ax1.set_ylabel('Transactions', labelpad=10)
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=90, ha="center")
ax2 = ax1.twinx()
lines = plt.plot(to_group[fees_burned], marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black', markersize=8)

xmin, xmax = ax1.get_xlim()
ymin, ymax = ax1.get_ylim()

if ymax >= 1000:
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
if ymax >= 1000000:
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))

ax2.set_ylabel('Fees burned (ICX)', labelpad=10)
plt.setp(lines, 'color', 'white', 'linewidth', 1.0)
# ax1.legend(loc='upper center', bbox_to_anchor=(0.4, 0.95),
#           fancybox=True, shadow=True, ncol=5)
color = 'white'
m_line = mlines.Line2D([], [], color=color, label='Total ' + fees_burned_label, linewidth=1, marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black')
leg = plt.legend(handles=[m_line], loc='upper right', fontsize='medium', bbox_to_anchor=(0.98, 0.999), frameon=False)
for text in leg.get_texts():
    plt.setp(text, color='cyan')
plt.tight_layout(rect=[0,0,1,1])

xmin, xmax = ax1.get_xlim()
ymin, ymax = ax1.get_ylim()

ax1.text(xmax*0.97, ymax*0.82, all_tx,
        horizontalalignment='right',
        verticalalignment='center',
        linespacing = 1.5,
        fontsize=12,
        weight='bold')

handles, labels = ax1.get_legend_handles_labels()
ax1.legend(handles, labels,
        loc='upper right', bbox_to_anchor=(1, 1.08),
        frameon=False, fancybox=True, shadow=True, ncol=3)

ax1.text(xmax*1.07, ymax*-0.12, system_tx,
        horizontalalignment='right',
        verticalalignment='center',
        rotation=90,
        linespacing = 1.5,
        fontsize=8)

ax2.spines['right'].set_color('cyan')
ax2.yaxis.label.set_color('cyan')
ax2.tick_params(axis='y', colors="cyan")
        
plt.savefig(os.path.join(thisweekresultPath, 'tx_summary_' + start_date + '_to_' + end_date + '.png'))


# donuts
plot_df_donut = to_group[to_group.index !="System"]
plot_df_donut = plot_df_donut.reindex(sorted(plot_df_donut.columns), axis=1)

totals_reused = totals.reset_index().sort_values(by='index')
totals_reused = totals_reused.set_index(['index'])


plot_donut_df(df_col=0, title='Fees Burned', add_string=" ICX")
plot_donut_df(df_col=3, title='Regular Tx', add_string="")
plot_donut_df(df_col=2, title='Internal Tx', add_string="")
plot_donut_df(df_col=1, title='Internal Events', add_string="")

print(start_date + ' to ' + end_date + ' is done!')


def run_all():
    if __name__ == "__main__":
        run_all()
