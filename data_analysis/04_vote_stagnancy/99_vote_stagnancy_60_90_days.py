
#########################################################################
## Project: ICON Vote Stagnancy Investigation                          ##
## Date: November 2020                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Webscraping

# This file extracts information from two main different sources
# (https://iconvotemonitor.com/) & (https://tracker.icon.foundation/)

# Extracts vote information (delegator, validator, amount, etc) from https://iconvotemonitor.com/
# Big shout-out to Everstake!
# Data put together in a DataFrame, saved in CSV format.

# import json library
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
from datetime import date
import os
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# today date
today = date.today()
day1 = '_' + today.strftime("%Y_%m_%d")

# making path for loading/saving
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outPath = os.path.join(currPath, "04_vote_stagnancy")
resultsPath = os.path.join(outPath, "results" + day1)
if not os.path.exists(resultsPath):
   os.mkdir(resultsPath)

###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~###

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ P-Rep Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting p-rep information from icon governance page
prep_list_url = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000', headers={'User-Agent': 'Mozilla/5.0'})

# json format
jprep_list_url = json.load(urlopen(prep_list_url))

# extracting p-rep information by labels
prep_address = extract_values(jprep_list_url, 'address')
prep_name = extract_values(jprep_list_url, 'name')
prep_country = extract_values(jprep_list_url, 'country')
prep_city = extract_values(jprep_list_url, 'city')
prep_logo = extract_values(jprep_list_url, 'logo')

# combining strings into list
prep_d = {'address': prep_address,
     'name': prep_name,
     'country': prep_country,
     'city': prep_city,
     'logo': prep_logo}

# convert into dataframe
prep_df = pd.DataFrame(data=prep_d)

# export to csv
prep_df.to_csv(os.path.join(outPath, 'icon_prep_details.csv'), index=False)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# saving path for voting data per each P-REP
votesPath = os.path.join(outPath, "votes")
if not os.path.exists(votesPath):
    os.mkdir(votesPath)


all_df = []

# loop through P-Rep addresses and extract information, make into data frame, save as .csv
for k in range(len(prep_address)):

    # Request url, no limit here
    req = Request('https://api.iconvotemonitor.com/delegations?validators='+ prep_address[k], headers={'User-Agent': 'Mozilla/5.0'})

    jreq = json.load(urlopen(req))

    # extracting data by labels
    block_id = extract_values(jreq, 'block_id')
    delegator = extract_values(jreq, 'delegator')
    validator = extract_values(jreq, 'validator')
    votes = extract_values(jreq, 'amount')
    created_at = extract_values(jreq, 'created_at')
    validator_name = extract_values(jreq, 'validator_name')

    d=[]
    df=[]
    # combining strings into list
    d = {'block_id': block_id,
         'delegator': delegator,
         'validator': validator,
         'validator_name': validator_name,
         'votes': votes,
         'created_at': created_at}

    # convert into dataframe
    df = pd.DataFrame(data=d)

    df['votes'] = pd.to_numeric(df['votes'])

    all_df.append(df)

    try:
        print("Votes for " + validator_name[0] + ": Done - " + str(k + 1) + " out of " + str(len(prep_address)))
    except:
        print("An exception occurred - Possibly a new P-Rep without votes")


df = pd.concat(all_df)

# convert timestamp into date & day
# df['year'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y")
# df['month'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m")
# df['week'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%U")
df['date'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d")
df['datetime'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")

# getting date different from today and transaction datte
df['date_diff'] = (pd.to_datetime('today') - pd.to_datetime(df['created_at'], unit='s')).dt.days

# master data
temp_df = df.sort_values(by=['delegator','validator_name','created_at'], ascending=True).reset_index(drop=True)
# temp_df['cumsum_by_prep'] = temp_df.groupby(['delegator','validator_name'])['votes'].cumsum()
temp_df['sum_votes_by_prep'] = temp_df.groupby(['delegator','validator_name'])['votes'].transform('sum')
temp_df['sum_votes'] = temp_df.groupby(['delegator'])['votes'].transform('sum')
# temp_df = temp_df.sort_values(by=['delegator','created_at'], ascending=True).reset_index(drop=True)
# temp_df['cumsum'] = temp_df.groupby(['delegator'])['votes'].cumsum()


# current wallets that are taking part in voting (who has not left)
active_wallets = temp_df[temp_df['sum_votes'] > 0.0001]
active_wallets = active_wallets[active_wallets['sum_votes_by_prep'] > 0.0001]
active_wallets = active_wallets.groupby(['delegator', 'validator_name']).last().reset_index()

# getting unique active wallets
active_wallets_unique = active_wallets.drop_duplicates(['delegator','sum_votes','date_diff'])[['delegator','sum_votes','date_diff']]
active_wallets_unique = active_wallets_unique.sort_values(by=['delegator','date_diff']).groupby('delegator').first().reset_index()

# getting p-rep ranking here
prep_ranking = active_wallets.groupby('validator_name')['sum_votes_by_prep'].agg('sum').reset_index().\
    sort_values('sum_votes_by_prep', ascending=False).reset_index(drop=True)
prep_ranking['prep_ranking'] = range(1, len(prep_ranking)+1)

# 60 days +
del_60_plus = active_wallets_unique[active_wallets_unique['date_diff'] >= 60][['delegator']]
del_60_plus = del_60_plus.merge(active_wallets, on='delegator', how='left')
del_60_plus = del_60_plus.groupby('validator_name')['sum_votes_by_prep'].agg('sum').reset_index().rename(columns={'sum_votes_by_prep':'60_days'})

# 90 days +
del_90_plus = active_wallets_unique[active_wallets_unique['date_diff'] >= 90][['delegator']]
del_90_plus = del_90_plus.merge(active_wallets, on='delegator', how='left')
del_90_plus = del_90_plus.groupby('validator_name')['sum_votes_by_prep'].agg('sum').reset_index().rename(columns={'sum_votes_by_prep':'90_days'})


df_merged = prep_ranking.\
    merge(del_60_plus, on='validator_name', how='left').\
    merge(del_90_plus, on='validator_name', how='left').fillna(0)

# output
df_merged.to_csv(os.path.join(resultsPath, 'vote_stagnancy_60_90_days' + day1 + '.csv'), index=False)

