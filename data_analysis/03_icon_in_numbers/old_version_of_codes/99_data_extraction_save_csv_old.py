#########################################################################
## Project: ICON in Numbers                                            ##
## Date: July 2020 (updated Oct 2020)                                  ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# This is for 'ICON in Numbers' weekly series.
# It is an automated figure generator based on the time of your choosing (mainly tailored for weekly).
# Terms will require the timeframe to have finished (e.g. this may not work as intended if week 25 is still on-going).
# This will webscrape iconvotemonitor.com by Everstake and also P-Rep information from ICON Foundation site.
# It will then do data manipulation, recoding, calculation, aggregation and generate multiple figures.
# Please note that depending on the amount of vote change, the scale may be off, and needs to be manually modified (ylim mostly).


# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "03_icon_in_numbers")
if not os.path.exists(outPath):
    os.mkdir(outPath)
resultsPath = os.path.join(outPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

measuring_interval = 'week' # // 'year' // 'month' // 'week' // "date" // "day"//
terms = ['2020-47', '2020-46']
# weeks = ['2020-24', '2020-23']
# months = ['2020-05', '2020-06']
# years = ['2020']

this_term = terms[0]
last_term = terms[1]
# this_week = weeks[0]
# this_month = months[0]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
resultsPath_interval = os.path.join(resultsPath, this_term)
if not os.path.exists(resultsPath_interval):
    os.mkdir(resultsPath_interval)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

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
len_prep_address = len(prep_address)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

count = 0
def get_votes(prep_address, len_prep_address):
    global count

    # Request url, no limit here
    req = Request('https://api.iconvotemonitor.com/delegations?validators=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})

    # req = Request('https://api.iconvotemonitor.com/delegations?validators='+ prep_address[k] + '&limit=' + str(max_extract_count),
    #               headers={'User-Agent': 'Mozilla/5.0'})

    jreq = json.load(urlopen(req))

    # extracting data by labels
    # block_id = extract_values(jreq, 'block_id')
    delegator = extract_values(jreq, 'delegator')
    # validator = extract_values(jreq, 'validator')
    votes = extract_values(jreq, 'amount')
    created_at = extract_values(jreq, 'created_at')
    validator_name = extract_values(jreq, 'validator_name')

    # combining strings into list
    d = {# 'block_id': block_id,
         'delegator': delegator,
         # 'validator': validator,
         'validator_name': validator_name,
         'votes': votes,
         'created_at': created_at}

    # convert into dataframe
    df = pd.DataFrame(data=d)

    # convert timestamp into Year & measuring_interval (Week/Month), and summarise data by year + measuring_interval
    # df['datetime'] = pd.to_datetime(df['created_at'], unit = 's').dt.strftime("%Y-%m-%d %H:%M:%S")
    df['year'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y")
    df['month'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m")
    df['week'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%U")
    df['date'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d")
    df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%U-%a")

    # if measuring_interval == 'week':
    #     df[measuring_interval] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%U")
    # elif measuring_interval == 'month':
    #     df[measuring_interval] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%m")

    # df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%a")
    df.drop(columns=['created_at'], inplace=True)

    df['votes'] = pd.to_numeric(df['votes'])

    df = df.groupby(['validator_name', 'delegator', 'year', 'month', 'week', 'date', 'day']).agg('sum').reset_index()
    # df = df.groupby(['validator_name', 'delegator', 'year', measuring_interval]).agg('sum').reset_index()

    try:
       print("Votes for " + validator_name[0] + ": Done - " + str(count) + " out of " + str(len_prep_address))
    except:
       print("An exception occurred - Possibly a new P-Rep without votes")

    count += 1

    return(df)

# threading
start = time()

all_votes = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(len(prep_address)):
        all_votes.append(executor.submit(get_votes, prep_address[k], len_prep_address))

temp_df = []
for task in as_completed(all_votes):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')

# all votes per wallet
df = pd.concat(temp_df)


df.to_csv(os.path.join(resultsPath, 'iconvotemonitor.csv'), index=False)
