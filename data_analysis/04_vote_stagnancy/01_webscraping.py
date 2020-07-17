
#########################################################################
## Project: ICON Vote Stagnancy Investigation                          ##
## Date: July 2020                                                     ##
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

# Extracts P-RREP information from https://tracker.icon.foundation/
# Extracts vote information (delegator, validator, amount, etc) from https://iconvotemonitor.com/
# Big shout-out to Everstake!
# Data put together in a DataFrame, saved separately in CSV format.

# import json library
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
from datetime import datetime
import os
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "output")
if not os.path.exists(outPath):
    os.mkdir(outPath)


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

     # convert timestamp into date & day
     df['datetime'] = pd.to_datetime(df['created_at'], unit = 's').dt.strftime("%Y-%m-%d %H:%M:%S")
     df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%a")

     # write to csv
     df.to_csv(os.path.join(votesPath, validator[0] + '.csv'), index=False)

        try:
            print("Votes for " + validator_name[0] + ": Done - " + str(k + 1) + " out of " + str(len(prep_address)))
        except:
            print("An exception occurred - Possibly a new P-Rep without votes")