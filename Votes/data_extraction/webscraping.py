
#########################################################################
## Project: ICON Vote Data Visualisation                               ##
## Date: May 2020                                                      ##
## Author: Tono (Transcranial Solutions)                               ##
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
# as the author has not yet learned to extract the data from the chain.
# It will be updated in the future.

# Extracts P-RREP information (name, country, city, logo, rewards, etc) from https://tracker.icon.foundation/
# Extracts vote information (delegator, validator, amount, etc) from https://iconvotemonitor.com/
# Big shout-out to Everstake!
# Data put together in a DataFrame, saved separately in CSV format.

# import json library
import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import os
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = currPath + '\\output\\'
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

# combining strings into list
prep_d = {'address': prep_address,
     'name': prep_name,
     'country': prep_country,
     'city': prep_city}

# convert into dataframe
prep_df = pd.DataFrame(data=prep_d)

# export to csv
prep_df.to_csv(outPath + 'icon_prep_details.csv', index=False)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ logos ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# extract logos separately
prep_logo = extract_values(jprep_list_url, 'logo')

# saving path for logos
logoPath = outPath + '\\logos\\'
if not os.path.exists(logoPath):
    os.mkdir(logoPath)

# going through each site to download image, based on 3 different file types
for i in range(len(prep_logo)):
    try:
        if prep_logo[i].split('.')[-1].lower() == "png":
            filetype = ".png"
        elif prep_logo[i].split('.')[-1].lower() == "svg":
            filetype = ".svg"
        elif prep_logo[i].split('.')[-1].lower() == "jpg":
             filetype = ".jpg"
        else:
            pass

        urllib.request.urlretrieve(prep_logo[i], logoPath + prep_name[i] + filetype)
        print(prep_name[i] + ": Done - " + str(i+1) + ' out of ' + str(len(prep_logo)))

    except:
        pass



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ P-REP Reward Info~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# saving path for voting data per each P-REP
rewardsPath = outPath + '\\rewards\\'
if not os.path.exists(rewardsPath):
    os.mkdir(rewardsPath)


for j in range(len(prep_address)):

    # getting p-rep reward information from icon tracker page
    ## first getting number of total pages
    prep_rewards_url = Request('https://tracker.icon.foundation/v3/address/claimIScoreList?count=1&address=' + prep_address[j],
                               headers={'User-Agent': 'Mozilla/5.0'})

    jprep_rewards_url = json.load(urlopen(prep_rewards_url))
    totalSize = extract_values(jprep_rewards_url, 'totalSize')

    # then apply total pages to extract correct amount of data
    prep_rewards_url = Request('https://tracker.icon.foundation/v3/address/claimIScoreList?count=' + str(totalSize)[1:-1] + '&address=' + prep_address[j],
                               headers={'User-Agent': 'Mozilla/5.0'})

    # json format
    jprep_rewards_url = json.load(urlopen(prep_rewards_url))

    # extracting information by labels
    rewards_address = extract_values(jprep_rewards_url, 'address')
    rewards_block = extract_values(jprep_rewards_url, 'height')
    rewards_date = extract_values(jprep_rewards_url, 'createDate')
    rewards_icx = extract_values(jprep_rewards_url, 'icx')

    rewards_d=[]
    rewards_df=[]
    # combining strings into list
    rewards_d = {'address': rewards_address,
         'block_id': rewards_block,
         'date': rewards_date,
         'rewards': rewards_icx}

    # convert into dataframe
    rewards_df = pd.DataFrame(data=rewards_d)

    # export to csv
    rewards_df.to_csv(rewardsPath + prep_address[j] + '_prep_rewards.csv', index=False)

    print("Rewards for " + prep_address[j] + ": Done - " + str(j + 1) + " out of " + str(len(prep_address)))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# saving path for voting data per each P-REP
votePath = outPath + '\\votes\\'
if not os.path.exists(votePath):
    os.mkdir(votePath)

# loop through P-Rep addresses and extract information, make into data frame, save as .csv
for k in range(len(prep_address)):

    # Request url, no limit here
    # req = Request('https://api.iconvotemonitor.com/delegations?validators=hx2f3fb9a9ff98df2145936d2bfcaa3837a289496b', headers={'User-Agent': 'Mozilla/5.0'})
    req = Request('https://api.iconvotemonitor.com/delegations?validators='+ prep_address[k], headers={'User-Agent': 'Mozilla/5.0'})

    jreq = json.load(urlopen(req))

    # extracting data by labels
    block_id = extract_values(jreq, 'block_id')
    delegator = extract_values(jreq, 'delegator')
    validator = extract_values(jreq, 'validator')
    votes = extract_values(jreq, 'amount')
    created_at = extract_values(jreq, 'created_at')
    # validator_name = extract_values(jreq, 'validator_name')

    d=[]
    df=[]
    # combining strings into list
    d = {'block_id': block_id,
         'delegator': delegator,
         'validator': validator,
         'votes': votes,
         'created_at': created_at}
         #'validator_name': validator_name

     # convert into dataframe
     df = pd.DataFrame(data=d)

     df.to_csv(votePath + validator[0] + '.csv', index=False)

     print("Votes for " + validator[0] + ": Done - " + str(k+1) + " out of " + str(len(prep_address)))
