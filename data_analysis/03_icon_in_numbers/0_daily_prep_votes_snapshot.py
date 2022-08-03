#########################################################################
## Project: ICON in Numbers                                            ##
## Date: May 2021                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# This is for 'ICON in Numbers' series.
# Daily snapshot of delegation for icon in numbers series.

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
import os
from datetime import date, datetime, timedelta

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outDataPath = os.path.join(inPath, "prep_votes")
if not os.path.exists(outDataPath):
    os.mkdir(outDataPath)

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

req = Request('https://api.iconvotemonitor.com/validators', headers={'User-Agent': 'Mozilla/5.0'})
jreq = json.load(urlopen(req))

validator = extract_values(jreq, 'address')
validator_name = extract_values(jreq, 'name')
votes = extract_values(jreq, 'delegated_amount')

d = {'validator': validator,
    'validator_name': validator_name,
    'cum_votes_update': votes}

df = pd.DataFrame(data=d)

today = datetime.utcnow()
day_today = today.strftime("%Y-%m-%d")
day1 = today.strftime("%Y_%m_%d")

df.to_csv(os.path.join(outDataPath, 'prep_votes_' + day1 + '.csv'), index=False)
