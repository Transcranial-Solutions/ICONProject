#########################################################################
## Project: Misc.                                                      ##
## Date: December 2021                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# High-level information from ICX tracker

# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
# import numpy as np
import os
from datetime import date, datetime, timedelta
import glob

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# path to save
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")

resultPath = os.path.join(inPath, "icon_tracker")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)
dataPath = os.path.join(resultPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ICX tracker Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
tracker_url = Request('https://tracker.icon.foundation/v3/main/mainInfo', headers={'User-Agent': 'Mozilla/5.0'})
jtracker_url = json.load(urlopen(tracker_url))

def extract_json(json_dict):
    marketCap = extract_values(json_dict, 'marketCap')
    icxSupply = extract_values(json_dict, 'icxSupply')
    icxCirculationy = extract_values(json_dict, 'icxCirculationy')
    publicTreasury = extract_values(json_dict, 'publicTreasury')

    # combining strings into list
    icxdata = {'marketCap': marketCap,
         'icxSupply': icxSupply,
         'icxCirculation': icxCirculationy,
         'publicTreasury': publicTreasury}

    # convert into dataframe
    return(pd.DataFrame(data=icxdata))



icx_df = extract_json(jtracker_url)

# today date
# today = date.today()
today = datetime.utcnow()
day_today = today.strftime("%Y-%m-%d")
day_excel = today.strftime("%d/%m/%Y")

icx_df.insert(loc=0, column='date', value=day_excel)

# save as csv
icx_df.to_csv(os.path.join(dataPath, 'basic_icx_stat_df_' + day_today + '.csv'), index=False)

listData = glob.glob(os.path.join(dataPath, "basic*.csv"))


all_df =[]
for k in range(len(listData)):
    df = pd.read_csv(listData[k]).head(1)
    all_df.append(df)

df = pd.concat(all_df)
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
df.index = pd.DatetimeIndex(df['date']).floor('D')
all_days = pd.date_range(df.index.min(), df.index.max(), freq='D')
df = df.reindex(all_days)
df.reset_index(inplace=True)
df = df.drop(columns='date').rename(columns={'index':'date'})
df['date'] = df['date'].dt.date
df.set_index('date', inplace=True)

# interpolate
df = df.interpolate()

# putting back the date
df = df.reset_index()

df.to_csv(os.path.join(resultPath, 'icx_tracker_compiled.csv'), index=False)
