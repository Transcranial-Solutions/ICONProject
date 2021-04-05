
#########################################################################
## Project: Exchange data extraction for ICX                           ##
## Date: April 2021                                                    ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# import json library
# import urllib
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
import os
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
from datetime import date, datetime
from datetime import timezone
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
import seaborn as sns
import scipy.stats as sp
from scipy import stats


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "09_exchange_data")
if not os.path.exists(outPath):
    os.mkdir(outPath)
resultsPath = os.path.join(outPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
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



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Coin Market Cap Data Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

which_coin = '2099' # ICON/$ICX
which_currency = 'USD' # USD, BTC, ETH
today = datetime.now()

def get_utc_timestampe(YYYY, MM, DD):
    dt = datetime(YYYY, MM, DD, 0, 0, 0, 0)
    return(pd.Timestamp(dt, tz='utc').timestamp())

ts1 = int(get_utc_timestampe(2017, 10, 27))
# ts2 = int(get_utc_timestampe(2021, 4, 1))
ts2 = int(pd.Timestamp(today, tz='utc').timestamp())

this_url = Request('https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?id=' +
                   which_coin +
                   '&convert=' + which_currency +
                   '&time_start=' + str(ts1) +
                   '&time_end=' + str(ts2),
                   headers={'User-Agent': 'Mozilla/5.0'})

# json format
jthis_url = json.load(urlopen(this_url))

# extracting icx price information by labels & combining strings into list
d = {'time_open': extract_values(jthis_url, 'time_open'),
     'time_close': extract_values(jthis_url, 'time_close'),
     'time_high': extract_values(jthis_url, 'time_high'),
     'time_low': extract_values(jthis_url, 'time_low'),
     'open_price': extract_values(jthis_url, 'open'),
     'close_price': extract_values(jthis_url, 'close'),
     'high_price': extract_values(jthis_url, 'high'),
     'low_price': extract_values(jthis_url, 'low'),
     'volume': extract_values(jthis_url, 'volume'),
     'market_cap': extract_values(jthis_url, 'market_cap'),
     'date': extract_values(jthis_url, 'timestamp')}

# removing first element -- timestamp of data extraction
d['date'].pop(0)

# convert into dataframe
df_price = pd.DataFrame(data=d)

def get_date_only(x):
    return(x.str.split('T').str[0])

# cleaning up dates
# time_cols = ['time_open', 'time_close', 'time_high', 'time_low', 'timestamp']
time_cols = ['date']
df_price = df_price.apply(lambda x: get_date_only(x) if x.name in time_cols else x)
df_price['return_price'] = df_price['close_price']/df_price['close_price'].shift()-1

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Data Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


measuring_interval = 'date' # // 'year' // 'month' // 'week' // "date" // "day"//
# terms = ['2021-11', '2021-10']
# weeks = ['2020-24', '2020-23']
# months = ['2020-05', '2020-06']
# years = ['2020']

# this_term = terms[0]
# last_term = terms[1]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ P-Rep Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting p-rep information from icon governance page
prep_list_url_reg = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000', headers={'User-Agent': 'Mozilla/5.0'})
prep_list_url_unreg = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000&grade=3', headers={'User-Agent': 'Mozilla/5.0'})

# json format
jprep_list_url_reg = json.load(urlopen(prep_list_url_reg))
jprep_list_url_unreg = json.load(urlopen(prep_list_url_unreg))

def extract_json(json_dict, reg_status):
    # extracting p-rep information by labels
    prep_address = extract_values(json_dict, 'address')
    prep_name = extract_values(json_dict, 'name')
    prep_country = extract_values(json_dict, 'country')
    prep_city = extract_values(json_dict, 'city')
    prep_logo = extract_values(json_dict, 'logo')

    # combining strings into list
    prep_d = {'address': prep_address,
         'name': prep_name,
         'country': prep_country,
         'city': prep_city,
         'logo': prep_logo}

    # convert into dataframe
    df = pd.DataFrame(data=prep_d)
    df['status'] = reg_status
    return(df)

prep_df = []
prep_df_reg = extract_json(jprep_list_url_reg, 'registered')
prep_df_unreg = extract_json(jprep_list_url_unreg, 'unregistered')
prep_df = pd.concat([prep_df_reg, prep_df_unreg]).reset_index(drop=True)

prep_address = prep_df['address']
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

    # fix week (week-00 into previous year week (week-52))
    fix_week = df['week'].str.contains("-00")
    df['temp_week'] = np.where(fix_week, df['year'].astype(int)-1, df['week'])
    df['temp_week'] = np.where(fix_week, df['temp_week'].astype(str) + '-52', df['week'])
    df = df.drop(columns=['week']).rename(columns={'temp_week':'week'})

    # df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%a")
    df.drop(columns=['created_at'], inplace=True)

    df['votes'] = pd.to_numeric(df['votes'])

    df = df.groupby(['validator_name', 'delegator', 'year', 'month', 'week', 'date', 'day']).agg('sum').reset_index()
    # df = df.groupby(['validator_name', 'delegator', 'year', measuring_interval]).agg('sum').reset_index()

    try:
       print("Votes for " + validator_name[0] + ": Done - " + str(count+1) + " out of " + str(len_prep_address))
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

ori_df = []
for task in as_completed(all_votes):
    ori_df.append(task.result())

print(f'Time taken: {time() - start}')

# all votes per wallet
df = pd.concat(ori_df)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Data -- by validator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# concatenate them into a dataframe
# get unique interval
unique_interval = df.drop_duplicates(measuring_interval)[[measuring_interval]].sort_values(measuring_interval).reset_index(drop=True)

df_delegator = df.groupby(['delegator', measuring_interval]).agg('sum').reset_index()
df_delegator = df_delegator.sort_values(by=['delegator', measuring_interval]).reset_index(drop=True)
df_delegator['cum_votes'] = df_delegator.groupby(['delegator'])['votes'].cumsum()

# get first and last to get the days staked (roughly) -- not entirely accurate
test = df_delegator.groupby('delegator').nth([0,-1]).reset_index()
test['date'] = pd.to_datetime(test['date'])
test['date_lag'] = test.groupby('delegator')['date'].shift()
test['date_diff'] = test['date'] - test['date_lag']
test = test[~test['date_diff'].isna()]
test['days'] = test['date_diff'].dt.days.astype('int16')
test = test[['delegator','days']]

df_delegator = pd.merge(df_delegator, test, on='delegator', how='left')
df_delegator = df_delegator[~df_delegator['days'].isna()]



## overall

def corr_plot(df, inVar1, inVar2, this_xlabel, this_ylabel, this_title):

    # m_var1 = df.groupby('validator_name')[[inVar1]].apply(np.median).reset_index(name='var1')
    # m_var2 = df.groupby('validator_name')[[inVar2]].apply(np.median).reset_index(name='var2')
    # m_days_ranking = pd.merge(m_var1, m_var2, on='validator_name', how='left')

    # plot
    sns.set(style="ticks")
    plt.style.use("dark_background")
    g = sns.jointplot(inVar1, inVar2, data=df,
                      kind="reg", height=6, truncate=False, color='m',
                      joint_kws={'scatter_kws': dict(alpha=0.5)}
                      ).annotate(sp.pearsonr)
    g.set_axis_labels(this_xlabel, this_ylabel, fontsize=14, weight='bold', labelpad=10)
    g = g.ax_marg_x
    g.set_title(this_title, fontsize=12, weight='bold')
    plt.tight_layout()

## unstaking - negative icx only

# df_neg = df_delegator[df_delegator['votes'] > 0]
# df_neg = df_delegator[df_delegator['votes'] < 0]
# df_neg = df_delegator[df_delegator['votes'] < -500000]

# df_neg = df_delegator[df_delegator['cum_votes'] < 0.001]
# df_neg = df_delegator.copy()


day_1 = "2020-10-01"
day_2 = "2021-04-01"




df_overall = df_neg.groupby([measuring_interval]).agg('sum').reset_index()
df_overall = df_overall.sort_values(by=[measuring_interval]).reset_index(drop=True)

# adding price info on overall df
df_overall = pd.merge(df_overall, df_price, on='date', how='left')
df_overall.dropna(subset=["market_cap"], inplace=True)
df_overall['lag_votes'] = df_overall['votes'].shift(8)

# df_overall = df_overall[df_overall['close_price'] > 1]
# df_overall = df_overall[df_overall['close_price'] < 1.1]


date_of_interest = pd.date_range(start=day_1, end=day_2, freq='D').strftime("%Y-%m-%d").to_list()

df_overall = df_overall[df_overall['date'].isin(date_of_interest)]


def drop_numerical_outliers(df, z_thresh=3):
    # Constrains will contain `True` or `False` depending on if it is a value below the threshold.
    constrains = df.select_dtypes(include=[np.number]) \
        .apply(lambda x: np.abs(stats.zscore(x)) < z_thresh, result_type='reduce') \
        .all(axis=1)
    return(df.drop(df.index[~constrains]))

df_test = drop_numerical_outliers(df_overall)
# df_test = df_overall.copy()

corr_plot(df_test,
          'votes',
          'return_price',
          'Vote Change',
          'ICX Price' + ' (' + which_currency + ')',
          'Votes vs ICX Price' + '\n(' + day_1 + ' ~ ' + day_2 + ')')

corr_plot(df_test,
          'lag_votes',
          'return_price',
          'Vote Change',
          'ICX Price' + ' (' + which_currency + ')',
          'Votes vs ICX Price (8 days lag)' + '\n(' + day_1 + ' ~ ' + day_2 + ')')



fig = plt.figure()
ax = plt.axes()
zscore_df = df_test.copy()
zscore_df = zscore_df[['lag_votes','votes','return_price']]

for col in zscore_df:
    col_zscore = col + '_zscore'
    zscore_df[col_zscore] = (zscore_df[col] - zscore_df[col].mean())/zscore_df[col].std(ddof=0)


from scipy.ndimage.filters import gaussian_filter1d

y = zscore_df['return_price_zscore']
x = df_test['date']
ysmoothed = gaussian_filter1d(y, sigma=2)
plt.plot(x, ysmoothed)

# y = zscore_df['votes_zscore']
# x = df_test['date']
# ysmoothed = gaussian_filter1d(y, sigma=2)
# plt.plot(x, ysmoothed)
# plt.show()

y = zscore_df['lag_votes_zscore']
x = df_test['date']
ysmoothed = gaussian_filter1d(y, sigma=2)
plt.plot(x, ysmoothed)



# plt.plot(df_overall['date'], zscore_df['close_price_zscore'])
# plt.plot(df_overall['date'], zscore_df['votes_zscore'])



#
# from sklearn.cluster import KMeans
#
# cluster_df = df_test[['lag_votes', 'close_price']]
#
# kmeans = KMeans(n_clusters=2).fit(cluster_df)
# centroids = kmeans.cluster_centers_
# print(centroids)
#
# plt.scatter(cluster_df['lag_votes'], cluster_df['close_price'], c=kmeans.labels_.astype(float), s=50, alpha=0.5)
# plt.scatter(centroids[:, 0], centroids[:, 1], c='red', s=50)
# plt.show()
#
#
#
#
#
# from sklearn.cluster import SpectralClustering
# model = SpectralClustering(n_clusters=3, affinity='nearest_neighbors',
#                            assign_labels='kmeans')
# labels = model.fit_predict(cluster_df)
# plt.scatter(cluster_df['lag_votes'], cluster_df['close_price'], c=labels,
#             s=50, cmap='viridis');










