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
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Data -- by validator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# concatenate them into a dataframe -- by validator_name
unique_date = df.drop_duplicates(['year', 'month', 'week', 'date', 'day'])[['year', 'month', 'week', 'date', 'day']].sort_values('date')

# get unique interval
unique_interval = df.drop_duplicates(measuring_interval)[[measuring_interval]].sort_values(measuring_interval).reset_index(drop=True)

df = df.groupby(['validator_name', 'delegator', measuring_interval]).agg('sum').reset_index()
df = df.sort_values(by=['validator_name', 'delegator', measuring_interval]).reset_index(drop=True)

# hope you don't mind, just shortening your names
df.loc[df['validator_name'] == 'ICONIST VOTE WISELY - twitter.com/info_prep', 'validator_name'] = 'ICONIST VOTE WISELY'
df.loc[df['validator_name'] == 'Piconbello { You Pick We Build }', 'validator_name'] = 'Piconbello'
df.loc[df['validator_name'] == 'UNBLOCK {ICX GROWTH INCUBATOR}', 'validator_name'] = 'UNBLOCK'
df.loc[df['validator_name'] == 'Gilga Capital (NEW - LETS GROW ICON)', 'validator_name'] = 'Gilga Capital'


# pivot wider & longer to get all the longitudinal data
df_wider = df.pivot_table(index=['validator_name', 'delegator'],
                          columns=[measuring_interval],
                          values='votes').reset_index()

df_longer = df_wider.melt(id_vars=['validator_name', 'delegator'], var_name=[measuring_interval], value_name='votes')
df_longer = df_longer.sort_values(by=['validator_name', 'delegator', measuring_interval, 'votes']).reset_index(drop=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# removing NaN to first non-NaN
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])[measuring_interval].cumcount()==0,'remove_this']= '1'
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])['votes'].apply(pd.Series.first_valid_index), 'remove_this'] = '2'
df_longer['remove_this'] = df_longer['remove_this'].ffill()
df_longer = df_longer[df_longer['remove_this'] != '1'].drop(columns='remove_this').reset_index(drop=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# getting the duration of interest (so that the data does not get cut off)
df_longer = df_longer[df_longer[measuring_interval] <= this_term]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# vote status -> voted, add cumulative votes & unvoted
df_longer['cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['votes'].cumsum()
df_longer['cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['cum_votes'].ffill()

# cumulative votes shifting to give proper vote/unvote status
df_longer['prev_cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['cum_votes'].shift()

# fill cumulative votes, make between -1e-6 and 1e-6 to zero
df_longer.loc[df_longer['votes'].between(-1e-6, 1e-6), 'votes'] = 0
df_longer.loc[df_longer['prev_cum_votes'].between(-1e-6, 1e-6), 'prev_cum_votes'] = 0
df_longer.loc[df_longer['cum_votes'].between(-1e-6, 1e-6), 'cum_votes'] = 0

# vote/unvote status
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])[measuring_interval].cumcount()==0,'vote_status_A']= 'voted'
df_longer.loc[df_longer['prev_cum_votes'].between(-1e-6, 1e-6) & ~np.isnan(df_longer['votes']), 'vote_status_A']= 'voted'
df_longer.loc[df_longer['cum_votes'].between(-1e-6, 1e-6) & ~np.isnan(df_longer['votes']), 'vote_status_B'] = 'unvoted'

# getting rid of non-needed rows (rows before first vote & after unvote)
remove_these = df_longer['cum_votes'].between(-1e-6, 1e-6) & df_longer['prev_cum_votes'].between(-1e-6, 1e-6)
df_longer = df_longer[~remove_these].drop(columns='prev_cum_votes')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# how many p-rep voted
## removing 'unvoted' for counting number of p-reps each voter voted per measuring_interval
a = df_longer['vote_status_B'] != 'unvoted'
count_voted_prep_per_measuring_interval = df_longer.copy()
count_voted_prep_per_measuring_interval['vote_status_A'] = count_voted_prep_per_measuring_interval.\
    groupby(['delegator'])['vote_status_A'].ffill()
count_voted_prep_per_measuring_interval = count_voted_prep_per_measuring_interval[a].\
    groupby(['delegator', measuring_interval]).\
    count()['vote_status_A'].reset_index().\
    rename(columns={'vote_status_A': 'how_many_prep_voted'})


# merge with df_longer
df_longer = pd.merge(df_longer,
                     count_voted_prep_per_measuring_interval,
                     on=['delegator', measuring_interval],
                     how='left')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# new wallet by measuring_interval -- first appearing wallet count
new_wallet_A = df_longer.sort_values(by = ['delegator', measuring_interval]).groupby('delegator').first().reset_index()
new_wallet_A = new_wallet_A.drop_duplicates(['delegator', measuring_interval])[['delegator', measuring_interval]]
new_wallet_A['new_wallet_A'] = 'voted'

# merge new_wallet with df_longer
df_longer = pd.merge(df_longer, new_wallet_A, on=['delegator', measuring_interval], how='left')


# adding new_wallet_B which shows unvoted & left
df_longer.loc[df_longer['how_many_prep_voted'].isnull(), 'unvoted_and_left'] = 1

# adding cumulative unvoted_and_left
cum_unvoted_and_left = df_longer.drop_duplicates(['delegator', measuring_interval, 'unvoted_and_left'])[['delegator', measuring_interval, 'unvoted_and_left']].\
    dropna().sort_values(by=['delegator', measuring_interval], ascending=True).reset_index(drop=True)
cum_unvoted_and_left['cum_unvoted_and_left'] = cum_unvoted_and_left.groupby(['delegator'])['unvoted_and_left'].transform('cumsum')
cum_unvoted_and_left = cum_unvoted_and_left.drop(columns=['unvoted_and_left'])
df_longer = pd.merge(df_longer, cum_unvoted_and_left, on=['delegator', measuring_interval], how='left')


# getting last time_interval (for those who left permanently)
df_longer = df_longer.sort_values(by = ['delegator', measuring_interval], ascending=True) # not by validator_name here!
lasts = df_longer.groupby(['delegator'])[measuring_interval].last().reset_index().rename(columns={measuring_interval: 'last_interval'})
df_longer = pd.merge(df_longer, lasts, on=['delegator'], how='left')

# having unvoted temporarily (who left and came back) and unvoted permanently (up to the date chosen) who never came back
df_longer.loc[~df_longer['unvoted_and_left'].isnull(), 'stopped_voting'] = 'unvoted'
df_longer.loc[(df_longer[measuring_interval] != df_longer['last_interval']) & (~df_longer['stopped_voting'].isnull()), 'stopped_voting_status'] = 'temporary'
df_longer.loc[(df_longer[measuring_interval] == df_longer['last_interval']) & (~df_longer['stopped_voting'].isnull()), 'stopped_voting_status'] = 'permanent'
df_longer.loc[(df_longer[measuring_interval] == df_longer['last_interval']) & (~df_longer['stopped_voting'].isnull()), 'new_wallet_B'] = 'unvoted' # last disappearing wallet count (A -> B), separately for counting
df_longer = df_longer.drop(columns=['last_interval'])


# adding returned voting (after leaving temporarily) -- note that it also includes first voting
df_longer['lag_stopped_voting_status'] = df_longer.groupby('delegator')['stopped_voting_status'].shift()
df_longer['lag_cum_unvoted_and_left'] = df_longer.groupby('delegator')['cum_unvoted_and_left'].shift()

v = df_longer['new_wallet_A'] == 'voted'

rv = ((df_longer['vote_status_A'] == 'voted') &
      (df_longer['lag_stopped_voting_status'] == 'temporary') &
      (df_longer['how_many_prep_voted'].notna()))

rv2 = ((df_longer['vote_status_A'] == 'voted') &
       (df_longer['vote_status_B'] == 'unvoted') &
       (df_longer['how_many_prep_voted'].isnull()) &
       (df_longer['lag_stopped_voting_status'] == 'temporary') &
       (df_longer['lag_cum_unvoted_and_left'] != df_longer['cum_unvoted_and_left']))

df_longer.loc[(v)|(rv)|(rv2), 'returned_voting'] = 'voted'
df_longer.loc[v, 'returned_voting_status'] = 'first'
df_longer.loc[(rv)|(rv2), 'returned_voting_status'] = 'returned'
# df_longer.loc[rv2, 'returned_voting_status'] = 'returned_left'
df_longer['returned_voting'] = df_longer.groupby(['delegator', measuring_interval])['returned_voting'].ffill()
df_longer['returned_voting_status'] = df_longer.groupby(['delegator', measuring_interval])['returned_voting_status'].ffill()
df_longer = df_longer.drop(columns=['lag_stopped_voting_status', 'lag_cum_unvoted_and_left'])

# just to have the number without decimals
def remove_decimal_with_int(df, inVar):
    df[inVar] = df[inVar].fillna(0).astype(int).astype(object).where(df[inVar].notnull())

# list to convert
lst = ['how_many_prep_voted' , 'unvoted_and_left', 'cum_unvoted_and_left']
for x in lst:
    remove_decimal_with_int(df_longer, x)

df_longer = df_longer[['validator_name', 'delegator', measuring_interval, 'votes', 'cum_votes',
                       'vote_status_A', 'vote_status_B', 'returned_voting', 'stopped_voting', 'returned_voting_status', 'stopped_voting_status',
                       'new_wallet_A', 'new_wallet_B', 'unvoted_and_left', 'cum_unvoted_and_left', 'how_many_prep_voted']]

# pd.crosstab(df_longer['unvoted_and_left'].fillna('missing'), df_longer['vote_status_B'].fillna('missing'), margins=True)

##################### SAVE HERE?


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Table for Count ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Main table

# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # getting the duration of interest (so that the data does not get cut off)
# df_longer = df_longer[df_longer[measuring_interval] <= this_term]
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# Voter count table ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
vote_status_count = df_longer.groupby(['validator_name', measuring_interval]).agg('count').reset_index()
vote_status_count = vote_status_count.\
    drop(columns=['delegator', 'votes', 'cum_votes', 'how_many_prep_voted', 'unvoted_and_left',
                  'cum_unvoted_and_left']).\
    rename(columns={'vote_status_A': 'Voted', 'vote_status_B': 'Unvoted',
                    'returned_voting': 'U_Voted', 'stopped_voting': 'U_Unvoted', ## these are for counts per week overall
                    'new_wallet_A': 'new_wallet_Voted', 'new_wallet_B': 'new_wallet_Unvoted'})

vote_status_count['Voter_diff'] = vote_status_count['Voted'] - vote_status_count['Unvoted']
# vote_status_count['U_Voter_diff'] = vote_status_count['U_Voted'] - vote_status_count['U_Unvoted']
vote_status_count['new_Voter_diff'] = vote_status_count['new_wallet_Voted'] - vote_status_count['new_wallet_Unvoted']

# cumulative sum function
def cum_sum(df, inVar, outVar, group_by):
    df[outVar] = df.groupby([group_by])[inVar].cumsum()

# over lists
inVar_lst = ['Voted', 'Unvoted', 'Voter_diff',
             # 'U_Voted', 'U_Unvoted', 'U_Voter_diff',
             'new_wallet_Voted', 'new_wallet_Unvoted', 'new_Voter_diff']
outVar_lst = ['cum_Voted', 'cum_Unvoted', 'cum_n_Voter',
              # 'cum_U_Voted', 'cum_U_Unvoted','cum_n_U_Voter',
              'cum_new_wallet_Voted', 'cum_new_wallet_Unvoted', 'cum_n_new_Voter']

for x, y in zip(inVar_lst, outVar_lst):
    cum_sum(vote_status_count, x, y, 'validator_name')

vote_status_count['pct_change_Voter'] = vote_status_count['Voter_diff'] / (vote_status_count.groupby('validator_name')['cum_n_Voter'].shift(1))
# vote_status_count['pct_change_U_Voter'] = vote_status_count['U_Voter_diff'] / (vote_status_count.groupby('validator_name')['cum_n_U_Voter'].shift(1))
vote_status_count['pct_change_new_Voter'] = vote_status_count['new_Voter_diff'] / (vote_status_count.groupby('validator_name')['cum_n_new_Voter'].shift(1))

# vote_status_count = vote_status_count.replace(np.inf, np.nan)

# Votes table ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
votes_sum = df_longer.groupby(['validator_name', measuring_interval]).agg('sum').reset_index()
votes_sum['pct_change_votes'] = votes_sum['votes'] / (votes_sum.groupby('validator_name')['cum_votes'].shift(1))
# votes_sum = votes_sum.replace(np.inf, np.nan)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


# Get ranks, Top 10, Top 1
def add_ranking(df, what_rank, sortings, meas,
                descendings, Top_10, Consecutive_Top_10, Longest_Streak_Top_10,
                ascendings, Place, Consecutive_Place, Longest_Streak_Place):

    df[what_rank] = df.sort_values(sortings, ascending=descendings) \
        .groupby(measuring_interval)[meas].rank(method='first', ascending=descendings).astype(int)

    # top 10 ranking for consecutive
    df.loc[df[what_rank] <= 10, Top_10] = '1'
    s = df.groupby('validator_name')[Top_10].apply(lambda x:(x!=x.shift()).cumsum())  # counter with condition
    df[Consecutive_Top_10] = df.sort_values(['validator_name', measuring_interval], ascending=ascendings).\
        groupby(['validator_name', s]).cumcount().add(1)  # if not consecutive (NaN), resets
    df.loc[df[Top_10].isnull(), Consecutive_Top_10] = df[Top_10] # NaN if NaN
    df[Longest_Streak_Top_10] = df.groupby(['validator_name'])[Consecutive_Top_10].transform('max')
    df[Longest_Streak_Top_10] = df[Longest_Streak_Top_10].fillna(0).astype(int).astype(object).where(df[Longest_Streak_Top_10].notnull())

    # place for consecutive
    df.loc[df[what_rank] == 1, Place] = '1'
    s = df.groupby('validator_name')[Place].apply(lambda x: (x != x.shift()).cumsum())  # counter with condition
    df[Consecutive_Place] = df.sort_values(['validator_name', measuring_interval], ascending=ascendings). \
        groupby(['validator_name', s]).cumcount().add(1)  # if not consecutive (NaN), resets
    df.loc[df[Place].isnull(), Consecutive_Place] = df[Place]  # NaN if NaN
    df[Longest_Streak_Place] = df.groupby(['validator_name'])[Consecutive_Place].transform('max')
    df[Longest_Streak_Place] = df[Longest_Streak_Place].fillna(0).astype(int).astype(object).where(df[Longest_Streak_Place].notnull())

    df.drop(columns=[Top_10, Place], inplace=True)


# Voters -- vote status count
add_ranking(df=vote_status_count,
            what_rank='win_rank_Voter',
            meas='Voter_diff',
            sortings=[measuring_interval, 'Voter_diff', 'Voted', 'pct_change_Voter', 'cum_n_Voter', 'cum_Voted'],
            descendings=False,
            Top_10='Top_10_win_Voter',
            Consecutive_Top_10='Consecutive_Top_10_win_Voter',
            Longest_Streak_Top_10='Longest_Top_10_win_Voter',
            ascendings=True,
            Place='First_Place_Voter',
            Consecutive_Place='Consecutive_First_Place_Voter',
            Longest_Streak_Place = 'Longest_First_Place_Voter')

add_ranking(df=vote_status_count,
            what_rank='loss_rank_Voter',
            meas='Voter_diff',
            sortings=[measuring_interval, 'Voter_diff', 'Voted', 'pct_change_Voter', 'cum_n_Voter', 'cum_Voted'],
            descendings=True,
            Top_10='Top_10_loss_Voter',
            Consecutive_Top_10='Consecutive_Top_10_loss_Voter',
            Longest_Streak_Top_10='Longest_Top_10_loss_Voter',
            ascendings=False,
            Place='Last_Place_Voter',
            Consecutive_Place='Consecutive_Last_Place_Voter',
            Longest_Streak_Place='Longest_Last_Place_Voter')

# Votes -- amount of votes
add_ranking(df=votes_sum,
            what_rank='win_rank_votes',
            meas='votes',
            sortings=[measuring_interval, 'votes', 'pct_change_votes', 'cum_votes'],
            descendings=False,
            Top_10='Top_10_win_votes',
            Consecutive_Top_10='Consecutive_Top_10_win_votes',
            Longest_Streak_Top_10='Longest_Top_10_win_votes',
            ascendings=True,
            Place='First_Place_votes',
            Consecutive_Place='Consecutive_First_Place_votes',
            Longest_Streak_Place='Longest_First_Place_votes')

add_ranking(df=votes_sum,
            what_rank='loss_rank_votes',
            meas='votes',
            sortings=[measuring_interval, 'votes', 'pct_change_votes', 'cum_votes'],
            descendings=True,
            Top_10='Top_10_loss_votes',
            Consecutive_Top_10='Consecutive_Top_10_loss_votes',
            Longest_Streak_Top_10='Longest_Top_10_loss_votes',
            ascendings=False,
            Place='Last_Place_votes',
            Consecutive_Place='Consecutive_Last_Place_votes',
            Longest_Streak_Place='Longest_Last_Place_votes')

combined_df = pd.merge(vote_status_count, votes_sum, how = 'outer', on = ['validator_name', measuring_interval])


# for combined
term_change_comb = combined_df[combined_df[measuring_interval].isin(terms)]
this_term_change_comb = combined_df[combined_df[measuring_interval].isin([this_term])]



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## votes change this week

# overall vote change (turning them into texts)
total_term_change = term_change_comb.groupby([measuring_interval])[['votes','cum_votes']].agg('sum').reset_index()
total_term_change['pct_change_votes'] = total_term_change['votes'] / (total_term_change['cum_votes'].shift(1))
total_term_change = total_term_change[total_term_change[measuring_interval].isin([this_term])].drop(columns=measuring_interval)
change_symbol = total_term_change['votes'].apply(lambda x: "+" if x>0 else '').values[0] # for voter count
face_color = total_term_change['votes'].apply(lambda x: "green" if x>0 else 'firebrick').values[0] # this is for box color

total_cum_text = "Total votes: " + round(total_term_change['cum_votes']).apply('{:,}'.format).values[0].split('.', 1)[0] + " ICX"
total_text = "Weekly change: " + change_symbol + round(total_term_change['votes']).apply('{:,}'.format).values[0].split('.', 1)[0] + " ICX"
total_pct_change_text = change_symbol + "{:.2%}".format(total_term_change['pct_change_votes'].values[0])

# total_text = total_cum_vote_text + '\n' + total_vote_text + ' (' + total_pct_change_text + ')'
total_change = total_text + ' (' + total_pct_change_text + ')'


# vote change by p-reps
this_term_change = this_term_change_comb.sort_values(by=['win_rank_votes'], ascending=True)
temp_this_term_change = this_term_change[this_term_change['win_rank_votes'].between(1,10) \
    | this_term_change['loss_rank_votes'].between(1,10)]

# temporary
# temp_this_term_change = temp_this_term_change[temp_this_term_change['validator_name'] != 'NEOPLY']

def insert_week(string, index):
    return string[:index] + ' week' + string[index:]

# plotting
def plot_vote_chage(ymin_mult=1.0, ymax_mult=1.4,
                    ymin_val=-800000, ymax_val=700000, ytick_scale=200000,
                    voter_mult=0.9, voter_diff_mult=1.01,
                    top10_1_mult=0.9, top10_2_mult=0.8,
                    topF_1_mult=0.48, topF_2_mult=0.38):

    # plotting
    sns.set(style="ticks")
    plt.style.use(['dark_background'])
    f, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=temp_this_term_change['validator_name'],
                y=temp_this_term_change['votes'], palette="RdYlGn_r", ax=ax,
                      edgecolor="grey")
    ax.axhline(0, color="w", clip_on=False)
    ax.set_xlabel('P-Reps', fontsize=14, weight='bold', labelpad= 10)
    ax.set_ylabel('Δ votes', fontsize=14, weight='bold', labelpad= 10)
    ax.set_title('Weekly Vote Change - Top 10 gained / lost \n ('+ insert_week(this_term, 4) +')', fontsize=14, weight='bold')
    # plt.yscale('symlog')

    # manual fix for graphs here
    ###############################################################
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    ymin_set = ymin*ymin_mult
    ymax_set = ymax*ymax_mult
    ax.set_ylim([ymin_set, ymax_set])
    ax.yaxis.set_ticks(np.arange(ymin_val, ymax_val, ytick_scale))
    ################################################################

    sns.despine(offset=10, trim=True)
    plt.tight_layout()
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
    ax.grid(False)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # adding voter count (plus minus)
    voter_diff = temp_this_term_change['Voter_diff']
    voter_diff_text = temp_this_term_change['Voter_diff'].apply(lambda x: "+" + str(x) if x>0 else x)
    # voted_count = '+ ' + temp_this_term_change_votes['Voted'].astype(str)
    # unvoted_count = '- ' + temp_this_term_change_votes['Unvoted'].astype(str)
    # voter_count = voted_count.str.cat(unvoted_count, join='left', sep='\n')

    # adjust color based on total change (green: positive, red: negative)
    temp_df = pd.DataFrame(voter_diff)
    temp_df['color'] = np.where(temp_df['Voter_diff'] < 0, 'red', 'green')
    temp_df['color'] = np.where(temp_df['Voter_diff'] == 0, 'white', temp_df['color'])
    font_col = temp_df.iloc[:,1]

    # change ymin*xx here
    for (p,t,c) in zip(ax.patches,voter_diff_text,font_col):
        # height = p.get_height()
        height = ymin*voter_diff_mult
        ax.text(p.get_x() + p.get_width() / 2.,
                height,
                t,
                color=c,
                fontsize=12,
                weight='bold',
                ha="center")

    ax.text(-0.2, ymin*voter_mult, '( Δ voters )',
            color='white', fontsize=10)

    props = dict(boxstyle='round', facecolor=face_color, alpha=1)
    ax.text(xmax, ymax_set*0.9, total_cum_text + '\n' + total_change,
            linespacing = 1.5,
            horizontalalignment='right',
            verticalalignment='top', bbox=props,
            color='white', fontsize=12)

    # longest streak (top 3)
    header_top_10_steak = 'Winning Streak (weeks)'
    top_10_streak = this_term_change_comb.\
        sort_values(by=['Longest_Top_10_win_votes','cum_votes'], ascending=False)[['validator_name', 'Longest_Top_10_win_votes']].\
        reset_index(drop=True).\
        head(3).to_string(index=False, header=False)

    ax.text(xmax, ymax*top10_1_mult, header_top_10_steak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='green', fontsize=10, weight='bold')

    # change ymax*xx here
    ax.text(xmax, ymax*top10_2_mult, top_10_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='white', fontsize=10)

    # 1st place streak
    header_first_streak = '1st Place Winning Streak (weeks)'
    first_streak = this_term_change_comb.\
        sort_values(by=['Longest_First_Place_votes','cum_votes'], ascending=False)[['validator_name', 'Longest_First_Place_votes']].\
        reset_index(drop=True).\
        head(3).to_string(index=False, header=False)

    # change ymax*xx here
    ax.text(xmax, ymax*topF_1_mult, header_first_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='green', fontsize=10, weight='bold')

    ax.text(xmax, ymax*topF_2_mult, first_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='white', fontsize=10)

    plt.tight_layout()

# adjust these numbers to get proper plot
plot_vote_chage(ymin_mult=1.0, ymax_mult=1.4, # these multiplier to change ylims
                ymin_val=-600000, ymax_val=1800000, ytick_scale=200000, # these are actual ylims & tick interval20
                voter_mult=0.85, voter_diff_mult=1.05, # voter change multiplier
                top10_1_mult=0.92, top10_2_mult=0.85, # where top 10 streak locates
                topF_1_mult=0.55, topF_2_mult=0.47) # where top first locates

# saving
plt.savefig(os.path.join(resultsPath_interval, '01_' + measuring_interval + "_vote_change.png"))
# plt.savefig(os.path.join(resultsPath_interval, '01_' + measuring_interval + "_vote_change_neoply.png"))


# adding top 10 ranking - voter
# this_term_change = this_term_change_comb.sort_values(by=['win_rank_Voter'], ascending=True)
# this_term_change = this_term_change[this_term_change['win_rank_Voter'].between(1,10) \
#     | this_term_change['loss_rank_Voter'].between(1,10)]



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## voters change this week

# overall voter change (turning them into texts)

# getting voted, unvoted and difference count and cumulative count -- by measuring interval
def wallet_count(df, A, B, meas):
    voted_A = df[df[A] == 'voted']
    voted_A = voted_A.drop_duplicates(['delegator', meas, A])[['delegator', meas, A]].\
        dropna().drop(columns='delegator')
    voted_A = voted_A.groupby([meas]).agg('count').reset_index()

    unvoted_B = df[df[B] == 'unvoted']
    unvoted_B = unvoted_B.drop_duplicates(['delegator', meas, B])[['delegator', meas, B]].\
        dropna().drop(columns='delegator')
    unvoted_B = unvoted_B.groupby([meas]).agg('count').reset_index()

    all_voted = voted_A.merge(unvoted_B,on=[meas])

    all_voted['diff_AB'] = all_voted[A] - all_voted[B]
    all_voted['cum_A'] = all_voted[A].cumsum()
    all_voted['cum_B'] = all_voted[B].cumsum()
    all_voted['diff_cum_AB'] = all_voted['cum_A'] - all_voted['cum_B']

    return(all_voted)

voting_unique_inc_return = wallet_count(df_longer, 'returned_voting', 'stopped_voting', measuring_interval)
voting_unique_first_last = wallet_count(df_longer, 'new_wallet_A', 'new_wallet_B', measuring_interval)

voting_unique_inc_return['pct_change_voters'] = voting_unique_inc_return['diff_AB'] / (voting_unique_inc_return['diff_cum_AB'].shift(1))
total_term_change = voting_unique_inc_return[voting_unique_inc_return[measuring_interval].isin([this_term])].drop(columns=measuring_interval)
change_symbol = total_term_change['diff_AB'].apply(lambda x: "+" if x>0 else '').values[0] # for voter count
face_color = total_term_change['diff_AB'].apply(lambda x: "green" if x>0 else 'red').values[0] # this is for box color

total_cum_text = "Total voters: " + round(total_term_change['diff_cum_AB']).apply('{:,}'.format).values[0].split('.', 1)[0]
total_text = "Weekly change: " + change_symbol + round(total_term_change['diff_AB']).apply('{:,}'.format).values[0].split('.', 1)[0]
total_pct_change_text = change_symbol + "{:.2%}".format(total_term_change['pct_change_voters'].values[0])
total_change = total_text + ' (' + total_pct_change_text + ')'


# voter change by p-reps
this_term_change = this_term_change_comb.sort_values(by=['win_rank_Voter'], ascending=True)
temp_this_term_change = this_term_change[this_term_change['win_rank_Voter'].between(1,10) \
    | this_term_change['loss_rank_Voter'].between(1,10)]


def insert_week(string, index):
    return string[:index] + ' week' + string[index:]


# plotting
def plot_voter_chage(ymin_mult=1.1, ymax_mult=1.3,
                    ymin_val=-20, ymax_val=35, ytick_scale=5,
                    first_time_voter_mult=0.97, new_voter_mult=1.1,
                    top10_1_mult=0.94, top10_2_mult=0.86,
                    topF_1_mult=0.65, topF_2_mult=0.57):

    # plotting
    sns.set(style="ticks")
    plt.style.use(['dark_background'])
    f, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(x=temp_this_term_change['validator_name'],
                y=temp_this_term_change['Voter_diff'], palette="RdYlGn_r", ax=ax,
                      edgecolor="grey")
    ax.axhline(0, color="w", clip_on=False)
    ax.set_xlabel('P-Reps', fontsize=14, weight='bold', labelpad= 10)
    ax.set_ylabel('Δ voters', fontsize=14, weight='bold', labelpad= 10)
    ax.set_title('Weekly Voter Change - Top 10 gained / lost \n ('+ insert_week(this_term, 4) +')', fontsize=14, weight='bold')
    # plt.yscale('symlog')

    # manual fix for graphs here
    ###############################################################
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    ymin_set = ymin * ymin_mult
    ymax_set = ymax * ymax_mult
    ax.set_ylim([ymin_set, ymax_set])
    ax.yaxis.set_ticks(np.arange(ymin_val, ymax_val, ytick_scale))
    ################################################################

    sns.despine(offset=10, trim=True)
    plt.tight_layout()
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
    ax.grid(False)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # adding voter count (plus minus)
    new_voter = temp_this_term_change['new_wallet_Voted']
    new_voter_text = temp_this_term_change['new_wallet_Voted'].apply(lambda x: "+" + str(x) if x>0 else x)
    # voted_count = '+ ' + temp_this_term_change_votes['Voted'].astype(str)
    # unvoted_count = '- ' + temp_this_term_change_votes['Unvoted'].astype(str)
    # voter_count = voted_count.str.cat(unvoted_count, join='left', sep='\n')

    # adjust color based on total change (green: positive, red: negative)
    temp_df = pd.DataFrame(new_voter)
    temp_df['color'] = np.where(temp_df['new_wallet_Voted'] < 0, 'red', 'green')
    temp_df['color'] = np.where(temp_df['new_wallet_Voted'] == 0, 'white', temp_df['color'])
    font_col = temp_df.iloc[:,1]

    for (p,t,c) in zip(ax.patches,new_voter_text,font_col):
        # height = p.get_height()
        height = ymin*new_voter_mult
        ax.text(p.get_x() + p.get_width() / 2.,
                height,
                t,
                color=c,
                fontsize=12,
                weight='bold',
                ha="center")

    ax.text(-0.2, ymin*first_time_voter_mult, '( First-time Voters )',
            color='white', fontsize=10)

    props = dict(boxstyle='round', facecolor=face_color, alpha=1)
    ax.text(xmax, ymax_set*0.9, total_cum_text + '\n' + total_change,
            linespacing = 1.5,
            horizontalalignment='right',
            verticalalignment='top', bbox=props,
            color='white', fontsize=12)

    # longest streak (top 3)
    header_top_10_steak = 'Winning Streak (weeks)'
    top_10_streak = this_term_change_comb.\
        sort_values(by=['Longest_Top_10_win_Voter','cum_n_Voter','cum_Voted'], ascending=False)[['validator_name', 'Longest_Top_10_win_Voter']].\
        reset_index(drop=True).\
        head(3).to_string(index=False, header=False)

    ax.text(xmax, ymax*top10_1_mult, header_top_10_steak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='green', fontsize=10, weight='bold')

    ax.text(xmax, ymax*top10_2_mult, top_10_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='white', fontsize=10)

    # 1st place streak
    header_first_streak = '1st Place Winning Streak (weeks)'
    first_streak = this_term_change_comb.\
        sort_values(by=['Longest_First_Place_Voter','cum_n_Voter','cum_Voted'], ascending=False)[['validator_name', 'Longest_First_Place_Voter']].\
        reset_index(drop=True).\
        head(3).to_string(index=False, header=False)

    ax.text(xmax, ymax*topF_1_mult, header_first_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='green', fontsize=10, weight='bold')

    ax.text(xmax, ymax*topF_2_mult, first_streak,
            linespacing = 1.4,
            horizontalalignment='right',
            verticalalignment='top',
            color='white', fontsize=10)

    plt.tight_layout()



plot_voter_chage(ymin_mult=1.1, ymax_mult=1.3,
                    ymin_val=-15, ymax_val=60, ytick_scale=5,
                    first_time_voter_mult=0.95, new_voter_mult=1.15, ## change these
                    top10_1_mult=0.95, top10_2_mult=0.87,
                    topF_1_mult=0.65, topF_2_mult=0.57)
# saving
plt.savefig(os.path.join(resultsPath_interval, '02_' + measuring_interval + "_voter_change.png"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# total votes per voter by measuring_interval
total_term_change = df_longer.groupby(['delegator', measuring_interval])[['votes','cum_votes']].agg('sum').reset_index()

# making bins to extract group
def bin_votes(row):
    if row['cum_votes'] < 1:
        val = '< 1'
    elif 1 <= row['cum_votes'] <= 1000:
        val = '1-1000'
    elif 1000 < row['cum_votes'] <= 10000:
        val = '1000-10K'
    elif 10000 < row['cum_votes'] <= 100000:
        val = '10K-100K'
    elif 100000 < row['cum_votes'] <= 1000000:
        val = '100K-1M'
    elif 1000000 < row['cum_votes']:
        val = '1M +'
    else:
        pass
        # val = -1
    return val


total_term_change['cum_votes_bin'] = total_term_change.apply(bin_votes, axis=1)

# binning data into categories
count_vote_bin = total_term_change[total_term_change['cum_votes'] != 0] # remove 0 balance here

vote_pct = count_vote_bin.groupby(['cum_votes_bin', measuring_interval])['cum_votes'].agg('sum').reset_index()
vote_pct = vote_pct[vote_pct[measuring_interval].isin([this_term])].\
    drop(columns=[measuring_interval]).reset_index(drop=True)
vote_pct['sum_votes'] = vote_pct['cum_votes'].sum()
vote_pct['pct_votes'] = (vote_pct['cum_votes'] / vote_pct['sum_votes']).map("{:.2%}".format)
vote_pct = vote_pct.set_index(list(vote_pct)[0])
vote_pct = vote_pct.reindex(['< 1', '1-1000', '1000-10K', '10K-100K', '100K-1M', '1M +']).reset_index()
# print(vote_pct)

grand_total_votes_text = 'Total Votes: ' + '{:,}'.format(vote_pct['sum_votes'][0].astype(int)) + ' ICX'

count_vote_bin = count_vote_bin.drop(columns=(['votes', 'cum_votes']))
count_vote_bin = count_vote_bin.groupby(['cum_votes_bin', measuring_interval]).agg('count').reset_index()
count_vote_bin = count_vote_bin[count_vote_bin[measuring_interval].isin([this_term])].\
    drop(columns=[measuring_interval]).reset_index(drop=True)
count_vote_bin = count_vote_bin.set_index(list(count_vote_bin)[0])
count_vote_bin = count_vote_bin.reindex(['< 1', '1-1000', '1000-10K', '10K-100K', '100K-1M', '1M +']).reset_index()

porcent = 100.*count_vote_bin['delegator']/count_vote_bin['delegator'].sum() # for plotting (legend)
porcet_vote = vote_pct['pct_votes'].to_list()

# donut chart.... oh no..
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(9, 6), subplot_kw=dict(aspect="equal"))
fig.patch.set_facecolor('black')

cmap = plt.get_cmap("Set3")
inner_colors = cmap(np.array(range(len(count_vote_bin['delegator']))))

my_circle=plt.Circle((0,0), 0.7, color='black')
wedges, texts = plt.pie(count_vote_bin['delegator'],
                                   labels=count_vote_bin['delegator'],
                                   counterclock=False,
                                   startangle=90,
                                   colors=inner_colors,
                                   textprops={'fontsize': 14, 'weight': 'bold'})
                                   #textprops={'color': "y"})

for text, color in zip(texts, inner_colors):
    text.set_color(color)

labels = ['{0} ({1:1.2f} % || {2})'.format(i,j,k) for i,j,k in zip(count_vote_bin['cum_votes_bin'], porcent, porcet_vote)]

plt.legend(wedges, labels,
          title="Vote range in ICX (% voters || % votes)",
          loc="center left",
          bbox_to_anchor=(1, 0, 0.5, 1),
          fontsize=10)

ax.text(0., 0., grand_total_votes_text,
        horizontalalignment='center',
        verticalalignment='center',
        linespacing = 2,
        fontsize=12,
        weight='bold')

ax.set_title('Number of Wallets by Vote Size ('+ insert_week(this_term, 4) +')', fontsize=14, weight='bold', y=1.08)

p=plt.gcf()
p.gca().add_artist(my_circle)
plt.axis('equal')
plt.show()
plt.tight_layout()



# saving
plt.savefig(os.path.join(resultsPath_interval, '03_' + measuring_interval + "_count_wallet_by_vote_size.png"))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## number of P-Reps voted

# making bins to extract group
def bin_NumPReps(df):
    if df['how_many_prep_voted'] <= 10:
        val = df['how_many_prep_voted']
    elif 11 <= df['how_many_prep_voted'] <= 19:
        val = '11-19'
    elif 20 <= df['how_many_prep_voted'] <= 29:
        val = '20-29'
    elif 30 <= df['how_many_prep_voted'] <= 39:
        val = '30-39'
    elif 40 <= df['how_many_prep_voted'] <= 49:
        val = '40-49'
    elif 50 <= df['how_many_prep_voted'] <= 59:
        val = '50-59'
    elif 60 <= df['how_many_prep_voted'] <= 69:
        val = '60-69'
    elif 70 <= df['how_many_prep_voted'] <= 79:
        val = '70-79'
    elif 80 <= df['how_many_prep_voted'] <= 89:
        val = '80-89'
    elif 90 <= df['how_many_prep_voted'] <= 99:
        val = '90-99'
    elif df['how_many_prep_voted'] >= 100:
        val = '100'
    else:
        pass
        val = -1
    return val

prep_voted_count = df_longer.groupby(['delegator', measuring_interval, 'how_many_prep_voted']).agg('sum').reset_index()

# Votes and Voters
votes_and_prep_count = prep_voted_count.groupby([measuring_interval, 'how_many_prep_voted'])['cum_votes'].agg(['sum','count']).reset_index()

# binning number of P-Reps voted ###
votes_and_prep_count_bin = votes_and_prep_count.copy()
votes_and_prep_count_bin['NumPReps_bin'] = votes_and_prep_count_bin.apply(bin_NumPReps, axis=1)
no_of_prep_voted_binned = votes_and_prep_count_bin.groupby([measuring_interval,'NumPReps_bin']).agg({'sum':'sum', 'count': 'sum'}).reset_index()

# getting the maximum number of prep voted given terms
# most_no_of_prep_voted = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin(terms)].groupby([measuring_interval]).tail(1)
most_no_of_prep_voted = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin([this_term])]['how_many_prep_voted'].tail(1)


# adding "11" as anything more than 10 for now
votes_and_prep_count['how_many_prep_voted'] = np.where(votes_and_prep_count['how_many_prep_voted']>10, 11, votes_and_prep_count['how_many_prep_voted'])

# summing '11'
votes_and_prep_count = votes_and_prep_count.groupby([measuring_interval, 'how_many_prep_voted']).\
    agg(['sum']).\
    reset_index().\
    droplevel(1, axis=1)

votes_and_prep_count = votes_and_prep_count.rename(columns={'sum': 'votes', 'count': 'voters'})

# Percentages across Week (Votes)
votes_and_prep_count['total_votes'] = votes_and_prep_count.groupby([measuring_interval])['votes'].transform('sum')
votes_and_prep_count['pct_votes'] = votes_and_prep_count['votes'] / votes_and_prep_count['total_votes']

# Percentages across Week (Voters)
votes_and_prep_count['total_voters'] = votes_and_prep_count.groupby([measuring_interval])['voters'].transform('sum')
votes_and_prep_count['pct_voters'] = votes_and_prep_count['voters'] / votes_and_prep_count['total_voters']

# votes_and_prep_count = votes_and_prep_count.drop(columns=['total_votes','total_voters'])

# Interested term and get difference between previous term & this term
votes_and_prep_count_term = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin(terms)].reset_index(drop=True)
votes_and_prep_count_term['how_many_prep_voted'] = np.where(votes_and_prep_count_term['how_many_prep_voted']>10, '11+', votes_and_prep_count_term['how_many_prep_voted'])

votes_and_prep_count_this_term = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin([this_term])].reset_index(drop=True)
votes_and_prep_count_this_term['how_many_prep_voted'] = np.where(votes_and_prep_count_this_term['how_many_prep_voted']>10, '11+', votes_and_prep_count_this_term['how_many_prep_voted'])

# getting last term data to get length and remove later
votes_and_prep_count_last_term = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin([last_term])].reset_index(drop=True)
last_term_length = len(votes_and_prep_count_last_term)

# votes_and_prep_count_term_diff = votes_and_prep_count_term.drop(columns=[measuring_interval]).groupby('how_many_prep_voted').diff().dropna().reset_index(drop=True)
votes_and_prep_count_term_diff = votes_and_prep_count_term.drop(columns=[measuring_interval]).groupby('how_many_prep_voted').diff().reset_index(drop=True)
votes_and_prep_count_term_diff = votes_and_prep_count_term_diff[last_term_length:].reset_index(drop=True) #remove last term here

# adding change symbols here for graph
change_symbol_votes = votes_and_prep_count_term_diff['pct_votes'].apply(lambda x: "+" if x>0 else '') # for voter count
change_symbol_voters = votes_and_prep_count_term_diff['pct_voters'].apply(lambda x: "+" if x>0 else '') # for voter count

# adding % symbol
votes_and_prep_count_term['pct_votes'] = votes_and_prep_count_term['pct_votes'].astype(float).map("{:.2%}".format)
votes_and_prep_count_term['pct_voters'] = votes_and_prep_count_term['pct_voters'].astype(float).map("{:.2%}".format)
votes_and_prep_count_this_term = votes_and_prep_count_term[votes_and_prep_count_term[measuring_interval].isin([this_term])].reset_index(drop=True)

# adding % symbol for diff
votes_and_prep_count_term_diff['pct_votes'] = votes_and_prep_count_term_diff['pct_votes'].astype(float).map("{:.2%}".format)
votes_and_prep_count_term_diff['pct_voters'] = votes_and_prep_count_term_diff['pct_voters'].astype(float).map("{:.2%}".format)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## Votes
grand_total_votes_text = 'Total Votes: ' + '{:,}'.format(votes_and_prep_count_this_term['total_votes'][0].astype(int)) + ' ICX' \
                         + '\n' + 'Total Wallets: '+ '{:,}'.format(votes_and_prep_count_this_term['total_voters'][0])

total_votes_text = '(' + votes_and_prep_count_this_term['how_many_prep_voted'].astype(str) + ') '  \
                   + round(votes_and_prep_count_this_term['votes']).astype(int).apply('{:,}'.format)

# for plotting (legend)
label_text =  votes_and_prep_count_this_term['how_many_prep_voted'].astype(str) \
              + ' P-Rep(s) - ' \
              + votes_and_prep_count_this_term['pct_votes'] \
              + ' (' + change_symbol_votes + votes_and_prep_count_term_diff['pct_votes']  + ') '

# donut chart.... oh no..
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(10, 6), subplot_kw=dict(aspect="equal"))
fig.patch.set_facecolor('black')

cmap = plt.get_cmap("Set3")
these_colors = cmap(np.array(range(len(votes_and_prep_count_this_term['votes']))))

my_circle=plt.Circle((0,0), 0.7, color='black')

wedges, texts = plt.pie(votes_and_prep_count_this_term['votes'],
                                   labels=total_votes_text,
                                   counterclock=False,
                                   startangle=90,
                                   colors=these_colors,
                                   textprops={'fontsize': 12, 'weight': 'bold'})
                                   #textprops={'color': "y"})

for text, color in zip(texts, these_colors):
    text.set_color(color)

ax.text(0., 0., grand_total_votes_text,
        horizontalalignment='center',
        verticalalignment='center',
        linespacing = 2,
        fontsize=12,
        weight='bold')

xmin, xmax = ax.get_xlim()
ymin, ymax = ax.get_ylim()

ax.text(xmax*2.5, ymin,
        'Highest Number of P-Reps Voted: ' + most_no_of_prep_voted.astype(str).item(),
        linespacing=1.4,
        horizontalalignment='right',
        verticalalignment='top',
        color='yellow', fontsize=11, weight='bold')

plt.legend(wedges, label_text,
          title="Number of P-Reps Voted (ICX)",
          loc="lower left",
          bbox_to_anchor=(1, 0, 0.5, 1),
           fontsize=10)

ax.set_title('Current Vote Spreading '+ insert_week(this_term, 4) + '\n (# of P-Reps Voted per Wallet)', fontsize=14, weight='bold', y=1.08)

p=plt.gcf()
p.gca().add_artist(my_circle)
plt.axis('equal')
plt.show()
plt.tight_layout()

# saving
plt.savefig(os.path.join(resultsPath_interval, '04_' + measuring_interval + "_number_of_preps_voted_per_wallet.png"))



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Vote Stagnancy -- just by having voted or not for that week

## logic to get if previous interval data is same (0) or different (1)
diff_logic = df_longer.copy()
diff_logic = diff_logic[['delegator', measuring_interval, 'validator_name', 'cum_votes', 'how_many_prep_voted']]
diff_logic = diff_logic.sort_values(by=['delegator', 'validator_name', measuring_interval])

## concatenating prep/votes/how many prep voted and comparing between time interval
diff_logic['how_many_prep_voted'] = diff_logic['how_many_prep_voted'].astype(str)
diff_logic['cum_votes'] = diff_logic['cum_votes'].astype(str)
diff_logic['period'] = diff_logic[['validator_name', 'cum_votes', 'how_many_prep_voted']].apply('/'.join, axis=1)
diff_logic = diff_logic[['delegator', measuring_interval, 'validator_name',  'how_many_prep_voted', 'period']]
diff_logic['lag_period'] = diff_logic['period'].shift()
diff_logic['same_as_lag'] = np.where(diff_logic['lag_period'] == diff_logic['period'], 1, 0)
diff_logic = diff_logic[['delegator', 'validator_name', measuring_interval,  'how_many_prep_voted', 'same_as_lag']]
diff_logic = diff_logic.groupby(['delegator', measuring_interval, 'how_many_prep_voted']).agg('sum').reset_index()

## adding first week logic (as there is no previous data to compare) -- making it different since it's that they voted
first_logic= diff_logic.groupby(['delegator'])[measuring_interval].first().reset_index()
first_logic['first'] = '1'

## then comparing that to how many prep, if same then they are same (0), if not, they are different (1)
diff_logic = pd.merge(diff_logic, first_logic, on=['delegator', measuring_interval], how='left')
diff_logic['diff_flag'] = np.where(diff_logic['how_many_prep_voted'] == diff_logic['same_as_lag'].astype(str), 0, 1)
diff_logic['diff_flag'] = np.where(diff_logic['first'] == '0', 0, diff_logic['diff_flag'])
diff_logic = diff_logic[['delegator', measuring_interval, 'diff_flag']]


## vote stagnancy from here ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

vote_stagnancy_count = df_longer.groupby(['delegator', measuring_interval]).agg('sum').reset_index()
## merging with diff_logic
vote_stagnancy_count = pd.merge(vote_stagnancy_count, diff_logic, on=['delegator', measuring_interval], how='left')

# applying two logics -- votes must be 0 (meaning they did not change votes) and also 'diff_flag' which also includes
# those that may not have up/down votes but same votes among different p-reps
vote_stagnancy_count['Stagnant'] = np.where((vote_stagnancy_count['votes'] == 0) &
                                            (vote_stagnancy_count['diff_flag'] == 0), 'No Vote Change', 'Vote Change (Up/Down)')

vote_stagnancy = vote_stagnancy_count.groupby([measuring_interval, 'Stagnant'])['cum_votes'].agg(['sum', 'count']).reset_index()
vote_stagnancy = vote_stagnancy.rename(columns = {'sum': 'votes', 'count': 'voters'})

# vote stagnancy % by votes
vote_stagnancy['total_votes'] = vote_stagnancy.groupby(measuring_interval)['votes'].transform('sum')
vote_stagnancy['pct_votes'] = vote_stagnancy['votes'] / vote_stagnancy['total_votes']

# vote stagnancy % by voters
vote_stagnancy['total_voters'] = vote_stagnancy.groupby(measuring_interval)['voters'].transform('sum')
vote_stagnancy['pct_voters'] = vote_stagnancy['voters'] / vote_stagnancy['total_voters']

first_appearing_interval = unique_interval[measuring_interval][0]
# vote_stagnancy = vote_stagnancy[vote_stagnancy[measuring_interval] != '2019-34']  # for order of the plot
vote_stagnancy = vote_stagnancy[vote_stagnancy[measuring_interval] != first_appearing_interval]  # for order of the plot
vote_stagnancy = vote_stagnancy.sort_values(by=['Stagnant', measuring_interval])

# currPath = os.getcwd()
# vote_stagnancy.to_csv(os.path.join(currPath, 'test.csv'), index=False)

sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x=measuring_interval, y='votes', hue="Stagnant", data=vote_stagnancy, palette=sns.color_palette('husl', n_colors=2))
h,l = ax.get_legend_handles_labels()

ax.set_xlabel('Week', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Votes (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Vote Stagnancy \n (based on active voting wallets per week)', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/10e6) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
sns.despine(offset=10, trim=True)

xs = vote_stagnancy[measuring_interval]
ys = vote_stagnancy['votes']

porcent = vote_stagnancy.copy()
porcent['pct_votes'] = porcent['pct_votes'].apply('{:.0%}'.format)
porcent.loc[porcent['Stagnant'] == 'Vote Change (Up/Down)', 'pct_votes'] = ''
porcent = porcent['pct_votes']

# zip joins x and y coordinates in pairs
for x,y,z in zip(xs,ys,porcent):

    plt.annotate(z, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,10), # distance from text to points (x,y)
                 ha='center', # horizontal alignment can be left, right or center
                 fontsize=8,
                 color='w',
                 weight='bold')

plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.legend(h[1:],l[1:],ncol=1,
          # title="Voting Activity Stagnation",
          fontsize=10,
          loc='upper left')
n = 2  # Keeps every n-th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()


# saving
plt.savefig(os.path.join(resultsPath_interval, '06_' + measuring_interval + "_vote_stagnancy_by_activity_of_wallet.png"))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
### 'Spread Your Votes!' Participants ###

def raffle_tickets(df):
    if df.loc['how_many_prep_voted'] <= 10:
        val = 0
    elif 11 <= df['how_many_prep_voted'] <= 19:
        val = 1
    elif 20 <= df['how_many_prep_voted'] <= 29:
        val = 2
    elif 30 <= df['how_many_prep_voted'] <= 39:
        val = 3
    elif 40 <= df['how_many_prep_voted'] <= 49:
        val = 4
    elif 50 <= df['how_many_prep_voted'] <= 59:
        val = 5
    elif 60 <= df['how_many_prep_voted'] <= 69:
        val = 6
    elif 70 <= df['how_many_prep_voted'] <= 79:
        val = 7
    elif 80 <= df['how_many_prep_voted'] <= 89:
        val = 8
    elif 90 <= df['how_many_prep_voted'] <= 99:
        val = 9
    elif df['how_many_prep_voted'] >= 100:
        val = 10
    else:
        pass
        val = -1
    return val

# For transparency, Spread Your Votes data
# df_longer_this_term = df_longer[df_longer[measuring_interval].isin([this_term])]

# SYV_participants = df_longer_this_term[df_longer_this_term['how_many_prep_voted'] > 10]
SYV_participants = df_longer[df_longer['how_many_prep_voted'] > 10]

SYV_participants = SYV_participants[['validator_name', 'delegator', measuring_interval, 'votes', 'cum_votes', 'how_many_prep_voted']]
SYV_participants['NumPReps_bin'] = SYV_participants.apply(bin_NumPReps, axis=1)
SYV_participants['raffle_tickets'] = SYV_participants.apply(raffle_tickets, axis=1)

# getting the % of votes per P-Rep
SYV_participants['sum_votes'] = SYV_participants.groupby([measuring_interval, 'delegator'])['cum_votes'].transform('sum')
SYV_participants = SYV_participants.rename(columns={'cum_votes' : 'total_votes'})
SYV_participants['vote_percentages_per_prep'] = SYV_participants['total_votes']/SYV_participants['sum_votes']

# for google sheets (participant information)
SYV_participants_percentages = SYV_participants.drop(columns=['NumPReps_bin','votes']).\
    groupby([measuring_interval, 'delegator','sum_votes','how_many_prep_voted','raffle_tickets']).\
    agg(['min', 'max', 'median', 'mean']).sort_values(by='how_many_prep_voted', ascending=False).reset_index()

SYV_participants_percentages[('vote_percentages_per_prep', 'min')] = SYV_participants_percentages[('vote_percentages_per_prep', 'min')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'max')] = SYV_participants_percentages[('vote_percentages_per_prep', 'max')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'median')] = SYV_participants_percentages[('vote_percentages_per_prep', 'median')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'mean')] = SYV_participants_percentages[('vote_percentages_per_prep', 'mean')].astype(float).map("{:.5%}".format)

# this term
SYV_participants_percentages_this_term = SYV_participants_percentages[SYV_participants_percentages[measuring_interval].isin([this_term])]
SYV_participants_percentages_this_term.to_csv(os.path.join(resultsPath_interval, 'IIN_SpreadYourVotes_RaffleTickets_' + this_term + '.csv'), index=False)

# for IIN
SYV_participants_summary = SYV_participants.drop_duplicates(['delegator',measuring_interval])\
    [['delegator', measuring_interval, 'how_many_prep_voted', 'NumPReps_bin', 'raffle_tickets','sum_votes']].\
    sort_values(by='how_many_prep_voted', ascending=False)

SYV_participants_summary_agg = SYV_participants_summary.drop(columns='how_many_prep_voted')
SYV_participants_summary_agg['sum_raffle_tickets'] = SYV_participants_summary_agg['raffle_tickets']

SYV_participants_summary_agg = SYV_participants_summary_agg.groupby([measuring_interval, 'NumPReps_bin', 'raffle_tickets']).\
    agg({'delegator':'count','sum_raffle_tickets':'sum','sum_votes':'sum'}).reset_index()

# this term
SYV_participants_summary_agg_this_term = SYV_participants_summary_agg[SYV_participants_summary_agg[measuring_interval].isin([this_term])]

SYV_participants_summary_agg_this_term = SYV_participants_summary_agg_this_term.\
    append(SYV_participants_summary_agg_this_term.sum(numeric_only=True).rename('Total')).\
    assign(Total=lambda x: x.sum(1))

SYV_participants_summary_agg_this_term['NumPReps_bin'] = np.where(SYV_participants_summary_agg_this_term['NumPReps_bin'].isna(), 'Total',
                                                                  SYV_participants_summary_agg_this_term['NumPReps_bin'])

SYV_participants_summary_agg_this_term['raffle_tickets'] = SYV_participants_summary_agg_this_term['raffle_tickets'].astype(int).apply('{:,}'.format)
SYV_participants_summary_agg_this_term['raffle_tickets'] = np.where(SYV_participants_summary_agg_this_term['NumPReps_bin'] == 'Total', '-', SYV_participants_summary_agg_this_term['raffle_tickets'])

# last term
SYV_participants_summary_agg_last_term = SYV_participants_summary_agg[SYV_participants_summary_agg[measuring_interval].isin([last_term])]

SYV_participants_summary_agg_last_term = SYV_participants_summary_agg_last_term.\
    append(SYV_participants_summary_agg_last_term.sum(numeric_only=True).rename('Total')).\
    assign(Total=lambda x: x.sum(1))

SYV_participants_summary_agg_last_term['NumPReps_bin'] = np.where(SYV_participants_summary_agg_last_term['NumPReps_bin'].isna(), 'Total',
                                                                  SYV_participants_summary_agg_last_term['NumPReps_bin'])

SYV_participants_summary_agg_last_term_delegator = SYV_participants_summary_agg_last_term[['NumPReps_bin', 'delegator', 'sum_votes']].\
    rename(columns={'delegator':'delegator_prev', 'sum_votes' : 'sum_votes_prev'})

# merge
SYV_participants_summary_agg_merged = pd.merge(SYV_participants_summary_agg_this_term,
                                               SYV_participants_summary_agg_last_term_delegator,
                                               on='NumPReps_bin',
                                               how='left').fillna(0)
SYV_participants_summary_agg_merged['delegator_diff'] = SYV_participants_summary_agg_merged['delegator'] - SYV_participants_summary_agg_merged['delegator_prev']
SYV_participants_summary_agg_merged['vote_diff'] = SYV_participants_summary_agg_merged['sum_votes'] - SYV_participants_summary_agg_merged['sum_votes_prev']

SYV_participants_summary_agg_merged['del_symbol'] = np.where(SYV_participants_summary_agg_merged['delegator_diff'] > 0, '+', '') # delegation difference
SYV_participants_summary_agg_merged['vote_symbol'] = np.where(SYV_participants_summary_agg_merged['vote_diff'] > 0, '+', '') # vote difference


# get rid of decimals
m = (SYV_participants_summary_agg_merged.dtypes=='float')
SYV_participants_summary_agg_merged.loc[:,m] = SYV_participants_summary_agg_merged.loc[:,m].astype(int).applymap('{:,}'.format)

# adding the delegation differences
this_order = ["11-19", "20-29","30-39","40-49","50-59","60-69","70-79","80-89","90-99","100","Total"]
SYV_participants_summary_agg_merged['NumPReps_bin'] = pd.Categorical(SYV_participants_summary_agg_merged['NumPReps_bin'], this_order)
SYV_participants_summary_agg_merged['delegator_diff'] = SYV_participants_summary_agg_merged['del_symbol'] + SYV_participants_summary_agg_merged['delegator_diff']
SYV_participants_summary_agg_merged['delegator_diff'] = np.where(SYV_participants_summary_agg_merged['delegator_diff'] == '0', '-', SYV_participants_summary_agg_merged['delegator_diff'])
SYV_participants_summary_agg_merged['delegator_diff'] = ' (' + SYV_participants_summary_agg_merged['delegator_diff'] + ')'
SYV_participants_summary_agg_merged['delegator'] = SYV_participants_summary_agg_merged['delegator'] + SYV_participants_summary_agg_merged['delegator_diff']

SYV_participants_summary_agg_merged['vote_diff'] = SYV_participants_summary_agg_merged['vote_symbol'] + SYV_participants_summary_agg_merged['vote_diff']
SYV_participants_summary_agg_merged['vote_diff'] = np.where(SYV_participants_summary_agg_merged['vote_diff'] == '0', '-', SYV_participants_summary_agg_merged['vote_diff'])
SYV_participants_summary_agg_merged['vote_diff'] = ' (' + SYV_participants_summary_agg_merged['vote_diff'] + ')'
SYV_participants_summary_agg_merged['sum_votes'] = SYV_participants_summary_agg_merged['sum_votes'] + SYV_participants_summary_agg_merged['vote_diff']

SYV_participants_summary_agg_merged = SYV_participants_summary_agg_merged.sort_values(by='NumPReps_bin')\
    [['NumPReps_bin','raffle_tickets', 'delegator', 'sum_raffle_tickets', 'sum_votes']]


SYV_participants_summary_agg_merged = SYV_participants_summary_agg_merged.\
    rename(columns={'NumPReps_bin': 'No. of P-Reps Voted', 'raffle_tickets': 'No. of Raffle Tickets \n Per Wallet',
                    'delegator': 'No. of Wallets',  'sum_raffle_tickets': "Total No. of \n Raffle Tickets",'sum_votes':'Total Votes (ICX)'})

import six

def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=12,
                     header_color='#40466e', row_colors=['black', 'black'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
        ax.set_title("'Spread Your Votes!'" + ' Participants (' + insert_week(this_term, 4) + ')', fontsize=15,
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

render_mpl_table(SYV_participants_summary_agg_merged, header_columns=0, col_width=2.4)

# saving
plt.savefig(os.path.join(resultsPath_interval, '05_' + measuring_interval + "_spread_your_votes_participants.png"))

# for lucky draw
SYV_participants_luckydraw_this_term = SYV_participants[SYV_participants[measuring_interval].isin([this_term])]
SYV_participants_this_term = SYV_participants[SYV_participants[measuring_interval].isin([this_term])]
SYV_participants_luckydraw_this_term = SYV_participants_luckydraw_this_term.groupby(['delegator',measuring_interval]).\
    head(SYV_participants_this_term['raffle_tickets'])[['delegator', measuring_interval, 'how_many_prep_voted', 'NumPReps_bin', 'raffle_tickets']]
SYV_participants_luckydraw_this_term = SYV_participants_luckydraw_this_term.sort_values(by='how_many_prep_voted', ascending=False)
SYV_participants_luckydraw_this_term.drop(columns='NumPReps_bin').to_csv(os.path.join(resultsPath_interval, 'IIN_SpreadYourVotes_LuckyDraw_' + this_term + '.csv'), index=False)


# import random
# random_number = random.randint(1, 10000)
# print(random_number)
# SYV_participants_luckydraw.sample(frac=1, random_state=random_number).head(1)


# checking if the range for the data is complete (just by looking at the dates)
print(unique_date[-1:])



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
voter_rank = combined_df.copy()
voter_rank['prep_ranking'] = voter_rank.sort_values([measuring_interval, 'cum_votes'], ascending=False) \
    .groupby(measuring_interval)['cum_votes'].rank(method='first', ascending=False).astype(int)

first_time_voter_history = voter_rank.sort_values([measuring_interval, 'new_wallet_Voted'], ascending=False).\
groupby([measuring_interval]).first().reset_index()

# just to see the behaviour of first-time voters
# first_time_voter_history = voter_rank.sort_values([measuring_interval, 'new_wallet_Voted'], ascending=False).\
# groupby([measuring_interval]).head(10).reset_index()
# median_rank = first_time_voter_history.groupby(["validator_name"])['prep_ranking'].agg('median').astype(int)
# top10_new_voter = first_time_voter_history.groupby(["validator_name"])[measuring_interval].\
#     agg('count').sort_values(ascending=False).reset_index()
# top10_new_voter.merge(median_rank, on='validator_name')


sns.set(style="ticks", rc={"lines.linewidth": 3})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.barplot(x=measuring_interval, y='new_wallet_Voted', hue='validator_name', data=first_time_voter_history,
            palette=sns.color_palette('husl', n_colors=3))
h,l = ax.get_legend_handles_labels()

ax.set_xlabel('Weeks', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Number of First-Time Voters', fontsize=14, weight='bold', labelpad=10)
ax.set_title('P-Reps with the most First-Time Voters since August 2019', fontsize=14,
             weight='bold')

sns.despine(offset=5, trim=True)
plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.grid(False)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
n = 2  # Keeps every n-th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()

###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~###
# voter ranking vs prep ranking
# voter_rank = combined_df.copy()
# voter_rank['prep_ranking'] = voter_rank.sort_values([measuring_interval, 'cum_votes'], ascending=False) \
#     .groupby(measuring_interval)['cum_votes'].rank(method='first', ascending=False).astype(int)
#
# voter_rank['first_time_voter_ranking'] = voter_rank.sort_values([measuring_interval, 'new_wallet_Voted'], ascending=False) \
#     .groupby(measuring_interval)['new_wallet_Voted'].rank(method='first', ascending=False).astype(int)
#
# voter_rank = voter_rank[['validator_name','cum_votes',measuring_interval,'prep_ranking', 'win_rank_Voter','first_time_voter_ranking']]
# # voter_rank = combined_df.groupby(['validator_name'])['win_rank_Voter'].agg(['mean','median'])
#
# import scipy.stats as sp
#
# def corr_plot(df, inVar1, inVar2, this_xlabel, this_ylabel, this_title):
#
#     m_var1 = df.groupby('validator_name')[[inVar1]].apply(np.median).reset_index(name='var1')
#     m_var2 = df.groupby('validator_name')[[inVar2]].apply(np.median).reset_index(name='var2')
#     m_days_ranking = pd.merge(m_var1, m_var2, on='validator_name', how='left')
#
#     # plot
#     sns.set(style="ticks")
#     plt.style.use("dark_background")
#     g = sns.jointplot("var1", "var2", data=m_days_ranking,
#                       kind="reg", height=6, truncate=False, color='m',
#                       joint_kws={'scatter_kws': dict(alpha=0.5)}
#                       ).annotate(sp.pearsonr)
#     g.set_axis_labels(this_xlabel, this_ylabel, fontsize=14, weight='bold', labelpad=10)
#     g = g.ax_marg_x
#     g.set_title(this_title, fontsize=12, weight='bold')
#     plt.tight_layout()
#
# corr_plot(voter_rank,
#           'win_rank_Voter',
#           'prep_ranking',
#           'Voter Ranking (median)',
#           'P-Rep Ranking (median)',
#           'Voter Ranking vs P-Rep Ranking (Weekly)')
#
# corr_plot(voter_rank,
#           'first_time_voter_ranking',
#           'prep_ranking',
#           'First Time Voter Ranking (median)',
#           'P-Rep Ranking (median)',
#           'First Time Voter Ranking vs P-Rep Ranking (Weekly)')


## plotting for the progress of vote spreading
Prep_11_plus = SYV_participants_summary.groupby([measuring_interval])['sum_votes'].agg(['count', 'sum']).reset_index()
total = "n=" + Prep_11_plus['count'].apply('{:,}'.format).unique()


sns.set(style="ticks", rc={"lines.linewidth": 3})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(8, 6))
sns.barplot(x=measuring_interval, y='sum', data=Prep_11_plus,
            palette=sns.cubehelix_palette(len(Prep_11_plus), start=.5, rot=-.75))
h,l = ax.get_legend_handles_labels()

ax.set_xlabel('Weeks', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Votes (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title("Votes in '11+ P-Rep' Voted Category", fontsize=14,
             weight='bold')
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000000) + ' M'))


sns.despine(offset=5, trim=True)
plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.grid(False)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

for i in range(len(total)):
    p = ax.patches[i]
    height = p.get_height()
    ax.text(p.get_x() + p.get_width() / 2., height + height * 0.02,
                total[i],
                ha="center",
            fontsize=10)


plt.tight_layout()



