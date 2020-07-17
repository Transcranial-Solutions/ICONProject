#########################################################################
## Project: ICON in Numbers                                            ##
## Date: July 2020                                                     ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# Processing the data (recoding & manipulation) necessary for analysis such as
# voting status, first time voter, left temporarily, etc.

# This will require data from the previous script (2_data_extraction.py)
# and other variables -- mainly 'measuring_interval' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
# import os
# import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Data -- by validator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# concatenate them into a dataframe -- by validator_name
df = pd.concat(all_df)
unique_date = df.drop_duplicates(['year', 'month', 'week', 'date', 'day'])[['year', 'month', 'week', 'date', 'day']].sort_values('date')
df = df.groupby(['validator_name', 'delegator', measuring_interval]).agg('sum').reset_index()
df = df.sort_values(by=['validator_name', 'delegator', measuring_interval]).reset_index(drop=True)

# hope you don't mind, just shortening your names
df.loc[df['validator_name'] == 'ICONIST VOTE WISELY - twitter.com/info_prep', 'validator_name'] = 'ICONIST VOTE WISELY'
df.loc[df['validator_name'] == 'Piconbello { You Pick We Build }', 'validator_name'] = 'Piconbello'
df.loc[df['validator_name'] == 'UNBLOCK {ICX GROWTH INCUBATOR}', 'validator_name'] = 'UNBLOCK'

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
