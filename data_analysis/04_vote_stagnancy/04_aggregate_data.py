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

# Data Preparation -- aggregating data

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import scipy.stats as sp
import seaborn as sns
import os


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading/saving
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outPath = os.path.join(currPath, "04_vote_stagnancy")
resultsPath = os.path.join(outPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)

# read data
count_cum_status = pd.read_csv(os.path.join(inPath, 'vote_status_per_day.csv'))

# count for main p-reps (as this may change in the future)
main_prep_count = 22

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data Preparation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# sort to count cumulatively
count_cum_status = count_cum_status.sort_values(by=['delegator', 'validator_name', 'date', 'vote_status'])

# changing date (str) to date format and adding week number
count_cum_status['week'] = pd.to_datetime(count_cum_status['date'], errors='coerce').dt.strftime('%Y-%U')


# adding total voting days per voter
total_voting_days_df = count_cum_status.drop_duplicates(['delegator', 'date'])[['delegator', 'date']]
total_voting_days_df['total_voting_days'] = total_voting_days_df.groupby(['delegator'])['date'].transform('count')
total_voting_days_df['total_voting_months'] = np.ceil(total_voting_days_df['total_voting_days'] / 30).astype(int)

count_cum_status = pd.merge(count_cum_status, total_voting_days_df, on=['delegator', 'date'], how='left')

# total votes per day per P-Rep to get ranking each day and how many days prep has been around
prep_ranking_per_day = count_cum_status.groupby(['validator_name', 'date'])['cum_votes'].agg('sum').reset_index()
prep_ranking_per_day['prep_ranking'] = prep_ranking_per_day.sort_values(by=['date', 'cum_votes'], ascending=False).\
    groupby('date').cumcount().add(1)
prep_ranking_per_day['prep_days_around'] = prep_ranking_per_day.groupby(['validator_name'])['date'].cumcount().add(1)
prep_ranking_per_day['prep_days_around_total'] = prep_ranking_per_day.groupby(['validator_name'])['date'].transform('count')
prep_ranking_per_day = prep_ranking_per_day.rename(columns={'cum_votes': 'prep_total_votes'})
# adding main/sub prep status and also main name + sub as one
prep_ranking_per_day['main_sub_prep'] = \
    np.where(prep_ranking_per_day['prep_ranking'] <= main_prep_count, 'Main', 'Sub')
prep_ranking_per_day['validator_sub'] = \
    np.where(prep_ranking_per_day['prep_ranking'] <= main_prep_count,
             prep_ranking_per_day['validator_name'],
             'Sub P-Reps')

count_cum_status = pd.merge(count_cum_status, prep_ranking_per_day, on=['validator_name', 'date'], how='left')

# getting the latest ranking
prep_ranking_last = prep_ranking_per_day.sort_values(by = 'date').groupby(['validator_name']).last().reset_index()[['validator_name','prep_ranking']]


# pure stagnancy (no vote change) -- resets if unvoted and voted again as we have the grouping 'vote_status_raw' included
s = count_cum_status.groupby('delegator').vote_status.apply(lambda x:((x!='unchanged')|(x!=x.shift())).cumsum())  # counter with condition
count_cum_status['cum_days_pure'] = count_cum_status.groupby(['delegator', s, 'vote_status_raw']).cumcount().add(1)  # if different vote_status
count_cum_status['lead_days_pure'] = count_cum_status['cum_days_pure'].shift(-1).fillna(1).astype(int)  # shifting column
count_cum_status['days_pure'] = count_cum_status['cum_days_pure'].\
    mask(count_cum_status['lead_days_pure'] != 1).bfill().astype(int)
# count_cum_status = count_cum_status.drop(columns=['lead_days_pure'])  # leaving this for subsetting later

# P-Rep-based stangnancy (vote up/down/unchanged but same P-Rep) -- resets if unvoted and voted again.
t = count_cum_status.groupby(['delegator']).vote_status_raw.apply(lambda x:(x!=x.shift()).cumsum())  # counter with condition
count_cum_status['cum_days_same_prep'] = count_cum_status.groupby(['delegator', 'validator_name', 'vote_status_raw', t]).cumcount().add(1)  # if different vote_status
count_cum_status['lead_days_same_prep'] = count_cum_status['cum_days_same_prep'].shift(-1).fillna(1).astype(int)
count_cum_status['days_same_prep'] = count_cum_status['cum_days_same_prep'].\
    mask(count_cum_status['lead_days_same_prep'] != 1).bfill().astype(int)
count_cum_status = count_cum_status.drop(columns=['lead_days_same_prep'])


## removing 'unvoted' for counting number of p-reps each voter voted per day (and get mean per voter)
a = count_cum_status['vote_status'] != 'unvoted'
count_voted_prep_per_day = count_cum_status.copy()
count_voted_prep_per_day = count_voted_prep_per_day[a].groupby(['delegator', 'date']).count()['vote_status'].reset_index().\
    rename(columns={'vote_status': 'how_many_prep_voted'})
count_voted_prep_per_day['how_many_prep_voted_mean'] = count_voted_prep_per_day.groupby('delegator')['how_many_prep_voted'].transform('mean').round().astype(int)

# merged with n_voted category
count_cum_status = pd.merge(count_cum_status, count_voted_prep_per_day, on=['delegator', 'date'], how='left')
count_cum_status['how_many_prep_voted'] = count_cum_status['how_many_prep_voted'].fillna(0).astype(int)
count_cum_status['how_many_prep_voted_mean'] = count_cum_status['how_many_prep_voted_mean'].fillna(0).astype(int)
count_cum_status['month'] = pd.to_datetime(count_cum_status['date']).dt.strftime("%Y-%m")

# aggregated & summarised data to be used
agg_df = count_cum_status[count_cum_status['lead_days_pure'] == 1][['delegator', 'validator_name', 'cum_votes',
                     'vote_status', 'total_voting_days', 'total_voting_months',
                     'prep_days_around_total', 'days_pure', 'days_same_prep',
                     'how_many_prep_voted_mean','prep_ranking']]

# getting max of the cumulative votes to derive % of the votes that were stagnated out of total
# max_votes = agg_df[['delegator', 'validator_name', 'cum_votes']]
# max_votes = max_votes.groupby(['delegator', 'validator_name'])['cum_votes'].agg(max).reset_index().rename(columns={'cum_votes': 'max_cum_votes'})

# getting most recent total votes for each p-rep
current_prep_votes = count_cum_status[count_cum_status['date'].max() == count_cum_status['date']]
current_prep_votes = current_prep_votes.\
    drop_duplicates(['validator_name','prep_total_votes', 'prep_ranking'])[['validator_name','prep_total_votes', 'prep_ranking']].\
    reset_index(drop=True)


# shortening your names, hope you don't mind!
def shorten_prep_name(inData):
    df = inData.copy()
    df.loc[df['validator_name'] == 'ICONIST VOTE WISELY - twitter.com/info_prep', 'validator_name'] = 'ICONIST VOTE WISELY'
    df.loc[df['validator_name'] == 'Piconbello { You Pick We Build }', 'validator_name'] = 'Piconbello'
    df.loc[df['validator_name'] == 'UNBLOCK {ICX GROWTH INCUBATOR}', 'validator_name'] = 'UNBLOCK'
    df.loc[df['validator_name'] == 'Gilga Capital (NEW - LETS GROW ICON)', 'validator_name'] = 'Gilga Capital'
    return(df)

agg_df = shorten_prep_name(agg_df)
current_prep_votes = shorten_prep_name(current_prep_votes)
