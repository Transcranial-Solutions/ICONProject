
#########################################################################
## Project: ICON Vote Data Visualisation                               ##
## Date: May 2020                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Vote status -- when voted, stayed voted, vote increased/decreased, unvoted

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading/saving
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
rewardsPath = os.path.join(inPath, "rewards")
votesPath = os.path.join(inPath, "votes")
resultsPath = os.path.join(inPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)

# read data
prep_df = pd.read_csv(os.path.join(inPath, 'final_prep_details.csv'))
votes_all = pd.read_csv(os.path.join(inPath, 'votes_all.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Get cumulative votes by group ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
votes_all = votes_all.sort_values(['delegator', 'validator_name', 'date'])

votes_cumsum = votes_all.groupby(['delegator', 'validator_name', 'date'])['votes'].sum().groupby(['delegator', 'validator_name']).cumsum().reset_index()
votes_cumsum = votes_cumsum.rename(columns={'votes': 'cum_votes'})
votes_cumsum = votes_cumsum.sort_values(['validator_name', 'date'])

# pivot wider
votes_cumsum_wider = votes_cumsum.pivot_table(index=['delegator', 'validator_name'],
                      columns='date',
                      values='cum_votes').reset_index()


# make data longer to have all the dates same
votes_cumsum_longer = votes_cumsum_wider.melt(id_vars=['delegator', 'validator_name'], var_name='date', value_name='cum_votes')
votes_cumsum_longer = votes_cumsum_longer.sort_values(['delegator', 'validator_name', 'date', 'cum_votes'])

# fill down by group
votes_cumsum_longer['cum_votes'] = votes_cumsum_longer.groupby(['delegator', 'validator_name'])['cum_votes'].ffill()

# add cum_votes shifted (to previous) column - by group
votes_cumsum_longer['prev_cum_votes'] = votes_cumsum_longer.groupby(['delegator', 'validator_name'])['cum_votes'].shift()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Get Vote status ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting rid of non-needed rows (rows before first vote & after unvote)
keep_these = (votes_cumsum_longer.cum_votes >= 1e-6) | (votes_cumsum_longer.prev_cum_votes >= 1e-6)
votes_cumsum_longer = votes_cumsum_longer[keep_these]


# add unvoted / voted indicator
votes_cumsum_longer = votes_cumsum_longer.assign(vote_status=['unvoted' if cum_votes <= 1e-6 else 'voted' for cum_votes in votes_cumsum_longer['cum_votes']])

# more logic for vote status -- vote stayed vs vote changed (up down)
votes_unchanged = votes_cumsum_longer['cum_votes'] == votes_cumsum_longer['prev_cum_votes']
votes_up = (votes_cumsum_longer['cum_votes'] > votes_cumsum_longer['prev_cum_votes']) & \
           (votes_cumsum_longer['cum_votes'] >= 1e-6)
votes_down = (votes_cumsum_longer['cum_votes'] < votes_cumsum_longer['prev_cum_votes']) & (votes_cumsum_longer['cum_votes'] >= 1e-6)
voted = (votes_cumsum_longer['vote_status'].shift() == 'unvoted') & (votes_cumsum_longer['prev_cum_votes'] <= 1e-6)

votes_cumsum_longer.loc[votes_unchanged, 'vote_status'] = 'unchanged'
votes_cumsum_longer.loc[votes_up, 'vote_status'] = 'up'
votes_cumsum_longer.loc[votes_down, 'vote_status'] = 'down'
votes_cumsum_longer.loc[voted, 'vote_status'] = 'voted'  # this was added for voted and unvoted if same p-rep over time

# dropping previous cumulative votes
votes_cumsum_longer = votes_cumsum_longer.drop(['prev_cum_votes'], axis=1)

# saving vote status file -- very large
votes_cumsum_longer.to_csv(os.path.join(inPath, 'vote_status_per_day.csv'), index=False)



