
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

# Data formatting for visualisation

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading
currPath = os.getcwd()
inPath = currPath + '\\output\\'
rewardsPath = inPath + '\\rewards\\'
votesPath = inPath + '\\votes\\'

# If have logo_file separately, then use:
have_logo_file = 1

# read prep details
prep_df = pd.read_csv(inPath + 'icon_prep_details.csv')

appended_rewards = []
appended_votes = []

# loop through prep addresses and row-bind summarised (sum by date) data
for i in range(len(prep_df.address)):
    rewards_df = pd.read_csv(rewardsPath + prep_df.address[i] + '_prep_rewards.csv')
    votes_df = pd.read_csv(votesPath + prep_df.address[i] + '.csv')

    rewards_df['date'] = pd.to_datetime(votes_df['datetime']).dt.strftime("%Y-%m-%d")
    rewards_per_day = rewards_df.groupby(['address', 'date'])['rewards'].sum().reset_index()

    votes_df['date'] = pd.to_datetime(votes_df['datetime']).dt.strftime("%Y-%m-%d")
    votes_per_day = votes_df.groupby(['validator', 'validator_name', 'date'])['votes'].sum().reset_index()

    appended_rewards.append(rewards_per_day)
    appended_votes.append(votes_per_day)

# concatenate
rewards_df_concat = pd.concat(appended_rewards)
votes_df_concat = pd.concat(appended_votes)

# add country and city information (p-rep name for rewards)
rewards_df_concat = pd.merge(rewards_df_concat, prep_df, how='left', on='address')

# cumulative sum for votes
votes_cumsum = votes_df_concat.groupby(['validator', 'date'])['votes'].sum().groupby(level=0).cumsum().reset_index()

# pivot wider
votes_wider = votes_cumsum.pivot(index = 'validator',
                      columns = 'date',
                      values = 'votes').reset_index()

temp_prep_df = prep_df.rename(columns={"address": "validator"})

# merge
votes_df_concat = pd.merge(temp_prep_df[['validator', 'name', 'country', 'city', 'logo']], votes_wider, how='left', on='validator')

if have_logo_file == 1:

    # loading my logo file -- manually found some logos that were missing
    logos = pd.read_csv(inPath + 'logo_find.csv')

    # dropping logo file since I have another logo file
    votes_df_concat = votes_df_concat.drop(['logo'], axis=1)
    votes_df_concat = pd.merge(votes_df_concat, logos, how='left', on='name')

    # changing logo column location
    cols = list(votes_df_concat)
    cols.insert(4, cols.pop(cols.index('logo')))
    votes_df_concat = votes_df_concat.loc[:, cols]

else:
    pass

# remove validator address
votes_df_concat = votes_df_concat.drop(['validator'], axis=1)

# output
votes_df_concat.to_csv(inPath + 'votes_per_day.csv', index=False)

