
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


# Data formatting & save for visualisation

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
rewardsPath = os.path.join(inPath, "rewards")
votesPath = os.path.join(inPath, "votes")

# If have logo_file separately, then use:
have_logo_file = 1

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Loading data and Formatting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# read prep details
prep_df = pd.read_csv(os.path.join(inPath, 'icon_prep_details.csv'))

# renaming country column
prep_df = prep_df.rename(columns={'country': 'alpha-3'})

# loading country information
country_code_url = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'
cc_df = pd.read_csv(country_code_url, index_col=0)
cc_df.index.name = 'country'
cc_df.reset_index(inplace=True)
cc_df = cc_df[['alpha-3', 'country', 'sub-region']]

# add country and city information (p-rep name for rewards)
prep_df = pd.merge(prep_df, cc_df, how='left', on='alpha-3')


# if you have your own logo website -- get this from our github repository
if have_logo_file == 1:

    # loading my logo file -- manually found some logos that were missing
    logos = pd.read_csv(os.path.join(inPath, 'logo_find_github_fixed.csv')).drop(columns=['city', 'country', 'address'])

    # dropping logo file since I have another logo file
    prep_df = prep_df.drop(['logo'], axis=1)
    prep_df = pd.merge(prep_df, logos, how='left', on='name')

    # changing logo column location
    cols = list(prep_df)
    cols.insert(4, cols.pop(cols.index('logo')))
    prep_df = prep_df.loc[:, cols]

else:
    pass


# checking and standardising city names
check = prep_df.groupby("city")["city"].count()

prep_df["city"] = prep_df["city"].str.strip()
prep_df["city"] = prep_df["city"].str.upper()
prep_df["city"] = prep_df["city"].replace("SGP", "SINGAPORE")
prep_df["city"] = prep_df["city"].replace("SHANG HAI", "SHANGHAI")
prep_df["city"] = prep_df["city"].replace("BEI JING", "BEIJING")
prep_df["city"] = prep_df["city"].replace("NEW YORK CITY", "NEW YORK")
prep_df["city"] = prep_df["city"].replace("MELBOURNE AND ADELAIDE", "ADELAIDE")
prep_df["city"] = prep_df["city"].replace("ZURICH,WARSAW", "ZURICH")
prep_df["city"] = prep_df["city"].str.title()

check = prep_df.sort_values(by=['city'])

# output final prep info
prep_df.to_csv(os.path.join(inPath, 'final_prep_details.csv'), index=False)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Append data and summarise ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
appended_rewards = []
appended_votes = []

# loop through prep addresses and row-bind summarised (sum by date) data
for i in range(len(prep_df.address)):
    rewards_df = pd.read_csv(os.path.join(rewardsPath, prep_df.address[i] + '_prep_rewards.csv'))
    votes_df = pd.read_csv(os.path.join(votesPath, prep_df.address[i] + '.csv'))

    # changing dates
    rewards_df['date'] = pd.to_datetime(votes_df['datetime']).dt.strftime("%Y-%m-%d")
    votes_df['date'] = pd.to_datetime(votes_df['datetime']).dt.strftime("%Y-%m-%d")

    # appending
    appended_rewards.append(rewards_df)
    appended_votes.append(votes_df)

# concatenate all files together
rewards_df_concat = pd.concat(appended_rewards)
votes_df_concat = pd.concat(appended_votes)

# add country and city information (p-rep name for rewards)
rewards_df_concat = pd.merge(rewards_df_concat, prep_df, how='left', on='address')

# save votes and rewards concatenated files
votes_df_concat.to_csv(os.path.join(inPath, 'votes_all.csv'), index=False)
rewards_df_concat.to_csv(os.path.join(inPath, 'rewards_all.csv'), index=False)


# cumulative sum for votes
votes_cumsum = votes_df_concat.groupby(['validator', 'date'])['votes'].sum().groupby('validator').cumsum().reset_index()

# pivot wider (format for visualisation)
votes_wider = votes_cumsum.pivot(index='validator',
                      columns='date',
                      values='votes').reset_index()

temp_prep_df = prep_df.rename(columns={"address": "validator"})

# merge votes with country information
votes_df_final = pd.merge(temp_prep_df[['validator', 'name', 'country', 'sub-region', 'city', 'logo']], votes_wider, how='left', on='validator')

# remove validator address
votes_df_final = votes_df_final.drop(['validator'], axis=1)

# output vote data -- for running bar chart
votes_df_final.to_csv(os.path.join(inPath, 'votes_per_day_wide.csv'), index=False)

