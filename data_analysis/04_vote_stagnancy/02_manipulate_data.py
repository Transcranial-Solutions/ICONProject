
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


# Data formatting & save

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
votesPath = os.path.join(inPath, "votes")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Loading data and Formatting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# read prep details
prep_df = pd.read_csv(os.path.join(inPath, 'icon_prep_details.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Append data and summarise ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
appended_votes = []

# loop through prep addresses and row-bind summarised (sum by date) data
for i in range(len(prep_df.address)):
    votes_df = pd.read_csv(os.path.join(votesPath, prep_df.address[i] + '.csv'))

    # changing dates
    votes_df['date'] = pd.to_datetime(votes_df['datetime']).dt.strftime("%Y-%m-%d")

    # appending
    appended_votes.append(votes_df)

# concatenate all files together
votes_df_concat = pd.concat(appended_votes)

# add country and city information (p-rep name for rewards)

# save votes and rewards concatenated files
votes_df_concat.to_csv(os.path.join(inPath, 'votes_all.csv'), index=False)
