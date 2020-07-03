#########################################################################
## Project: ICON Vote Data Visualisation                               ##
## Date: July 2020                                                     ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# Further aggregating and calculating data for plots

# This will require data from the previous script (3_data_processing.py)
# and other variables -- mainly 'measuring_interval', 'term' and 'this_term' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
# import numpy as np
# import os
# import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

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
