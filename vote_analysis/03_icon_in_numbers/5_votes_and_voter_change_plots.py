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

# Plotting votes and voters change

# This will require data from the previous script (4_master_table.py)
# and other variables -- mainly 'measuring_interval', 'term' and 'this_term' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## votes change this week

# overall vote change (turning them into texts)
total_term_change = term_change_comb.groupby([measuring_interval])[['votes','cum_votes']].agg('sum').reset_index()
total_term_change['pct_change_votes'] = total_term_change['votes'] / (total_term_change['cum_votes'].shift(1))
total_term_change = total_term_change[total_term_change[measuring_interval].isin([this_term])].drop(columns=measuring_interval)
change_symbol = total_term_change['votes'].apply(lambda x: "+" if x>0 else '').values[0] # for voter count
face_color = total_term_change['votes'].apply(lambda x: "green" if x>0 else 'red').values[0] # this is for box color

total_cum_text = "Total votes: " + round(total_term_change['cum_votes']).apply('{:,}'.format).values[0].split('.', 1)[0] + " ICX"
total_text = "Weekly change: " + change_symbol + round(total_term_change['votes']).apply('{:,}'.format).values[0].split('.', 1)[0] + " ICX"
total_pct_change_text = change_symbol + "{:.2%}".format(total_term_change['pct_change_votes'].values[0])

# total_text = total_cum_vote_text + '\n' + total_vote_text + ' (' + total_pct_change_text + ')'
total_change = total_text + ' (' + total_pct_change_text + ')'


# vote change by p-reps
this_term_change = this_term_change_comb.sort_values(by=['win_rank_votes'], ascending=True)
temp_this_term_change = this_term_change[this_term_change['win_rank_votes'].between(1,10) \
    | this_term_change['loss_rank_votes'].between(1,10)]


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
    ax.set_ylim([ymin_set,ymax_set])
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

    props = dict(boxstyle='round', facecolor='green', alpha=1)
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
                ymin_val=-800000, ymax_val=700000, ytick_scale=200000, # these are actual ylims & tick interval
                voter_mult=0.9, voter_diff_mult=1.01, # voter change multiplier
                top10_1_mult=0.9, top10_2_mult=0.8, # where top 10 streak locates
                topF_1_mult=0.48, topF_2_mult=0.38) # where top first locates

# saving
plt.savefig(os.path.join(resultsPath, measuring_interval + "_vote_change.png"))


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

    props = dict(boxstyle='round', facecolor='green', alpha=1)
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
                    ymin_val=-20, ymax_val=35, ytick_scale=5,
                    first_time_voter_mult=0.97, new_voter_mult=1.1,
                    top10_1_mult=0.94, top10_2_mult=0.86,
                    topF_1_mult=0.65, topF_2_mult=0.57)
# saving
plt.savefig(os.path.join(resultsPath, measuring_interval + "_voter_change.png"))