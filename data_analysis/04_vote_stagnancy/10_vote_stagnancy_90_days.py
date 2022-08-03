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

# Need to run it in sequence as it may need data from previous scripts (04 - 09)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import scipy.stats as sp
import seaborn as sns
import os


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#### plotting

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ days and votes stagnancy ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# data for today to be used for current votes
today_df = count_cum_status[count_cum_status['date'].max() == count_cum_status['date']]
today_df = shorten_prep_name(today_df)

entire_vote_df = today_df.copy()
entire_vote_df = entire_vote_df.drop_duplicates(['validator_sub', 'prep_total_votes', 'date'])[['validator_sub', 'prep_total_votes', 'date']]
entire_vote_df['entire_votes'] = entire_vote_df.groupby('date')['prep_total_votes'].transform('sum')
entire_vote_df = entire_vote_df.drop_duplicates(['validator_sub', 'entire_votes'])[['validator_sub', 'entire_votes']]


# stagnant votes with given days
def stag_votes(df, stag_days, inVar, stag_type):
    df['Stagnancy'] = np.where(df[inVar] > stag_days, 'More than ' + str(stag_days) + ' days', str(stag_days) + ' days or less')
    df = df.groupby(['validator_name','validator_sub','Stagnancy'])['cum_votes'].agg(['count', 'sum']).reset_index()
    df = pd.merge(df, current_prep_votes, on='validator_name', how='left')
    df = df.groupby(['validator_sub','Stagnancy']).agg('sum').reset_index()
    df['pct_stagnant'] = (df['sum'] / df['prep_total_votes']) * 100
    df = pd.merge(df, entire_vote_df, on='validator_sub', how='left')
    df['pct_entire_votes'] = (df['sum'] / df['entire_votes'])
    df['pct_prep_votes'] = (df['prep_total_votes'] / df['entire_votes'])
    df = df.sort_values(by=['Stagnancy', 'sum'], ascending=False)
    df['pct_label'] = df['pct_prep_votes'].map("{:.2%}".format) + ' / ' + df['pct_entire_votes'].map("{:.2%}".format)

    # just getting more than stag_days
    df = df[df['Stagnancy'] == 'More than ' + str(stag_days) + ' days']
    df['sort_by_this'] = np.where(df['validator_sub'] == 'Sub P-Reps', 1, 2)
    df = df.sort_values(by=['sort_by_this', 'sum'], ascending=False).reset_index(drop=True)

    # title label
    df['total_stag_votes'] = df.groupby('Stagnancy')['sum'].transform('sum')
    df['pct_total_stag_votes'] = df['total_stag_votes'] / df['entire_votes']

    # return(df)

    title_label = 'Total: {:,.0f}'.format(df['total_stag_votes'][0]) + ' ICX ({:.2%})'.format(df['pct_total_stag_votes'][0])

    # for labelling in the graph (stagnant amount)
    sum_label = df['sum'].map(lambda x: '{:,.1f}'. format(x/1000000) + 'M').to_list()
    pct_label = df['pct_label'].to_list()

    # scatterplot
    sns.set(style="ticks")
    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.scatterplot(x="validator_sub", y="pct_stagnant",
                         hue="sum", size="sum",
                         sizes=(10, 10000),
                         alpha=0.7, edgecolors="white", linewidth=1,
                         data=df)
    ax.set_xlabel('P-Reps (% voted / % stagnant of network votes)', fontsize=14, weight='bold', labelpad=10)
    ax.set_ylabel('Stagnant Votes (%)', fontsize=14, weight='bold', labelpad=10)
    plt.title('Current Vote Stagnancy (> ' + str(stag_days) + ' days) in each P-Rep: ' + stag_type + '\n' + title_label,
              fontsize=14,
              weight='bold',
              linespacing=2)
    ax.set_ylim([-10, 110])

    sns.despine(offset=10, trim=True)
    plt.tight_layout()
    ax.set_xticklabels(ax.get_xticklabels(), rotation=55, ha="right")
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    ax.get_legend().remove()

    ymin, ymax = ax.get_ylim()
    ymin_pos = np.empty(len(pct_label))
    ymin_pos.fill(ymin)
    max_y = df['pct_stagnant'].to_list()

    pos = range(len(pct_label))

    for tick, label in zip(pos, ax.get_xticklabels()):
        ax.text(pos[tick], ymin_pos[tick], pct_label[tick],
                horizontalalignment='left', size='small', weight='semibold', color='w', rotation=55)

    for tick, label in zip(pos, ax.get_xticklabels()):
        ax.text(pos[tick], max_y[tick] + max_y[tick]*0.1, sum_label[tick],
                horizontalalignment='center', size='small', weight='semibold', color='w', rotation=30)

    plt.tight_layout()

# days pure
stag_votes_df = stag_votes(df=today_df, stag_days=90, inVar='days_pure', stag_type='Untouched')

# saving
plt.savefig(os.path.join(resultsPath, '10_vote_stagnancy_untouched_90_days.png'))

# same P-Rep
stag_votes_df = stag_votes(df=today_df, stag_days=90, inVar='days_same_prep', stag_type='Unmoved')

# saving
plt.savefig(os.path.join(resultsPath, '11_vote_stagnancy_unmoved_90_days.png'))

