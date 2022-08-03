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

# Need to run it in sequence as it may need data from previous scripts (04 - 07)

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plots by P-Rep ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

def plot_box_by_prep(df, inVar, fig_title):

    m = df.groupby('validator_name')[[inVar]].apply(np.median)
    # print(m)
    m.name = 'median_days'
    df = df.join(m, on=['validator_name']).sort_values(
        by=['prep_months_around_total', 'median_days', inVar])

    # median ranking -- for plot
    df['this_ranking'] = df.groupby(['validator_name'])['prep_ranking'].transform('median').astype(int)
    this_ranking = df.drop_duplicates('validator_name').sort_values(
        by=['prep_months_around_total', 'median_days', inVar])['this_ranking'].to_list()

    # boxplot by p-rep ~~~~~~~~~~~~~~~~~#
    plt.rcParams["axes.labelsize"] = 14
    sns.set(style="ticks")
    plt.style.use(['dark_background'])
    fig, ax = plt.subplots(figsize=(18, 8))
    fig = sns.boxplot(x='validator_name', y=inVar, hue='prep_months_around_total', data=df,
                      linewidth=1.2, dodge=False)
    ax.set_xlabel('P-Reps', fontsize=14, weight='bold', labelpad=10)
    ax.set_ylabel('Days', fontsize=14, weight='bold', labelpad=10)
    plt.title(fig_title, fontsize=14, weight='bold')
    sns.despine(offset=10, trim=True)
    plt.tight_layout()

    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
    plt.xticks(fontsize=8)
    plt.yticks(fontsize=12)
    plt.setp(ax.artists, edgecolor='k')
    ax.legend().set_title('P-Rep Age \n (months)')
    ax.grid(False)

    ymin, ymax = ax.get_ylim()
    ymin_pos = np.empty(len(this_ranking))
    ymin_pos.fill(ymin)

    pos = range(len(this_ranking))
    for tick, label in zip(pos, ax.get_xticklabels()):
        ax.text(pos[tick], ymin_pos[tick] + - 2, this_ranking[tick],
                horizontalalignment='center', size='x-small', color='w', weight='semibold', rotation=90)

    plt.tight_layout()

# Pure (unchanged) by P-Rep
plot_box_by_prep(agg_df_max_pure, 'days_pure', 'Consecutive Days with Votes Untouched in each P-Rep Node')

# saving
plt.savefig(os.path.join(resultsPath, '06_days_votes_untouched_by_prep_age.png'))

# Same P-Rep by P-Rep
plot_box_by_prep(agg_df_max_prep, 'days_same_prep', 'Consecutive Days with Votes Unmoved in each P-Rep Node')

# saving
plt.savefig(os.path.join(resultsPath, '07_days_votes_unmoved_by_prep_age.png'))


