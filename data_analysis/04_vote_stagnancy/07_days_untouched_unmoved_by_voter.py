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

# Need to run it in sequence as it may need data from previous scripts (04 - 06)

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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Pure (unchanged/untouched) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting maximum unchanged days  -- this seems like the best method here
agg_df_max_pure = agg_df.copy()
agg_df_max_pure = agg_df_max_pure[agg_df_max_pure['vote_status'] == 'unchanged']  # get only 'pure' == unchanged days that were stagnant
idx = agg_df_max_pure.groupby(['delegator','validator_name'])['days_pure'].transform(max) == agg_df_max_pure['days_pure']
agg_df_max_pure = agg_df_max_pure[idx].reset_index(drop=True)

# adding max cumulative votes here
# agg_df_max = pd.merge(agg_df_max, max_votes, on=['delegator', 'validator_name'], how='left')
# agg_df_max['pct_votes'] = agg_df_max['cum_votes'] / agg_df_max['max_cum_votes']

# adding some months variables
agg_df_max_pure['months_pure'] = np.ceil(agg_df_max_pure['days_pure'] / 30).astype(int)
agg_df_max_pure['months_same_prep'] = np.ceil(agg_df_max_pure['days_same_prep'] / 30).astype(int)
agg_df_max_pure['total_voting_months'] = agg_df_max_pure['total_voting_months'].astype(int)
agg_df_max_pure['how_many_prep_voted_mean'] = agg_df_max_pure['how_many_prep_voted_mean'].astype(int)
agg_df_max_pure['prep_months_around_total'] = np.ceil(agg_df_max_pure['prep_days_around_total'] / 30).astype(int)

# unique count of voter
unique_n_voter = agg_df_max_pure.groupby(['total_voting_months'])['delegator'].nunique().T.squeeze().apply('(n={:,})'.format).tolist()


# violinplot by total voting months ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
sns.set(style="ticks")
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(9, 6))
ax = sns.violinplot(x='total_voting_months',
                    y='days_pure',
                    data=agg_df_max_pure,
                    linewidth=1.8,
                    dodge=False,
                    scale="width",
                    palette=sns.cubehelix_palette(len(unique_n_voter), start=.5, rot=-.75))
ax.set_xlabel('Voter Age (months)', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Days', fontsize=14, weight='bold', labelpad=10)
plt.title('Consecutive Days with Votes Untouched by Voting Duration', fontsize=14, weight='bold')
sns.despine(offset=10, trim=True)
ax.grid(False)

# to get positions to add other details in the graph
ymin, ymax = ax.get_ylim()
ymin_pos = np.empty(len(unique_n_voter))
ymin_pos.fill(ymin)

# adding median days
medians = agg_df_max_pure.groupby(['total_voting_months'])['days_pure'].median().astype(int).values
pos = range(len(unique_n_voter))
for tick,label in zip(pos,ax.get_xticklabels()):
    ax.text(pos[tick], ymin_pos[tick] + - 5, medians[tick].astype(str) + ' days' + '\n' + unique_n_voter[tick],
            horizontalalignment='center', size='x-small', color='w', weight='semibold')

plt.tight_layout()

# saving
plt.savefig(os.path.join(resultsPath, '04_days_votes_unchanged_by_voter_age.png'))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Same P-Rep (unmoved) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting maximum same p-rep days  -- this seems like the best method here

agg_df_max_prep = agg_df.copy()
idx = agg_df_max_prep.groupby(['delegator','validator_name'])['days_same_prep'].transform(max) == agg_df_max_prep['days_same_prep']
agg_df_max_prep = agg_df_max_prep[idx].reset_index(drop=True)
agg_df_max_prep = agg_df_max_prep.drop_duplicates(['delegator', 'validator_name'])

agg_df_max_prep['months_pure'] = np.ceil(agg_df_max_prep['days_pure'] / 30).astype(int)
agg_df_max_prep['months_same_prep'] = np.ceil(agg_df_max_prep['days_same_prep'] / 30).astype(int)
agg_df_max_prep['total_voting_months'] = agg_df_max_prep['total_voting_months'].astype(int)
agg_df_max_prep['how_many_prep_voted_mean'] = agg_df_max_prep['how_many_prep_voted_mean'].astype(int)
agg_df_max_prep['prep_months_around_total'] = np.ceil(agg_df_max_prep['prep_days_around_total'] / 30).astype(int)


# violinplot by total voting months ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
sns.set(style="ticks")
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(9, 6))
ax = sns.violinplot(x='total_voting_months', y='days_same_prep',
                    data=agg_df_max_prep, linewidth=1.8, dodge=False, scale="width",
                    palette=sns.cubehelix_palette(len(unique_n_voter), start=.5, rot=-.75))
ax.set_xlabel('Voter Age (months)', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Days', fontsize=14, weight='bold', labelpad=10)
plt.title('Consecutive Days with Votes Unmoved by Voting Duration', fontsize=14, weight='bold')
sns.despine(offset=10, trim=True)
ax.grid(False)

# to get positions to add other details in the graph
ymin, ymax = ax.get_ylim()
ymin_pos = np.empty(len(unique_n_voter))
ymin_pos.fill(ymin)

# adding median days
medians = agg_df_max_prep.groupby(['total_voting_months'])['days_same_prep'].median().astype(int).values
pos = range(len(unique_n_voter))
for tick,label in zip(pos,ax.get_xticklabels()):
    ax.text(pos[tick], ymin_pos[tick] + - 5, medians[tick].astype(str) + ' days' + '\n' + unique_n_voter[tick],
            horizontalalignment='center', size='x-small', color='w', weight='semibold')

plt.tight_layout()


# saving
plt.savefig(os.path.join(resultsPath, '05_days_votes_unmoved_by_voter_age.png'))
