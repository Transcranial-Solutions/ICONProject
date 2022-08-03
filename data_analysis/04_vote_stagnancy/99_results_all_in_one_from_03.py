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

# All-in-one results (with data preparation)
# Run this after 3_vote_status.py and it will generate all the results at once.
# It'll take a few minutes.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import scipy.stats as sp
import seaborn as sns
import os
from datetime import date


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# today date
today = date.today()
day1 = '_' + today.strftime("%Y_%m_%d")

# making path for loading/saving
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")
outPath = os.path.join(currPath, "04_vote_stagnancy")
resultsPath = os.path.join(outPath, "results" + day1)
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
current_prep_voters = current_prep_votes.groupby(['validator_name'])['delegator'].agg('count').reset_index().rename(columns={'delegator': 'total_voter_count'})

current_prep_votes = current_prep_votes.\
    drop_duplicates(['validator_name','prep_total_votes', 'prep_ranking'])[['validator_name','prep_total_votes', 'prep_ranking']].\
    reset_index(drop=True)

current_prep_votes = pd.merge(current_prep_votes, current_prep_voters, on='validator_name', how='left')

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



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#### plotting
# seeing how many voters been voting entire time -- baseline

# better to average it
total_voting_days_plot = agg_df.groupby(['delegator', 'total_voting_months']).agg('mean').reset_index()
total = float(len(total_voting_days_plot))  # one person per row (this is for % in the figure)
total_voting_days_plot = total_voting_days_plot.groupby('total_voting_months')['delegator'].count().reset_index()

# barplot
sns.set(style="darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(8, 6))
ax = sns.barplot(x="total_voting_months",
                 y="delegator",
                 data=total_voting_days_plot,
                 edgecolor="white", palette=sns.cubehelix_palette(len(total_voting_days_plot), start=.5, rot=-.75)) #, color="b")
ax.grid(False)
ax.set_xlabel('Voter Age (months)', fontsize=12, labelpad=10, weight='bold')
ax.set_ylabel('Wallets', fontsize=12, labelpad=10, weight='bold')

ax.set_title('Number of Wallets by Voting Duration', fontsize=14, weight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

all_height = []
for p in ax.patches:
    height = p.get_height()
    ax.text(p.get_x() + p.get_width() / 2.,
            height + 30,
            '{0: .0%}'.format(height / total),
            ha="center")
    all_height.append(height)

ax.set(ylim=(0, max(all_height) + max(all_height)*0.1))

plt.tight_layout()


# saving
plt.savefig(os.path.join(resultsPath, '01_wallet_count_by_voter_age' + day1 + '.png'))




#~~~~~~~~~~~~~~~~~~~~~~ proportions by sum total / sum count by number of months staked ~~~~~~~~~~~~~~~~~~~~~~~#
pct_df = agg_df.groupby(['vote_status', 'total_voting_months'])['days_pure'].agg('sum').reset_index()
pct_df['total'] = pct_df.groupby(['total_voting_months'])['days_pure'].transform('sum') # one person per row (this is for % in the figure)
total = pct_df['total'].apply('{:,}'.format).unique()
pct_df['pct'] = pct_df.days_pure / pct_df.total * 100
pct_df = pct_df.pivot(index='total_voting_months', columns='vote_status', values='pct')
pct_df = pct_df[['unchanged', 'unvoted', 'down', 'up', 'voted']]


sns.set(style="darkgrid")
plt.style.use(['dark_background'])
ax = pct_df.plot(kind='bar', stacked=True, figsize=(8, 6), edgecolor="black")
ax.set_xlabel('Voter Age (months)', fontsize=12, weight='bold')
ax.set_ylabel('Proportions (%)', fontsize=12, weight='bold')
ax.set_title('Proportion of Voting Activity by Voting Duration', fontsize=14, weight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=0, ha="right")
ax.grid(False)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)


for i in range(len(total)):
    p = ax.patches[i]
    height = 100
    ax.text(p.get_x() + p.get_width() / 2., height + 1,
                total[i],
                ha="center",
            fontsize=10)

handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels),
           loc='center', bbox_to_anchor=(0.5, -0.18),
           fancybox=True, shadow=True, ncol=5)
plt.tight_layout(rect=[0,0,1,1])

# saving
plt.savefig(os.path.join(resultsPath, '02_Voting_Activity_By_Voter_Age' + day1 + '.png'))



#~~~~~~~~~~~~~~~~~~~~~~ proportions by sum total / sum count by P-Reps ~~~~~~~~~~~~~~~~~~~~~~~#
pct_df = agg_df.groupby(['vote_status', 'validator_name'])['days_pure'].agg('sum').reset_index()
pct_df['total'] = pct_df.groupby(['validator_name'])['days_pure'].transform('sum')
pct_df['pct'] = pct_df.days_pure / pct_df.total * 100
pct_df = pct_df.pivot(index='validator_name',columns='vote_status', values='pct').sort_values(by='unchanged')
pct_df = pct_df[['unchanged', 'unvoted', 'down', 'up', 'voted']]


sns.set(style="darkgrid")
plt.style.use(['dark_background'])
ax = pct_df.plot(kind='bar', stacked=True, figsize=(18, 10), edgecolor="black")
ax.set_xlabel('P-Reps', fontsize=12, weight='bold')
ax.set_ylabel('Proportions (%)', fontsize=12, weight='bold')
ax.set_title('Proportion of Voting Activity in each P-Rep Node', fontsize=14, weight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.grid(False)
plt.xticks(fontsize=8)
plt.yticks(fontsize=12)
handles, labels = ax.get_legend_handles_labels()
ax.legend(reversed(handles), reversed(labels),
           loc='center', bbox_to_anchor=(0.18, 1),
           fancybox=True, shadow=True, ncol=5)
plt.tight_layout(rect=[0,0,1,1])

# saving
plt.savefig(os.path.join(resultsPath, '03_Voting_Activity_By_PRep_Age' + day1 + '.png'))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Pure (unchanged/untouched) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
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
plt.savefig(os.path.join(resultsPath, '04_days_votes_unchanged_by_voter_age' + day1 + '.png'))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Same P-Rep (unmoved) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
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
plt.savefig(os.path.join(resultsPath, '05_days_votes_unmoved_by_voter_age' + day1 + '.png'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plots by P-Rep ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

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
plt.savefig(os.path.join(resultsPath, '06_days_votes_untouched_by_prep_age' + day1 + '.png'))

# Same P-Rep by P-Rep
plot_box_by_prep(agg_df_max_prep, 'days_same_prep', 'Consecutive Days with Votes Unmoved in each P-Rep Node')

# saving
plt.savefig(os.path.join(resultsPath, '07_days_votes_unmoved_by_prep_age' + day1 + '.png'))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ correlation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

def corr_plot(df, inVar1, inVar2, this_xlabel, this_ylabel, this_title):

    m_var1 = df.groupby('validator_name')[[inVar1]].apply(np.median).reset_index(name='var1')
    m_var2 = df.groupby('validator_name')[[inVar2]].apply(np.median).reset_index(name='var2')
    m_days_ranking = pd.merge(m_var1, m_var2, on='validator_name', how='left')

    # plot
    sns.set(style="ticks")
    plt.style.use("dark_background")
    g = sns.jointplot("var1", "var2", data=m_days_ranking,
                      kind="reg", height=6, truncate=False, color='m',
                      joint_kws={'scatter_kws': dict(alpha=0.5)}
                      ).annotate(sp.pearsonr)
    g.set_axis_labels(this_xlabel, this_ylabel, fontsize=14, weight='bold', labelpad=10)
    g = g.ax_marg_x
    g.set_title(this_title, fontsize=12, weight='bold')
    plt.tight_layout()

# days unchanged vs P-Rep ranking
corr_plot(agg_df_max_pure,
          'days_pure',
          'prep_ranking',
          'Days unchanged (median)',
          'P-Rep Ranking (median)',
          'Days of Votes Untouched vs P-Rep Ranking')

# saving
plt.savefig(os.path.join(resultsPath, '08_days_votes_untouched_vs_PRep_ranking_corr' + day1 + '.png'))

# days of same P-Rep vs P-Rep ranking
corr_plot(agg_df_max_prep,
          'days_same_prep',
          'prep_ranking',
          'Days in same P-Rep (median)',
          'P-Rep Ranking (median)',
          'Days of Votes Unmoved vs P-Rep Ranking')

# saving
plt.savefig(os.path.join(resultsPath, '09_days_votes_unmoved_vs_PRep_ranking_corr' + day1 + '.png'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ days and votes stagnancy ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# data for today to be used for current votes
today_df = count_cum_status[count_cum_status['date'].max() == count_cum_status['date']]
today_df = shorten_prep_name(today_df)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ data for Scott ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# this data will be used for 'vote spreading' implementation

not_stagnant_user = today_df[today_df['days_pure'] < 30].drop_duplicates(['delegator'])[['delegator']]
today_df_only_stag_user = today_df.copy()

# today_df_only_stag_user = today_df_only_stag_user[~today_df_only_stag_user.isin(not_stagnant_user.to_dict('l')).any(1)]
today_df_only_stag_user = today_df_only_stag_user[~today_df_only_stag_user.isin(not_stagnant_user.to_dict('l')).iloc[:, 0]]

def stag_votes_scott(df, stag_days, inVar):
    # df = df[df[inVar] >= stag_days] # outdated -- this is by P-Rep, not by user
    temp_df = df[df[inVar] < stag_days].drop_duplicates(['delegator'])[['delegator']] # finding users with less than stag_days
    df = df[~df.isin(temp_df.to_dict('l')).iloc[:, 0]] # remove these users from the data
    df = df.groupby(['validator_name','date'])['cum_votes'].agg(['count', 'sum']).reset_index()
    df = pd.merge(df, current_prep_votes, on='validator_name', how='left')
    # df['pct_stagnant'] = (df['sum'] / df['prep_total_votes'])
    df['included_network_total_votes'] = df.groupby('date')['prep_total_votes'].transform('sum')
    df['current_network_total_votes'] = current_prep_votes['prep_total_votes'].sum()
    # df['pct_entire_votes'] = (df['sum'] / df['entire_votes'])
    # df['pct_prep_votes'] = (df['prep_total_votes'] / df['entire_votes'])
    df = df.sort_values(by=['prep_ranking'])
    # df['pct_label'] = df['pct_prep_votes'].map("{:.2%}".format) + ' / ' + df['pct_entire_votes'].map("{:.2%}".format)
    df = df.rename(columns={'validator_name': 'P-Reps',
                            'date': 'extraction_date',
                            'count': 'stagnant_voter_count',
                            'sum': 'stagnant_votes'})
    df = df[['P-Reps', 'stagnant_voter_count', 'total_voter_count',
             'stagnant_votes', 'prep_total_votes', 'included_network_total_votes', 'current_network_total_votes',
             'prep_ranking','extraction_date']]
    return(df)

## 30 days and 90 days
stag_days = 30
stag_days_df = stag_votes_scott(df=today_df, stag_days=stag_days, inVar='days_pure')
stag_days_df.to_csv(os.path.join(resultsPath, 'vote_stagnancy_' + str(stag_days) + '_days' + '.csv'), index=False)

stag_days_df_30 = stag_days_df.copy()
stag_days_df_30 = stag_days_df_30[['P-Reps', 'prep_ranking', 'stagnant_votes']].rename(columns={'stagnant_votes': 'stagnant_votes_30_days'})

stag_days = 60
stag_days_df = stag_votes_scott(df=today_df, stag_days=stag_days, inVar='days_pure')
stag_days_df.to_csv(os.path.join(resultsPath, 'vote_stagnancy_' + str(stag_days) + '_days' + '.csv'), index=False)

stag_days_df_60 = stag_days_df.copy()
stag_days_df_60 = stag_days_df_60[['P-Reps', 'prep_ranking', 'stagnant_votes']].rename(columns={'stagnant_votes': 'stagnant_votes_60_days'})

stag_days = 90
stag_days_df = stag_votes_scott(df=today_df, stag_days=stag_days, inVar='days_pure')
stag_days_df.to_csv(os.path.join(resultsPath, 'vote_stagnancy_' + str(stag_days) + '_days' + '.csv'), index=False)

stag_days_df_90 = stag_days_df.copy()
stag_days_df_90 = stag_days_df_90[['P-Reps', 'prep_ranking', 'stagnant_votes']].rename(columns={'stagnant_votes': 'stagnant_votes_90_days'})

current_prep_votes_csv = current_prep_votes.rename(columns={'validator_name': 'P-Reps'}).sort_values(by='prep_ranking')
current_prep_votes_csv.to_csv(os.path.join(resultsPath, 'current_votes.csv'), index=False)

current_prep_ranking = current_prep_votes_csv[['P-Reps', 'prep_ranking']]

stag_days_df_all = current_prep_ranking.\
    merge(stag_days_df_30, on=['P-Reps', 'prep_ranking'], how='left').\
    merge(stag_days_df_60, on=['P-Reps', 'prep_ranking'], how='left').\
    merge(stag_days_df_90, on=['P-Reps', 'prep_ranking'], how='left').fillna(0)

stag_days_df_all.to_csv(os.path.join(resultsPath, 'vote_stagnancy_all' + '.csv'), index=False)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


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
plt.savefig(os.path.join(resultsPath, '10_vote_stagnancy_untouched_90_days' + day1 + '.png'))

# same P-Rep
stag_votes_df = stag_votes(df=today_df, stag_days=90, inVar='days_same_prep', stag_type='Unmoved')

# saving
plt.savefig(os.path.join(resultsPath, '11_vote_stagnancy_unmoved_90_days' + day1 + '.png'))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Historical vote stagnancy ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# getting total stagnant days and votes over time (going back to the past from today)
def votes_per_day(inData, inVar1, inVar2, type, this_interval, max_days):
    votes_per_day = inData.groupby('date')[inVar1].agg('sum').reset_index().rename(columns={'cum_votes': 'total_votes'})
    votes_per_day_pure = inData.groupby(['date', inVar2])[inVar1].agg('sum').reset_index().rename(columns={'cum_votes': 'votes'})
    votes_per_day_pure['cum_votes'] = votes_per_day_pure.sort_values(by=['date', inVar2]).groupby('date')['votes'].cumsum()
    df = pd.merge(votes_per_day_pure, votes_per_day, on='date', how='left')
    df = df[df['date'].max() == df['date']]
    df['stag_votes'] = df['total_votes'] - df['cum_votes']
    df['pct_stag'] = df['stag_votes'] / df['total_votes'] * 100
    df = df.rename(columns={inVar2: 'cum_days'})
    df['Stagnancy'] = type
    # df = df.rename(columns={'stag_votes': 'stag_votes_' + type, 'pct_stag': 'pct_stag_' + type})
    df = df.drop(columns=['votes', 'cum_votes', 'total_votes','date'])
    df = df[:-1] # remove last row
    df = df[:1].append(df.iloc[this_interval-1:max_days:this_interval, :]) # every nth day with maximum
    return(df)

# add variables
max_days = 300
this_interval = 20

# pure type
votes_per_day_pure = votes_per_day(inData=count_cum_status,
                                   inVar1='cum_votes',
                                   inVar2='cum_days_pure',
                                   type='Untouched',
                                   max_days=max_days,
                                   this_interval=this_interval)

# same p-rep
votes_per_day_same_prep = votes_per_day(inData=count_cum_status,
                                        inVar1='cum_votes',
                                        inVar2='cum_days_same_prep',
                                        type='Unmoved',
                                        max_days=max_days,
                                        this_interval=this_interval)

# merge
votes_per_day_df = votes_per_day_pure.append(votes_per_day_same_prep)
stag_votes_label = votes_per_day_df['stag_votes'].map(lambda x: '{:,.0f}'. format(x/1000000) + 'M').to_list()

# plot
sns.set(style="ticks")
plt.style.use("dark_background")
fig, ax = plt.subplots(figsize=(9, 7))
sns.lineplot(x="cum_days", y="pct_stag", data=votes_per_day_df, hue='Stagnancy', palette=sns.color_palette('husl', n_colors=2))
ax.set_xlabel('Stangnant duration (days)', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Stagnant Votes (%)', fontsize=14, weight='bold', labelpad=10)
plt.title('Total Vote Stagnancy by Duration', fontsize=14, weight='bold', linespacing=2)
ax.set_xlim([-5, max_days])
ax.set_ylim([-5, 110])
sns.despine(offset=10, trim=True)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

xs = votes_per_day_df['cum_days']
ys = votes_per_day_df['pct_stag']

# zip joins x and y coordinates in pairs
for x,y,z in zip(xs,ys,stag_votes_label):

    plt.annotate(z, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,5), # distance from text to points (x,y)
                 ha='left', # horizontal alignment can be left, right or center
                 fontsize=9,
                 color='w',
                 weight='bold',
                 rotation=5)

plt.tight_layout()

# saving
plt.savefig(os.path.join(resultsPath, '12_vote_stagnancy_by_votes_going_backward' + day1 + '.png'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Vote Stagnancy Time-Series ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Vote Stagnancy -- by having voted or not for that day

## vote stagnancy data preparation
vote_stagnancy = count_cum_status.copy()
vote_stagnancy['Stagnant'] = np.where(vote_stagnancy['vote_status'] != 'unchanged', 'Vote Change (Up/Down)', 'No Vote Change')
vote_stagnancy = vote_stagnancy.groupby(['date', 'Stagnant'])['cum_votes'].agg(['sum', 'count']).reset_index()
vote_stagnancy = vote_stagnancy.rename(columns={'sum': 'votes', 'count': 'voters'})

# vote stagnancy % by votes
vote_stagnancy['total_votes'] = vote_stagnancy.groupby('date')['votes'].transform('sum')
vote_stagnancy['pct_votes'] = vote_stagnancy['votes'] / vote_stagnancy['total_votes']

# vote stagnancy % by voters
vote_stagnancy['total_voters'] = vote_stagnancy.groupby('date')['voters'].transform('sum')
vote_stagnancy['pct_voters'] = vote_stagnancy['voters'] / vote_stagnancy['total_voters']
vote_stagnancy = vote_stagnancy[vote_stagnancy['date'] != '2019-08-26']  # for order of the plot
vote_stagnancy = vote_stagnancy.sort_values(by=['Stagnant', 'date'])


# get 1st in group
first_vote_stagnancy = vote_stagnancy.groupby('Stagnant').nth(0).reset_index()
# getting every nth data
n = 5
idx = vote_stagnancy.groupby('Stagnant', group_keys=False).apply(lambda x: x.index[n-1::n].to_series())
vote_stagnancy = first_vote_stagnancy.append(vote_stagnancy.loc[idx])
vote_stagnancy = vote_stagnancy.sort_values(by=['Stagnant', 'date'])


# plot
sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='date', y='votes', hue="Stagnant", data=vote_stagnancy, palette=sns.color_palette('husl', n_colors=2))
h,l = ax.get_legend_handles_labels()

ax.set_xlabel('Date', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Votes (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Daily Vote Stagnancy \n (based on active voting wallets per day)', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/10e6) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
sns.despine(offset=10, trim=True)

xs = vote_stagnancy['date']
ys = vote_stagnancy['votes']

porcent = vote_stagnancy.copy()
porcent['pct_votes'] = porcent['pct_votes'].apply('{:.0%}'.format)
porcent.loc[porcent['Stagnant'] == 'Vote Change (Up/Down)', 'pct_votes'] = ''
porcent = porcent['pct_votes']

# zip joins x and y coordinates in pairs
n = 0
for x,y,z in zip(xs,ys,porcent):
    if n % 2 == 0:

        plt.annotate(z, # this is the text
                     (x,y), # this is the point to label
                     textcoords="offset points", # how to position the text
                     xytext=(0,10), # distance from text to points (x,y)
                     ha='center', # horizontal alignment can be left, right or center
                     fontsize=8,
                     color='w',
                     weight='bold')
    n = n+1

plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.legend(h[1:],l[1:],ncol=1,
          # title="Voting Activity Stagnation",
          fontsize=10,
          loc='upper left')
n = 2  # Keeps every n-th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()


# saving
plt.savefig(os.path.join(resultsPath, '13_vote_stagnancy_by_votes_over_time' + day1 + '.png'))




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Vote Spread by counting Number of P-Reps voted ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#
# temp_prep_voted = count_cum_status[['date', 'delegator', 'validator_name', 'how_many_prep_voted']].\
#     sort_values(by=['date', 'delegator', 'validator_name']).reset_index(drop=True)
#
# # date to select (same range as above graph)
# date_of_interest = xs.drop_duplicates().reset_index(drop=True)
# date_of_interest = date_of_interest.iloc[::6]
# month_of_interest = date_of_interest.str.split('-').str[0] + '-' + date_of_interest.str.split('-').str[1]
# date_of_interest_lag = date_of_interest.shift().bfill()
#
# # running a loop to get data between first date to date of interest
# # count total number of unique P-Reps during the period (works as a cumulative count here)
# prep_voted = []
# prep_voted_max = []
# for x,y in zip(date_of_interest_lag, date_of_interest):
#
#     # for cumulative count in given months (0-1, 0-2, 0-3 etc)
#     mask1 = (temp_prep_voted['date'] >= date_of_interest[0]) & (temp_prep_voted['date'] <= y)
#     df1 = temp_prep_voted.loc[mask1]
#
#     # for mean number between the duration (overlapping)
#     mask2 = (temp_prep_voted['date'] >= x) & (temp_prep_voted['date'] <= y)
#     df2 = temp_prep_voted.loc[mask2]
#
#     # for count between the duration (overlapping)
#     df1df2 = temp_prep_voted.loc[mask2]
#
#     # to get maximum number over the entire period (cumulative) by month
#     df1_mask1 = df1.groupby('delegator')['validator_name'].nunique().reset_index()
#     df1_mask1 = df1_mask1.rename(columns={'validator_name': 'cum_prep_count_continuous'})
#
#     df1_mask2 = df1df2.groupby('delegator')['validator_name'].nunique().reset_index()
#     df1_mask2 = df1_mask2.rename(columns={'validator_name': 'cum_prep_count'})
#
#     df2_mask2 = df2.drop_duplicates(['delegator', 'how_many_prep_voted', 'date'])[['delegator', 'how_many_prep_voted', 'date']] # how many prep voted per day
#     df2_mask2 = df2_mask2.groupby('delegator')['how_many_prep_voted'].agg('mean').round().astype(int).reset_index()
#     df2_mask2 = df2_mask2.rename(columns={'how_many_prep_voted': 'mean_prep_count'})
#
#     df = pd.merge(df1_mask2, df2_mask2, on='delegator', how='inner')
#     df = df.assign(date=y)
#
#     df_max = df1_mask1.assign(date=y)
#
#     # gathering data
#     prep_voted.append(df)
#     prep_voted_max.append(df_max)
#
#     print('Done: ' + x + ' & ' + y + ' ...')
#
# prep_voted_per_date = pd.concat(prep_voted).sort_values(by='date')
# prep_voted_history = pd.concat(prep_voted_max).sort_values(by='date')
#
#
#
# prep_voted_per_date = pd.melt(prep_voted_per_date,
#                              id_vars=['delegator','date'],
#                              value_vars=['cum_prep_count', 'mean_prep_count'])
#
# # rename values
# prep_voted_per_date['variable'] = np.where(prep_voted_per_date['variable'] == 'cum_prep_count', 'Cumulative', 'Average')
# prep_voted_per_date = prep_voted_per_date.sort_values(by='variable')
# prep_voted_per_date['month'] = prep_voted_per_date['date'].str.split('-').str[0] + '-' + prep_voted_per_date['date'].str.split('-').str[1]
#
# # getting max for the month period
# cum_max_by_date = prep_voted_history.groupby('date')['cum_prep_count_continuous'].max().reset_index()
# cum_max_by_date['month'] = cum_max_by_date['date'].str.split('-').str[0] + '-' + cum_max_by_date['date'].str.split('-').str[1]
#
#
# # plot
# sns.set(style="ticks")
# plt.style.use("dark_background")
#
# # Initialize the figure
# f, ax = plt.subplots(figsize=(10, 8))
# sns.violinplot(x="month", y="value", hue="variable", data=prep_voted_per_date, palette=sns.color_palette('husl', n_colors=2))
#
# sns.pointplot(x="month", y="cum_prep_count_continuous",
#               data=cum_max_by_date, dodge=.532, join=False, markers='*')
#
# h,l = ax.get_legend_handles_labels()
#
# ax.set_xlabel('Month(s)', fontsize=14, weight='bold', labelpad=10)
# ax.set_ylabel('P-Reps Voted', fontsize=14, weight='bold', labelpad=10)
# ax.set_title('Number of P-Reps Voted Over Time', fontsize=14, weight='bold', linespacing=1.5)
# sns.despine(offset=10, trim=True)
#
# ax.legend(h[0:], l[0:], ncol=1,
#           fontsize=10,
#           loc='upper left')
#
# plt.tight_layout()
#
# # saving
# plt.savefig(os.path.join(resultsPath, '14_vote_spreading_over_time.png'))

