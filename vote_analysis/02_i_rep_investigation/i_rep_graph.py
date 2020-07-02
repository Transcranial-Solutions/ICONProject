

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt # for improving our visualizations
import matplotlib.lines as mlines
import seaborn as sns
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading/saving
currPath = os.getcwd()
inPath = 'E:\\Transcranial_Solutions\\Analysis\\i_rep_and_bond_requirement\\'


# read data
g1 = pd.read_csv(os.path.join(inPath, 'graph_1.csv'))
g2 = pd.read_csv(os.path.join(inPath, 'projection.csv'))
g3 = pd.read_csv(os.path.join(inPath, 'irep.csv'))


def add_error_and_sort(df, add_error=0, error=0):
    df = df.groupby(['Date']).mean().reset_index()

    if add_error == 1:
        df['Error'] = g1['Bond Requirement (%)'] * error
    else:
        pass

    df['date'] = pd.to_datetime(g1['Date']).dt.strftime("%Y-%m-%d")
    df = df.sort_values(by='date')
    return(df)

g1 = add_error_and_sort(g1, 1, 0.05)
g2 = add_error_and_sort(g2, 1, 0.15)
g3 = add_error_and_sort(g3)



sns.set(style="darkgrid")
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(8, 5))
ax = sns.lineplot(x="Date", y="irep", data=g3, color="red", sort=False)
# plt.grid(b=None)
ax.grid(False)

ax.set_ylabel('i_rep')
ax.set_ylim(0,)
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('Change in i_rep Over Time', fontsize=12, weight='bold')
ax.yaxis.grid(True)

n = 30 # Keeps every 7th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()


plt.savefig(os.path.join(inPath, "irep_change.png"))


sns.set(style="darkgrid")
## current
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(g1['Date'], g1['Bond Requirement (%)'], yerr=g1['Error'], align='center', alpha=0.5, ecolor='white', capsize=0)
ax.grid(False)
n = 15 # Keeps every 7th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
ax.set_ylabel('Bond Requirement (%)')
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('% of bond saved over time (6 months)', fontsize=12, weight='bold')
ax.yaxis.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(inPath, "bond_in_6_months.png"))


sns.set(style="darkgrid")

## projection
# sns.set(style="darkgrid")
plt.rcParams['axes.facecolor'] = 'black'
fig, ax = plt.subplots(figsize=(16, 5))
ax.bar(g2['Date'], g2['Bond Requirement (%)'], yerr=g2['Error'], align='center', alpha=0.5, ecolor='white', capsize=0)
# ax.grid(False)
ax.grid(False)
n = 30
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
ax.set_ylabel('Bond Requirement (%)')
ax.set_xlabel('Time (mm/dd/yyyy)')
ax.set_title('Linear projection earnings for 14k i_rep (1 year)', fontsize=12, weight='bold')
ax.yaxis.grid(True)
plt.tight_layout()

plt.savefig(os.path.join(inPath, "bond_in_1_year.png"))




################################
#
# votes_all = pd.read_csv(os.path.join(inPath, 'votes_all.csv'))
# first_vote = votes_all.sort_values(by=['validator_name', 'date']).groupby(['validator_name']).first().reset_index()
# first_vote = first_vote[['validator_name', 'date']]
# first_vote['first_date'] = 'FIRST'
#
# rewards = pd.read_csv(os.path.join(inPath, 'rewards_all.csv'))
# rewards = rewards.groupby(['name', 'date']).sum()['rewards'].reset_index().rename(columns={'name': 'validator_name'})
#
# # pivot wider (format for visualisation)
# rewards_wider = rewards.pivot(index='validator_name',
#                       columns='date',
#                       values='rewards').reset_index()
#
# rewards_longer = rewards_wider.melt(id_vars=['validator_name'], var_name='date', value_name='rewards').sort_values(by=['validator_name', 'date']).reset_index(drop=True)
#
# merged = pd.merge(rewards_longer, first_vote, on=['validator_name', 'date'], how='left')
#
# s = pd.Series()
# 2423423432 try here, get last non_na data
#
# merged.groupby(['validator_name'])['date'].nth([0,-1]).reset_index()
#
# merged['last_date'] = merged.groupby(['validator_name']).apply(merged.ix[-1[]])



# #votes all
# votes_all = pd.read_csv(os.path.join(inPath, 'votes_all.csv'))
# votes_all = votes_all.drop(columns=['block_id','delegator','validator','created_at','datetime','day'])
# votes_all = votes_all.groupby(['validator_name','date'])['votes'].agg('sum').reset_index()
# votes_all = votes_all.sort_values(by=['validator_name','date'])
# votes_all['cum_votes'] = votes_all.groupby(['validator_name'])['votes'].cumsum()
# votes_all['five_percent'] = votes_all['cum_votes']*0.05
#
#
# # rewards just to check stuff for now
# rewards = pd.read_csv(os.path.join(inPath, 'rewards_all.csv'))
# rewards = rewards.drop(columns=['block_id','datetime','address','alpha-3','city','logo','country','sub-region'])
# rewards = rewards.sort_values(by=['name','date']).rename(columns={'name': 'validator_name'})
# rewards['cum_rewards'] = rewards.groupby(['validator_name'])['rewards'].cumsum()
# rewards['twentyfive_percent'] = rewards['cum_rewards']*0.25
#
#
# test = pd.merge(rewards, votes_all, on=['validator_name', 'date'], how='left')
#
# test.to_csv(os.path.join(inPath, 'test_rewards.csv'), index=False)




#
# # Most popular by 1~2 prep voters (not unique per voter (multiple pairing) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# # barplot
# sns.set(style="darkgrid")
# plt.style.use("dark_background")
# ax = plt.subplots(figsize=(7, 5))
# ax = sns.barplot(x="Date", y="Bond Requirement (%)", data=g1, color="b")
# ax.set_xlabel('P-Reps', fontsize=13)
# ax.set_ylabel('Proportions (%)', fontsize=13)
# ax.set_title('Most Popular P-Reps by those voting 1~2 P-Reps on Average', fontsize=12, weight='bold')
# ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
# plt.xticks(fontsize=10)
# plt.yticks(fontsize=10)
# plt.tight_layout()
# #save
# ax.figure.savefig(os.path.join(resultsPath, "popular_prep_by_1_2_voters.png"))
#
# # stacked barplot
# indexing_cols = one_prep_voter_count_all['Category'][0:6].tolist()
# indexing_cols.append('Others')
#
# df1 = one_prep_voter_count_month.pivot(index='month', columns='Category', values='pct').fillna(0)
# df1 = df1.reindex(columns=indexing_cols)
# index = pd.Index(sorted(one_prep_voter_count_month['month'].unique()), name='month')
#
# sns.set(style="dark")
# plt.style.use("dark_background")
# ax1 = df1.plot(kind='bar', stacked=True, figsize=(8.5, 6.5))
# plt.title('Voters delegating 1~2 P-Reps on Average Over Time \n (Top 6 P-Reps and Others)', fontsize=12, weight='bold')
# ax1.set_xlabel('Time (month)')
# ax1.set_ylabel('Proportions (%)')
# ax1.set_xticklabels(ax1.get_xticklabels(), rotation=0, ha="center")
# ax2 = ax1.twinx()
# lines = plt.plot(n_one_prep_voter_month['month'],
#                n_one_prep_voter_month['counts'])
# ax2.set_ylabel('Voter Count')
# plt.setp(lines, 'color', 'r', 'linewidth', 2.0)
# ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15),
#           fancybox=True, shadow=True, ncol=5)
# color = 'tab:red'
# red_line = mlines.Line2D([], [], color=color, label='Red line represents voter count', linewidth=3)
# plt.legend(handles=[red_line], loc='upper left', fontsize='small', bbox_to_anchor=(0.67, -0.09), frameon=False)
# plt.tight_layout(rect=[0,0,1,1])
#
# plt.savefig(os.path.join(resultsPath, "popular_prep_by_1_2_voters_per_month.png"))
#
