#########################################################################
## Project: ICON Voting Behaviour                                      ##
## Date: May 2020                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# Vote distribution / counting

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
inPath = os.path.join(currPath, "output")
rewardsPath = os.path.join(inPath, "rewards")
votesPath = os.path.join(inPath, "votes")
resultsPath = os.path.join(currPath, "01_voting_behaviour")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)
resultsPath_2 = os.path.join(resultsPath, "results")
if not os.path.exists(resultsPath_2):
    os.mkdir(resultsPath_2)

# read data
prep_df = pd.read_csv(os.path.join(inPath, 'final_prep_details.csv'))
votes_cumsum_longer = pd.read_csv(os.path.join(inPath, 'vote_status_per_day.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data Preparation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
### For counting
# removing 'unvoted' for counting
count_voted_prep_per_day = votes_cumsum_longer[votes_cumsum_longer['vote_status'] != 'unvoted']

count_voted_prep_per_day = count_voted_prep_per_day.groupby(['delegator', 'date']).size().reset_index(name='counts')
count_voted_prep_per_day.sort_values(by=['counts'])

count_voted_prep_per_day['month'] = pd.to_datetime(count_voted_prep_per_day['date']).dt.strftime("%Y-%m")
count_voted_prep_per_day = count_voted_prep_per_day.sort_values(['date'])

# mean by voter per month
mean_by_month = count_voted_prep_per_day.groupby(['delegator', 'month'])['counts'].mean().reset_index()
mean_by_month = mean_by_month.sort_values(['month'])

# mean by voter for entire dataset
mean_by_all = count_voted_prep_per_day.groupby(['delegator'])['counts'].mean().reset_index()


# making bins to extract group
def f(row):
    if 1 <= row['counts'] < 2:
        val = '1-2'
    elif 2 <= row['counts'] < 3:
        val = '2-3'
    elif 3 <= row['counts'] < 4:
        val = '3-4'
    elif 4 <= row['counts'] < 5:
        val = '4-5'
    elif 5 <= row['counts'] < 6:
        val = '5-6'
    elif 6 <= row['counts'] < 7:
        val = '6-7'
    elif 7 <= row['counts'] < 8:
        val = '7-8'
    elif 8 <= row['counts'] < 9:
        val = '8-9'
    elif 9 <= row['counts'] <= 10:
        val = '9-10'
    else:
        val = -1
    return val

mean_by_all['bar_bin'] = mean_by_all.apply(f, axis=1)
mean_by_month['bar_bin'] = mean_by_month.apply(f, axis=1)

# making pivot tables for voting change over time
pivot_table_month_temp = mean_by_month.groupby(['month', 'bar_bin']).size().reset_index(name='counts')
pivot_table_month_percent = pd.crosstab(pivot_table_month_temp.bar_bin,
                                        pivot_table_month_temp.month,
                                        values=pivot_table_month_temp.counts,
                                        aggfunc=np.sum).apply(lambda x: x/x.sum()).applymap(lambda x: "{:.0f}%".format(100*x))

pivot_table_month = pivot_table_month_temp.pivot(index='bar_bin', columns='month', values='counts').fillna(0)


# unique voter and prep table
unique_voter_prep = votes_cumsum_longer[['delegator', 'validator_name']]
unique_voter_prep = unique_voter_prep.drop_duplicates(subset=['delegator', 'validator_name'])

# 1~2 p-rep voters
# mean by all
one_prep_voter = mean_by_all[mean_by_all['bar_bin'] == '1-2'].reset_index(drop=True)
n_one_prep_voter_all = "(N = " + str(len(one_prep_voter.index)) + ")"
one_prep_voter = pd.merge(one_prep_voter, unique_voter_prep, how='inner', on=['delegator'])
one_prep_voter_count = one_prep_voter.groupby(['validator_name'])['bar_bin'].agg('count').reset_index()
# one_prep_voter_count = one_prep_voter_count.sort_values(by='bar_bin', ascending=False).reset_index(drop=True)
one_prep_voter_count['pct'] = one_prep_voter_count['bar_bin']/one_prep_voter_count['bar_bin'].sum() * 100
one_prep_voter_count['Category'] = one_prep_voter_count['validator_name']
one_prep_voter_count.loc[one_prep_voter_count['pct'] < 1, 'Category'] = 'Others (<1% each)'
one_prep_voter_count = one_prep_voter_count.groupby(['Category'])[['pct', 'bar_bin']].apply(sum).reset_index()
one_prep_voter_count_all = one_prep_voter_count[one_prep_voter_count.Category != 'Others (<1% each)'].sort_values(by='bar_bin', ascending=False)
one_prep_voter_count_all = one_prep_voter_count_all.append(one_prep_voter_count[one_prep_voter_count.Category == 'Others (<1% each)']).reset_index(drop=True)


# mean by month
one_prep_voter = mean_by_month[mean_by_month['bar_bin'] == '1-2'].reset_index(drop=True)
n_one_prep_voter_month = one_prep_voter.groupby(['month']).size().reset_index(name='counts')
one_prep_voter = pd.merge(one_prep_voter, unique_voter_prep, how='inner', on=['delegator'])
one_prep_voter_count = one_prep_voter.groupby(['validator_name', 'month'])['bar_bin'].agg('count').reset_index()
# one_prep_voter_count = one_prep_voter_count.sort_values(by='bar_bin', ascending=False).reset_index(drop=True)
one_prep_voter_count['pct'] = one_prep_voter_count['bar_bin']/one_prep_voter_count.groupby('month')['bar_bin'].transform('sum') * 100
one_prep_voter_count['Category'] = one_prep_voter_count['validator_name']
make_other = one_prep_voter_count.Category.isin(one_prep_voter_count_all['Category'][0:6].tolist())
one_prep_voter_count.loc[~make_other, "Category"] = 'Others'
# one_prep_voter_count.loc[one_prep_voter_count['pct'] < 5, 'Category'] = 'Others (<1% each)'
one_prep_voter_count = one_prep_voter_count.groupby(['Category', 'month'])[['pct', 'bar_bin']].apply(sum).reset_index()
one_prep_voter_count_month = one_prep_voter_count[one_prep_voter_count.Category != 'Others'].sort_values(by='bar_bin', ascending=False)
one_prep_voter_count_month = one_prep_voter_count_month.append(one_prep_voter_count[one_prep_voter_count.Category == 'Others']).reset_index(drop=True)





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ P-reps voted per voter ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# distribution density plot for number of preps voted per voter (to-date) ~~~~~~~~~~~~~~~~~#
sns.set_style("darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(7, 5))
ax = sns.distplot(mean_by_all['counts'], vertical=True,
                  kde_kws={"color": "w", "lw": 2, "label": "KDE"})\
    .set(xlabel="Density",
         ylabel="Vote Allocation")
plt.title('Distribution of Average Number of P-Reps Voted Per Voter', fontsize=12, weight='bold')
plt.tight_layout()
# save
plt.savefig(os.path.join(resultsPath_2, "n_prep_voted_per_voter_to_date.png"))

# violin plot by month ~~~~~~~~~~~~~~~~~#
plt.rcParams["axes.labelsize"] = 14
sns.set_style("darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(9, 5))
ax = sns.violinplot(x="month", y="counts", data=mean_by_month, linewidth=1.75)\
    .set(xlabel="Time (month)",
    ylabel="Vote Allocation")
plt.title('Spread of Vote Allocation Over Time (Violin plot)', fontsize=14, weight='bold')
plt.tight_layout()
# save
plt.savefig(os.path.join(resultsPath_2, "n_prep_voted_per_voter_per_month.png"))

# heatmap
pivot_table_month = pivot_table_month.sort_index(ascending=False)
sns.set_style("darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(8, 5))
ax = sns.heatmap(pivot_table_month, annot=True, fmt="d")
ax.set_xlabel('Time (month)', fontsize=13)
ax.set_ylabel('Vote Allocation', fontsize=13)
ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
ax.set_yticklabels(ax.get_yticklabels(), rotation=0, ha="right")
plt.title('Spread of Vote Allocation Over Time (Table)', fontsize=14, weight='bold')
plt.tight_layout()
# save
plt.savefig(os.path.join(resultsPath_2, "n_prep_voted_per_voter_per_month_table.png"))

# Most popular by 1~2 prep voters (not unique per voter (multiple pairing) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# barplot
sns.set(style="darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(7, 5))
ax = sns.barplot(x="Category", y="pct", data=one_prep_voter_count_all.iloc[0:20], color="b")
ax.set_xlabel('P-Reps', fontsize=13)
ax.set_ylabel('Proportions (%)', fontsize=13)
ax.set_title('Most Popular P-Reps by those voting 1~2 P-Reps on Average \n' + n_one_prep_voter_all, fontsize=12, weight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.tight_layout()
#save
ax.figure.savefig(os.path.join(resultsPath_2, "popular_prep_by_1_2_voters.png"))

# stacked barplot
indexing_cols = one_prep_voter_count_all['Category'][0:6].tolist()
indexing_cols.append('Others')

df1 = one_prep_voter_count_month.pivot(index='month', columns='Category', values='pct').fillna(0)
df1 = df1.reindex(columns=indexing_cols)
index = pd.Index(sorted(one_prep_voter_count_month['month'].unique()), name='month')

sns.set(style="dark")
plt.style.use("dark_background")
ax1 = df1.plot(kind='bar', stacked=True, figsize=(8.5, 6.5))
plt.title('Voters delegating 1~2 P-Reps on Average Over Time \n (Top 6 P-Reps and Others)', fontsize=12, weight='bold')
ax1.set_xlabel('Time (month)')
ax1.set_ylabel('Proportions (%)')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=0, ha="center")
ax2 = ax1.twinx()
lines = plt.plot(n_one_prep_voter_month['month'],
               n_one_prep_voter_month['counts'])
ax2.set_ylabel('Voter Count')
plt.setp(lines, 'color', 'r', 'linewidth', 2.0)
ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15),
          fancybox=True, shadow=True, ncol=5)
color = 'tab:red'
red_line = mlines.Line2D([], [], color=color, label='Red line represents voter count', linewidth=3)
plt.legend(handles=[red_line], loc='upper left', fontsize='small', bbox_to_anchor=(0.67, -0.09), frameon=False)
plt.tight_layout(rect=[0,0,1,1])

plt.savefig(os.path.join(resultsPath_2, "popular_prep_by_1_2_voters_per_month.png"))

