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

# Vote distribution / counting / plotting

import pandas as pd
import scipy.stats as sp
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
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
resultsPath = os.path.join(inPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)
resultsPath_2 = os.path.join(resultsPath, "part_1")
if not os.path.exists(resultsPath_2):
    os.mkdir(resultsPath_2)

# read data
prep_df = pd.read_csv(os.path.join(inPath, 'final_prep_details.csv'))
votes_cumsum_longer = pd.read_csv(os.path.join(inPath, 'vote_status_per_day.csv'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Counting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
### For counting
# removing 'unvoted' for counting
count_voted_prep_per_day = votes_cumsum_longer[votes_cumsum_longer['vote_status'] != 'unvoted']

count_voted_prep_per_day = count_voted_prep_per_day.groupby(['delegator', 'date']).size().reset_index(name='counts')
count_voted_prep_per_day.sort_values(by=['counts'])

count_voted_prep_per_day['month'] = pd.to_datetime(count_voted_prep_per_day['date']).dt.strftime("%Y-%m")
count_voted_prep_per_day = count_voted_prep_per_day.sort_values(['date'])


#----------------------------------------------------------------------------------------------------------------#
# getting unique voters per day/week/month (wallet - including unchanged votes)
unique_voters_per_day = votes_cumsum_longer.copy()
unique_voters_per_day['week'] = pd.to_datetime(unique_voters_per_day['date']).dt.strftime("%Y-%m-%U")
unique_voters_per_day['month'] = pd.to_datetime(unique_voters_per_day['date']).dt.strftime("%Y-%m")

unique_voters_per_day = votes_cumsum_longer.groupby(['date']).delegator.nunique().reset_index()
unique_voters_per_week = votes_cumsum_longer.groupby(['week']).delegator.nunique().reset_index()
unique_voters_per_month = votes_cumsum_longer.groupby(['month']).delegator.nunique().reset_index()

# total votes per day & month
total_votes_per_day = votes_cumsum_longer.groupby(['date']).sum().reset_index()
total_votes_per_day['month'] = pd.to_datetime(total_votes_per_day['date']).dt.strftime("%Y-%m")
total_votes_per_day['week'] = pd.to_datetime(total_votes_per_day['date']).dt.strftime("%Y-%m-%U")

total_votes_per_week = total_votes_per_day.groupby(['week']).last().reset_index()
total_votes_per_month = total_votes_per_day.groupby(['month']).last().reset_index()

# merged
votes_and_voters_per_day = pd.merge(total_votes_per_day, unique_voters_per_day, how='inner', on='date')
votes_and_voters_per_week = pd.merge(total_votes_per_week, unique_voters_per_week, how='inner', on='week')
votes_and_voters_per_month = pd.merge(total_votes_per_month, unique_voters_per_month, how='inner', on='month')


#~~~~~~~~~~~~~~~~~~~~~~ Current number of Votes, Voters and Adjusted (outlier removed) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# get current votes
last_date = sorted(votes_cumsum_longer.date.unique())[-1]
unique_preps = pd.DataFrame(votes_cumsum_longer.validator_name.unique(), columns=['validator_name'])
current_votes = votes_cumsum_longer[votes_cumsum_longer.date == last_date]

# adding zscore for cummulative votes within group (p-reps)
zscore = lambda x: (x - x.mean()) / x.std()
current_votes.insert(5, 'z_score', current_votes.groupby(['validator_name'])['cum_votes'].transform(zscore))

# p-rep total votes
current_votes_per_prep = current_votes.groupby(['validator_name'])['cum_votes'].sum().reset_index()
current_votes_per_prep = current_votes_per_prep.sort_values(by=['cum_votes'], ascending=False).reset_index(drop=True)

# p-rep votes outliers and without outliers
vote_outlier = current_votes.loc[current_votes['z_score'].abs()>3]
vote_outlier_removed = current_votes.loc[current_votes['z_score'].abs()<=3]
# reindexing based on original order
current_votes_per_prep_outlier_removed = vote_outlier_removed.groupby(['validator_name'])['cum_votes'].sum().reset_index()
current_votes_per_prep_outlier_removed = current_votes_per_prep_outlier_removed.set_index('validator_name')
current_votes_per_prep_outlier_removed = current_votes_per_prep_outlier_removed.reindex(index=current_votes_per_prep['validator_name']).reset_index()

# no of wallets per p-rep
current_voters_per_prep = current_votes[current_votes['vote_status'] != 'unvoted']
current_voters_per_prep = current_voters_per_prep.groupby(['validator_name']).agg(n_voters = ('delegator', 'count')).reset_index()
current_voters_per_prep = pd.merge(unique_preps, current_voters_per_prep, how='left', on='validator_name')
current_voters_per_prep.n_voters = current_voters_per_prep.n_voters.fillna(0).astype(int)
current_voters_per_prep = current_voters_per_prep.sort_values(by=['n_voters'], ascending=False).reset_index(drop=True)

# no of wallets per p-rep (outlier removed)
current_voters_per_prep_outlier_removed = vote_outlier_removed[vote_outlier_removed['vote_status'] != 'unvoted']
current_voters_per_prep_outlier_removed = current_voters_per_prep_outlier_removed.groupby(['validator_name']).agg(n_voters=('delegator', 'count')).reset_index()
current_voters_per_prep_outlier_removed = pd.merge(unique_preps, current_voters_per_prep_outlier_removed, how='left', on='validator_name')
current_voters_per_prep_outlier_removed.n_voters = current_voters_per_prep_outlier_removed.n_voters.fillna(0).astype(int)
current_voters_per_prep_outlier_removed = current_voters_per_prep_outlier_removed.sort_values(by=['n_voters'], ascending=False).reset_index(drop=True)

# median & mean votes by p-rep (original and outlier removed)
mean_median_votes_per_prep = current_votes.groupby(['validator_name'])['cum_votes'].agg(['mean','median']).reset_index()
mean_median_votes_per_prep_outlier_removed = vote_outlier_removed.groupby(['validator_name'])['cum_votes'].agg(['mean','median']).reset_index()

# Merging datasets
votes_and_voters_per_prep = pd.merge(current_votes_per_prep, current_voters_per_prep,  how='left', on=['validator_name'])
votes_and_voters_per_prep = pd.merge(votes_and_voters_per_prep, mean_median_votes_per_prep,  how='left', on=['validator_name'])
votes_and_voters_per_prep = pd.merge(votes_and_voters_per_prep, unique_preps,  how='right', on=['validator_name']).fillna(0)

# outlier removed
votes_and_voters_per_prep_outlier_removed = pd.merge(current_votes_per_prep_outlier_removed, current_voters_per_prep_outlier_removed,  how='left', on=['validator_name'])
votes_and_voters_per_prep_outlier_removed = pd.merge(votes_and_voters_per_prep_outlier_removed, mean_median_votes_per_prep_outlier_removed,  how='left', on=['validator_name'])
votes_and_voters_per_prep_outlier_removed = pd.merge(votes_and_voters_per_prep_outlier_removed, unique_preps,  how='right', on=['validator_name']).fillna(0)





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Current Votes ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Current Votes by P-Reps ~~~~~~~~~~~~~~~~~~~~~~~#
sns.set(style="whitegrid")
plt.style.use("dark_background")
ax1 = plt.subplots(figsize=(10, 12))
ax1 = sns.barplot(x="cum_votes", y="validator_name", data=votes_and_voters_per_prep,
                  edgecolor="white")
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000000) + ' M'))
color = 'tab:red'
ax2 = sns.barplot(x='cum_votes', y='validator_name',
                  data=votes_and_voters_per_prep_outlier_removed,
                  color=color,
                  edgecolor="black")
ax2.tick_params(axis='y', color=color)
ax2.set_xlabel('Votes', fontsize=13)
ax2.set_ylabel('P-Reps', fontsize=13)
ax2.set_title('Total Votes in each P-Rep', fontsize=14, weight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=5.5)
red_line = mlines.Line2D([], [], color=color, label='Red bars represent data without outliers', linewidth=3)
plt.legend(handles=[red_line], loc='lower right', fontsize='medium')
plt.tight_layout()

# save figure
ax2.figure.savefig(os.path.join(resultsPath_2, "total_n_votes_by_prep.png"))


# Current Number of Voters by P-Reps ~~~~~~~~~~~~~~~~~~~~~~~#
sns.set(style="whitegrid")
plt.style.use("dark_background")
ax1 = plt.subplots(figsize=(9, 12))
ax1 = sns.barplot(x="n_voters", y="validator_name", data=votes_and_voters_per_prep, edgecolor="white")
color = 'tab:red'
ax2 = sns.barplot(x='n_voters', y='validator_name',
                  data=votes_and_voters_per_prep_outlier_removed, color=color, edgecolor="black")
ax2.tick_params(axis='y', color=color)
ax2.set_xlabel('Voters', fontsize=13)
ax2.set_ylabel('P-Reps', fontsize=13)
ax2.set_title('Total Number of Voters by P-Rep \n(in total vote ranking order)', fontsize=14, weight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=5.5)
red_line = mlines.Line2D([], [], color=color, label='Red bars represent data without outliers', linewidth=3)
plt.legend(handles=[red_line], loc='lower right', fontsize='medium')
plt.tight_layout()

# save figure
ax2.figure.savefig(os.path.join(resultsPath_2, "total_n_voters_by_prep.png"))


# Average Number of Votes per Voter by P-Reps ~~~~~~~~~~~~~~~~~~~~~~~#
sns.set(style="whitegrid")
plt.style.use("dark_background")
ax1 = plt.subplots(figsize=(9, 12))
ax1 = sns.barplot(x="mean", y="validator_name", data=votes_and_voters_per_prep, edgecolor="white")
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000) + ' K'))
color = 'tab:red'
ax2 = sns.barplot(x='mean', y='validator_name',
                  data=votes_and_voters_per_prep_outlier_removed, color=color, edgecolor="black")
ax2.tick_params(axis='y', color=color)
ax2.set_xlabel('Votes', fontsize=13)
ax2.set_ylabel('P-Reps', fontsize=13)
ax2.set_title('Average Votes per Voter in each P-Rep \n(in total vote ranking order)', fontsize=14, weight='bold')
plt.xticks(fontsize=12)
plt.yticks(fontsize=5.5)
red_line = mlines.Line2D([], [], color=color, label='Red bars represent data without outliers', linewidth=3)
plt.legend(handles=[red_line], loc='lower right', fontsize='medium')
plt.tight_layout()

# save figure
ax2.figure.savefig(os.path.join(resultsPath_2, "mean_n_votes_per_voter_by_prep.png"))

# Median Votes by P-Reps ~~~~~~~~~~~~~~~~~~~~~~~#
# sns.set(style="whitegrid")
# ax1 = plt.subplots(figsize=(10,9))
# ax1 = sns.barplot(x="median", y="validator_name", data=votes_and_voters_per_prep)
# ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000) + ' K'))
# color = 'tab:red'
# ax2 = sns.barplot(x='median', y='validator_name', data = votes_and_voters_per_prep_outlier_removed, color=color)
# ax2.tick_params(axis='y', color=color)
# ax2.set_xlabel('Votes',fontsize=13)
# ax2.set_ylabel('P-Reps',fontsize=13)
# plt.xticks(fontsize=12)
# plt.yticks(fontsize=5.5)
# plt.tight_layout()


# Count P-Reps by Country ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
df = pd.merge(votes_and_voters_per_prep, prep_df[['name', 'alpha-3', 'city', 'country', 'sub-region']],
              how='left', left_on='validator_name', right_on='name')
df = df[['cum_votes', 'alpha-3', 'country']]
df = df.groupby(['country', 'alpha-3'])['cum_votes'].agg(['sum', 'count']).reset_index()
df = df.sort_values(by="count", ascending=False)


# Number of P-Reps in given country
sns.set(style="darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(8, 6))
ax = sns.barplot(x="count", y="country", data=df, color="b")
ax.set_xlabel('P-Reps (count)', fontsize=10)
ax.set_ylabel('Countries', fontsize=10)
ax.set_title('Number of P-Reps in Various Countries', fontsize=12, weight='bold')
plt.xticks(fontsize=10)
plt.yticks(fontsize=8)
plt.tight_layout()
# save figure
ax.figure.savefig(os.path.join(resultsPath_2, "prep_count_per_country.png"))


# World Map ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"

fig = go.Figure(data=go.Choropleth(locations=df['alpha-3'],
    z=df['sum'], text=df['country'],
    colorscale='Portland', autocolorscale=False, reversescale=False,
    marker_line_color='white', marker_line_width=0.5,
    colorbar_title='Number of votes',
),
layout=go.Layout(geo=dict(bgcolor='rgba(0,0,0,0)', lakecolor='#4E5D6C',
                            landcolor='rgba(51,17,0,0.2)', subunitcolor='grey'),
                   paper_bgcolor='#4E5D6C', plot_bgcolor='#4E5D6C'))

fig.update_layout(autosize=False, width=1200, height=800,
    title_text='<b>Vote Distribution by Country</b> ',
    font=dict(color="white", size=16),
    geo=dict(showframe=False, showcoastlines=False,
        projection_type='equirectangular'),
    annotations=[dict(x=0.55, y=0.1, xref='paper', yref='paper',
        text='2020 May P-Rep Self-Reported Residing Country',
        showarrow=False,
        font=dict(color="white", size=20))]
)
fig.show()

# save figure
fig.write_image(os.path.join(resultsPath_2, "total_n_voters_map.png"))



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voter Count Over Time ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# barplot (voter count over time - month)
sns.set(style="darkgrid")
plt.style.use("dark_background")
ax = plt.subplots(figsize=(7, 5))
ax = sns.barplot(x="month", y="delegator", data=votes_and_voters_per_month, color="b")
ax.set_xlabel('Time (month)', fontsize=13)
ax.set_ylabel('Voters', fontsize=13)
ax.set_title('Number of Voters Over Time', fontsize=14, weight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.tight_layout()
#save
ax.figure.savefig(os.path.join(resultsPath_2, "n_voters_over_time.png"))


# joint (voters vs votes correlation - weeks)
sns.set(style="darkgrid")
plt.style.use("dark_background")
g = sns.jointplot("cum_votes", "delegator", data=votes_and_voters_per_week,
                  kind="reg", height=6, joint_kws={'scatter_kws': dict(alpha=0.5)}
                  ).annotate(sp.pearsonr)
g.set_axis_labels("Votes", "Voters")
g = g.ax_marg_x
g.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000000) + ' M'))
g.set_title('Votes vs Voters', fontsize=12, weight='bold')
plt.tight_layout()
# save
g.figure.savefig(os.path.join(resultsPath_2, "votes_vs_voters_corr.png"))


