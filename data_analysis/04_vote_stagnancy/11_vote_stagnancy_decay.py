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

# Need to run it in sequence as it may need data from previous scripts (04 - 10)

import pandas as pd
# import numpy as np
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Historical vote stagnancy (decay) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

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
plt.savefig(os.path.join(resultsPath, '12_vote_stagnancy_by_votes_going_backward.png'))
