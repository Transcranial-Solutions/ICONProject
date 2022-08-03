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

# Need to run it in sequence as it may need data from previous scripts (04 - 11)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
# import scipy.stats as sp
import seaborn as sns
import os


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#### plotting

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
plt.savefig(os.path.join(resultsPath, '13_vote_stagnancy_by_votes_over_time.png'))


