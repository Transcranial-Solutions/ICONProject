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

# Plotting vote stagnancy, and looking at votes (ICX) specifically

# It was derived by counting the number of wallets that changed votes (up/down) and those did not each week.

# This will require data from the previous script (7_number_of_preps_voted_by_wallet_plot.py)
# and other variables -- mainly 'measuring_interval', 'term' and 'this_term' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
# import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Vote Stagnancy -- just by having voted or not for that week

vote_stagnancy_count = df_longer.groupby(['delegator', measuring_interval]).agg('sum').reset_index()
vote_stagnancy_count['Stagnant'] = np.where(vote_stagnancy_count['votes'] == 0, 'No Vote Change', 'Vote Change (Up/Down)')

vote_stagnancy = vote_stagnancy_count.groupby([measuring_interval, 'Stagnant'])['cum_votes'].agg(['sum', 'count']).reset_index()
vote_stagnancy = vote_stagnancy.rename(columns = {'sum': 'votes', 'count': 'voters'})

# vote stagnancy % by votes
vote_stagnancy['total_votes'] = vote_stagnancy.groupby(measuring_interval)['votes'].transform('sum')
vote_stagnancy['pct_votes'] = vote_stagnancy['votes'] / vote_stagnancy['total_votes']

# vote stagnancy % by voters
vote_stagnancy['total_voters'] = vote_stagnancy.groupby(measuring_interval)['voters'].transform('sum')
vote_stagnancy['pct_voters'] = vote_stagnancy['voters'] / vote_stagnancy['total_voters']



# vote_stagnancy['votes'] = vote_stagnancy['votes'].astype(int)

# currPath = os.getcwd()
# vote_stagnancy.to_csv(os.path.join(currPath, 'test.csv'), index=False)


# sns.set(style="ticks")
# plt.style.use(['dark_background'])
# f, ax = plt.subplots(figsize=(12, 8))
# sns.lineplot(x=measuring_interval, y='voters', hue="Stagnant", data=vote_stagnancy)
# plt.tight_layout()
# ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
# plt.tight_layout()



sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x=measuring_interval, y='votes', hue="Stagnant", data=vote_stagnancy, palette=sns.color_palette('husl', n_colors=2))
h,l = ax.get_legend_handles_labels()

ax.set_xlabel('Week', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Votes (ICX)', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Vote Stagnancy \n (based on active voting wallets per week)', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/10e6) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
sns.despine(offset=10, trim=True)

xs = vote_stagnancy[measuring_interval]
ys = vote_stagnancy['votes']

porcent = vote_stagnancy.copy()
porcent['pct_votes'] = porcent['pct_votes'].apply('{:.0%}'.format)
porcent.loc[porcent['Stagnant'] == 'Vote Change (Up/Down)', 'pct_votes'] = ''
porcent = porcent['pct_votes']

# zip joins x and y coordinates in pairs
for x,y,z in zip(xs,ys,porcent):

    plt.annotate(z, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,10), # distance from text to points (x,y)
                 ha='center', # horizontal alignment can be left, right or center
                 fontsize=8,
                 color='w',
                 weight='bold')

plt.tight_layout()
ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
ax.legend(h[1:],l[1:],ncol=1,
          # title="Voting Activity Stagnation",
          fontsize=10,
          loc='upper left')
n = 2  # Keeps every n-th label
[l.set_visible(False) for (i,l) in enumerate(ax.xaxis.get_ticklabels()) if i % n != 0]
plt.tight_layout()
