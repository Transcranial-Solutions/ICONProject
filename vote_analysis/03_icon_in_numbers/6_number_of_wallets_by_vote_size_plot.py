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

# Plotting number of wallets by vote size (binned allocation)

# This will require data from the previous script (5_votes_and_voters_change_plots.py)
# and other variables -- mainly 'measuring_interval', 'term' and 'this_term' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
# import os
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# total votes per voter by measuring_interval
total_term_change = df_longer.groupby(['delegator', measuring_interval])[['votes','cum_votes']].agg('sum').reset_index()

# making bins to extract group
def bin_votes(row):
    if row['cum_votes'] < 1:
        val = '< 1'
    if 1 <= row['cum_votes'] < 1000:
        val = '1-1000'
    elif 1000 <= row['cum_votes'] < 10000:
        val = '1000-10K'
    elif 10000 <= row['cum_votes'] < 100000:
        val = '10K-100K'
    elif 100000 <= row['cum_votes'] < 1000000:
        val = '100K-1M'
    elif 1000000 <= row['cum_votes']:
        val = '1M +'
    else:
        pass
        # val = -1
    return val


total_term_change['cum_votes_bin'] = total_term_change.apply(bin_votes, axis=1)

# binning data into categories
count_vote_bin = total_term_change[total_term_change['cum_votes'] != 0] # remove 0 balance here
count_vote_bin = count_vote_bin.drop(columns=(['votes', 'cum_votes']))
count_vote_bin = count_vote_bin.groupby(['cum_votes_bin', measuring_interval]).agg('count').reset_index()
count_vote_bin = count_vote_bin[count_vote_bin[measuring_interval].isin([this_term])].\
    drop(columns=[measuring_interval]).reset_index(drop=True)
count_vote_bin = count_vote_bin.set_index(list(count_vote_bin)[0])
count_vote_bin = count_vote_bin.reindex(['< 1', '1-1000', '1000-10K', '10K-100K', '100K-1M', '1M +'])
count_vote_bin = count_vote_bin.reset_index()

porcent = 100.*count_vote_bin['delegator']/count_vote_bin['delegator'].sum() # for plotting (legend)

# donut chart.... oh no..
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(8, 6), subplot_kw=dict(aspect="equal"))
fig.patch.set_facecolor('black')

cmap = plt.get_cmap("Set3")
inner_colors = cmap(np.array(range(len(count_vote_bin['delegator']))))

my_circle=plt.Circle((0,0), 0.7, color='black')
wedges, texts = plt.pie(count_vote_bin['delegator'],
                                   labels=count_vote_bin['delegator'],
                                   counterclock=False,
                                   startangle=90,
                                   colors=inner_colors,
                                   textprops={'fontsize': 14, 'weight': 'bold'})
                                   #textprops={'color': "y"})

for text, color in zip(texts, inner_colors):
    text.set_color(color)

labels = ['{0} ({1:1.2f} %)'.format(i,j) for i,j in zip(count_vote_bin['cum_votes_bin'], porcent)]

plt.legend(wedges, labels,
          title="Vote range (ICX)",
          loc="center left",
          bbox_to_anchor=(1, 0, 0.5, 1),
           fontsize=10)

ax.set_title('Number of Wallets by Vote Size ('+ insert_week(this_term, 4) +')', fontsize=14, weight='bold', y=1.08)

p=plt.gcf()
p.gca().add_artist(my_circle)
plt.axis('equal')
plt.show()
plt.tight_layout()
