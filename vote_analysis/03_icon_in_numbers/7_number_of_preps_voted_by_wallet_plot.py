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

# Plotting number of P-Reps voted by wallet, and looking at votes (ICX) specifically

# This will require data from the previous script (6_number_of_wallets_by_vote_size_plots.py)
# and other variables -- mainly 'measuring_interval', 'term' and 'this_term' from the previous setting


# from urllib.request import Request, urlopen
# import json
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
# import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## number of P-Reps voted
prep_voted_count = df_longer.groupby(['delegator', measuring_interval, 'how_many_prep_voted']).agg('sum').reset_index()

# Votes and Voters
votes_and_prep_count = prep_voted_count.groupby([measuring_interval, 'how_many_prep_voted'])['cum_votes'].agg(['sum','count']).reset_index()
votes_and_prep_count = votes_and_prep_count.rename(columns={'sum': 'votes', 'count': 'voters'})

# Percentages across Week (Votes)
votes_and_prep_count['total_votes'] = votes_and_prep_count.groupby([measuring_interval])['votes'].transform('sum')
votes_and_prep_count['pct_votes'] = votes_and_prep_count['votes'] / votes_and_prep_count['total_votes']

# Percentages across Week (Voters)
votes_and_prep_count['total_voters'] = votes_and_prep_count.groupby([measuring_interval])['voters'].transform('sum')
votes_and_prep_count['pct_voters'] = votes_and_prep_count['voters'] / votes_and_prep_count['total_voters']

# votes_and_prep_count = votes_and_prep_count.drop(columns=['total_votes','total_voters'])

# Interested term and get difference between previous term & this term
votes_and_prep_count_term = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin(terms)].reset_index(drop=True)
votes_and_prep_count_this_term = votes_and_prep_count[votes_and_prep_count[measuring_interval].isin([this_term])].reset_index(drop=True)
votes_and_prep_count_term_diff = votes_and_prep_count_term.drop(columns=[measuring_interval]).groupby('how_many_prep_voted').diff().dropna().reset_index(drop=True)

# adding change symbols here for graph
change_symbol_votes = votes_and_prep_count_term_diff['pct_votes'].apply(lambda x: "+" if x>0 else '') # for voter count
change_symbol_voters = votes_and_prep_count_term_diff['pct_voters'].apply(lambda x: "+" if x>0 else '') # for voter count

# adding % symbol
votes_and_prep_count_term['pct_votes'] = votes_and_prep_count_term['pct_votes'].astype(float).map("{:.2%}".format)
votes_and_prep_count_term['pct_voters'] = votes_and_prep_count_term['pct_voters'].astype(float).map("{:.2%}".format)
votes_and_prep_count_this_term = votes_and_prep_count_term[votes_and_prep_count_term[measuring_interval].isin([this_term])].reset_index(drop=True)

# adding % symbol for diff
votes_and_prep_count_term_diff['pct_votes'] = votes_and_prep_count_term_diff['pct_votes'].astype(float).map("{:.2%}".format)
votes_and_prep_count_term_diff['pct_voters'] = votes_and_prep_count_term_diff['pct_voters'].astype(float).map("{:.2%}".format)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

## Votes
grand_total_votes_text = 'Total Votes: ' + '{:,}'.format(votes_and_prep_count_this_term['total_votes'][0].astype(int)) + ' ICX' \
                         + '\n' + 'Total Wallets: '+ '{:,}'.format(votes_and_prep_count_this_term['total_voters'][0])

total_votes_text = '(' + votes_and_prep_count_this_term['how_many_prep_voted'].astype(str) + ') '  \
                   + round(votes_and_prep_count_this_term['votes']).astype(int).apply('{:,}'.format)

# for plotting (legend)
label_text =  votes_and_prep_count_this_term['how_many_prep_voted'].astype(str) \
              + ' P-Rep(s) - ' \
              + votes_and_prep_count_this_term['pct_votes'] \
              + ' (' + change_symbol_votes + votes_and_prep_count_term_diff['pct_votes']  + ') '

# donut chart.... oh no..
plt.style.use(['dark_background'])
fig, ax = plt.subplots(figsize=(10, 6), subplot_kw=dict(aspect="equal"))
fig.patch.set_facecolor('black')

cmap = plt.get_cmap("Set3")
these_colors = cmap(np.array(range(len(votes_and_prep_count_this_term['votes']))))

my_circle=plt.Circle((0,0), 0.7, color='black')

wedges, texts = plt.pie(votes_and_prep_count_this_term['votes'],
                                   labels=total_votes_text,
                                   counterclock=False,
                                   startangle=90,
                                   colors=these_colors,
                                   textprops={'fontsize': 12, 'weight': 'bold'})
                                   #textprops={'color': "y"})

for text, color in zip(texts, inner_colors):
    text.set_color(color)

ax.text(0., 0., grand_total_votes_text,
        horizontalalignment='center',
        verticalalignment='center',
        linespacing = 2,
        fontsize=12,
        weight='bold')


plt.legend(wedges, label_text,
          title="Number of P-Reps Voted (ICX)",
          loc="lower left",
          bbox_to_anchor=(1, 0, 0.5, 1),
           fontsize=10)

ax.set_title('Current Vote Spreading '+ insert_week(this_term, 4) + '\n (# of P-Reps Voted per Wallet)', fontsize=14, weight='bold', y=1.08)

p=plt.gcf()
p.gca().add_artist(my_circle)
plt.axis('equal')
plt.show()
plt.tight_layout()

# saving
plt.savefig(os.path.join(resultsPath, measuring_interval + "_number_of_preps_voted_per_wallet.png"))