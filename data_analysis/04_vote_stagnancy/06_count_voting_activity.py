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


# Need to run it in sequence as it may need data from previous scripts (04 - 05)

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
plt.savefig(os.path.join(resultsPath, '02_Voting_Activity_By_Voter_Age.png'))



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
plt.savefig(os.path.join(resultsPath, '03_Voting_Activity_By_PRep_Age.png'))

