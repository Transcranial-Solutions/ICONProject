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

# Need to run it in sequence as it needs data from previous script (04_aggregate_data.py)

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
# number of voters by voter age

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
plt.savefig(os.path.join(resultsPath, '01_wallet_count_by_voter_age.png'))

