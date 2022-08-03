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

# Need to run it in sequence as it may need data from previous scripts (04 - 08)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import matplotlib.ticker as ticker
import scipy.stats as sp
import seaborn as sns
import os


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Plotting ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#### plotting

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ correlation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

def corr_plot(df, inVar1, inVar2, this_xlabel, this_ylabel, this_title):

    m_var1 = df.groupby('validator_name')[[inVar1]].apply(np.median).reset_index(name='var1')
    m_var2 = df.groupby('validator_name')[[inVar2]].apply(np.median).reset_index(name='var2')
    m_days_ranking = pd.merge(m_var1, m_var2, on='validator_name', how='left')

    # plot
    sns.set(style="ticks")
    plt.style.use("dark_background")
    g = sns.jointplot("var1", "var2", data=m_days_ranking,
                      kind="reg", height=6, truncate=False, color='m',
                      joint_kws={'scatter_kws': dict(alpha=0.5)}
                      ).annotate(sp.pearsonr)
    g.set_axis_labels(this_xlabel, this_ylabel, fontsize=14, weight='bold', labelpad=10)
    g = g.ax_marg_x
    g.set_title(this_title, fontsize=12, weight='bold')
    plt.tight_layout()

# days unchanged vs P-Rep ranking
corr_plot(agg_df_max_pure,
          'days_pure',
          'prep_ranking',
          'Days unchanged (median)',
          'P-Rep Ranking (median)',
          'Days of Votes Untouched vs P-Rep Ranking')

# saving
plt.savefig(os.path.join(resultsPath, '08_days_votes_untouched_vs_PRep_ranking_corr.png'))

# days of same P-Rep vs P-Rep ranking
corr_plot(agg_df_max_prep,
          'days_same_prep',
          'prep_ranking',
          'Days in same P-Rep (median)',
          'P-Rep Ranking (median)',
          'Days of Votes Unmoved vs P-Rep Ranking')

# saving
plt.savefig(os.path.join(resultsPath, '09_days_votes_unmoved_vs_PRep_ranking_corr.png'))
