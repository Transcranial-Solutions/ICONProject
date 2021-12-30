#########################################################################
## Project: Transaction Data Analysis                                  ##
## Date: December 2021                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################


# webscraping icon.community and save into csv

import pandas as pd
import os
import glob
import re
import seaborn as sns
import matplotlib.pyplot as plt  # for improving our visualizations
import matplotlib.ticker as ticker
import matplotlib.lines as mlines


start_date = '2021-12-01'
end_date = '2021-12-31'
my_title = 'Transactions (December 2021)'
interval = 'date'


# path
currPath = os.getcwd()
if not "08_transaction_data" in currPath:
    projectPath = os.path.join(currPath, "08_transaction_data")
    if not os.path.exists(projectPath):
        os.mkdir(projectPath)
else:
    projectPath = currPath

dataPath = os.path.join(projectPath, "data")
if not os.path.exists(dataPath):
    os.mkdir(dataPath)

resultPath = os.path.join(projectPath, "results")
if not os.path.exists(resultPath):
    os.mkdir(resultPath)

walletPath = os.path.join(currPath, "wallet")
if not os.path.exists(walletPath):
    os.mkdir(walletPath)



listData = glob.glob(os.path.join(resultPath, "tx_summary_*.csv"))


all_df =[]
for k in range(len(listData)):
    this_date = re.findall(r'tx_summary_(.+?).csv', listData[k])[0].replace("_", "/")
    df = pd.read_csv(listData[k])
    df['date'] = this_date
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    all_df.append(df)

df = pd.concat(all_df)

df.to_csv(os.path.join(resultPath, 'compiled_tx_summary.csv'), index=False)

df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
df['week'] = pd.to_datetime(df['date'], unit='s').dt.strftime("%Y-%U")
df['month'] = pd.to_datetime(df['date'], unit='s').dt.strftime("%Y-%m")
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

sum_df = df.groupby(interval).agg('sum')
use_df = sum_df[(sum_df.index >= start_date) & (sum_df.index <= end_date)]

plot_df = use_df.drop(columns='Fees burned')

to_group = use_df.agg('sum')
all_tx = 'Total Transactions: ' + '{:,}'.format(
    to_group['Regular Tx'].astype(int) + to_group['Internal Tx'].astype(int)) + '\n' + \
         'Total Events (including Tx): ' + '{:,}'.format(
    to_group['Regular Tx'].astype(int) + to_group['Internal Tx'].astype(int) + to_group[
        'Internal Event (excluding Tx)'].astype(int))
totals = to_group.astype(int).map("{:,}".format)
fees_burned = 'Fees burned' + ' (' + totals['Fees burned'] + ' ICX)'

# stacked barplot
sns.set(style="dark")
plt.style.use("dark_background")
ax1 = plot_df.plot(kind='bar', stacked=True, figsize=(16, 8))
plt.title(my_title, fontsize=14, weight='bold', pad=10, loc='left')
ax1.set_xlabel('Time', labelpad=10, weight='bold')
ax1.set_ylabel('Transactions', labelpad=10, weight='bold')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=90, ha="center")
ax2 = ax1.twinx()
lines = plt.plot(use_df['Fees burned'], marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black', markersize=8)

xmin, xmax = ax1.get_xlim()
ymin, ymax = ax1.get_ylim()
ymin2, ymax2 = ax2.get_ylim()

ymax_set = ymax*1.1
ax1.set_ylim([ymin,ymax_set])


if ymax >= 1000:
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
if ymax >= 1000000:
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))

if ymax2 >= 1000:
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x / 1e3) + ' K'))
if ymax2 >= 1000000:
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x / 1e6) + ' M'))


ax2.set_ylabel('Fees burned (ICX)', labelpad=10)
plt.setp(lines, 'color', 'white', 'linewidth', 1.0)
# ax1.legend(loc='upper center', bbox_to_anchor=(0.4, 0.95),
#           fancybox=True, shadow=True, ncol=5)
color = 'white'
m_line = mlines.Line2D([], [], color=color, label='Total ' + fees_burned, linewidth=1, marker='h', linestyle='dotted', mfc='mediumturquoise', mec='black')
leg = plt.legend(handles=[m_line], loc='upper right', fontsize='medium', bbox_to_anchor=(1, 1.20), frameon=False)
for text in leg.get_texts():
    plt.setp(text, color='cyan')

ax1.text(xmax*0.99, ymax*1.21, all_tx,
        horizontalalignment='right',
        verticalalignment='center',
        linespacing = 1.5,
        fontsize=12,
        weight='bold')

handles, labels = ax1.get_legend_handles_labels()
ax1.legend(handles, labels,
           loc='upper right', bbox_to_anchor=(1, 1.07),
           frameon=False, fancybox=True, shadow=True, ncol=3)

# ax1.text(xmax*1.00, ymax*-0.40, system_tx,
#         horizontalalignment='right',
#         verticalalignment='center',
#         linespacing = 1.5,
#         fontsize=8)

ax2.spines['right'].set_color('cyan')
ax2.yaxis.label.set_color('cyan')
ax2.tick_params(axis='y', colors="cyan")


plt.tight_layout(rect=[0,0,1,1])



