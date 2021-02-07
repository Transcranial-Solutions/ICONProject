#########################################################################
## Project: ICON in Numbers                                            ##
## Date: December 2020                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# This is for 'ICON in Numbers' weekly series.
# It is an automated figure generator based on the time of your choosing (mainly tailored for weekly).
# Terms will require the timeframe to have finished (e.g. this may not work as intended if week 25 is still on-going).
# This will webscrape iconvotemonitor.com by Everstake and also P-Rep information from ICON Foundation site.
# It will then do data manipulation, recoding, calculation, aggregation and generate multiple figures.
# Please note that depending on the amount of vote change, the scale may be off, and needs to be manually modified (ylim mostly).


# import json library
from urllib.request import Request, urlopen
import json
import pandas as pd
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = os.getcwd()
outPath = os.path.join(currPath, "03_icon_in_numbers")
if not os.path.exists(outPath):
    os.mkdir(outPath)
resultsPath = os.path.join(outPath, "results")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)
resultsPath = os.path.join(resultsPath, "daily")
if not os.path.exists(resultsPath):
    os.mkdir(resultsPath)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

measuring_interval = 'date' # // 'year' // 'month' // 'week' // "date" // "day"//
terms = ['2021-02-07', '2021-02-06']
# weeks = ['2020-24', '2020-23']
# months = ['2020-05', '2020-06']
# years = ['2020']

this_term = terms[0]
last_term = terms[1]
# this_week = weeks[0]
# this_month = months[0]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
resultsPath_interval = os.path.join(resultsPath, this_term)
if not os.path.exists(resultsPath_interval):
    os.mkdir(resultsPath_interval)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

## value extraction function
def extract_values(obj, key):
  # Pull all values of specified key from nested JSON
  arr = []

  def extract(obj, arr, key):

    # Recursively search for values of key in JSON tree
    if isinstance(obj, dict):
      for k, v in obj.items():
        if isinstance(v, (dict, list)):
          extract(v, arr, key)
        elif k == key:
          arr.append(v)
    elif isinstance(obj, list):
      for item in obj:
        extract(item, arr, key)
    return arr

  results = extract(obj, arr, key)
  return results


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ P-Rep Info ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# getting p-rep information from icon governance page
prep_list_url_reg = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000', headers={'User-Agent': 'Mozilla/5.0'})
prep_list_url_unreg = Request('https://tracker.icon.foundation/v3/iiss/prep/list?count=1000&grade=3', headers={'User-Agent': 'Mozilla/5.0'})

# json format
jprep_list_url_reg = json.load(urlopen(prep_list_url_reg))
jprep_list_url_unreg = json.load(urlopen(prep_list_url_unreg))

def extract_json(json_dict, reg_status):
    # extracting p-rep information by labels
    prep_address = extract_values(json_dict, 'address')
    prep_name = extract_values(json_dict, 'name')
    prep_country = extract_values(json_dict, 'country')
    prep_city = extract_values(json_dict, 'city')
    prep_logo = extract_values(json_dict, 'logo')

    # combining strings into list
    prep_d = {'address': prep_address,
         'name': prep_name,
         'country': prep_country,
         'city': prep_city,
         'logo': prep_logo}

    # convert into dataframe
    df = pd.DataFrame(data=prep_d)
    df['status'] = reg_status
    return(df)

prep_df = []
prep_df_reg = extract_json(jprep_list_url_reg, 'registered')
prep_df_unreg = extract_json(jprep_list_url_unreg, 'unregistered')
prep_df = pd.concat([prep_df_reg, prep_df_unreg]).reset_index(drop=True)

prep_address = prep_df['address']
len_prep_address = len(prep_address)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Extraction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

count = 0
def get_votes(prep_address, len_prep_address):
    global count

    # Request url, no limit here
    req = Request('https://api.iconvotemonitor.com/delegations?validators=' + prep_address, headers={'User-Agent': 'Mozilla/5.0'})

    # req = Request('https://api.iconvotemonitor.com/delegations?validators='+ prep_address[k] + '&limit=' + str(max_extract_count),
    #               headers={'User-Agent': 'Mozilla/5.0'})

    jreq = json.load(urlopen(req))

    # extracting data by labels
    # block_id = extract_values(jreq, 'block_id')
    delegator = extract_values(jreq, 'delegator')
    # validator = extract_values(jreq, 'validator')
    votes = extract_values(jreq, 'amount')
    created_at = extract_values(jreq, 'created_at')
    validator_name = extract_values(jreq, 'validator_name')

    # combining strings into list
    d = {# 'block_id': block_id,
         'delegator': delegator,
         # 'validator': validator,
         'validator_name': validator_name,
         'votes': votes,
         'created_at': created_at}

    # convert into dataframe
    df = pd.DataFrame(data=d)

    # convert timestamp into Year & measuring_interval (Week/Month), and summarise data by year + measuring_interval
    # df['datetime'] = pd.to_datetime(df['created_at'], unit = 's').dt.strftime("%Y-%m-%d %H:%M:%S")
    df['year'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y")
    df['month'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m")
    df['week'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%U")
    df['date'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%m-%d")
    df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%Y-%U-%a")

    # if measuring_interval == 'week':
    #     df[measuring_interval] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%U")
    # elif measuring_interval == 'month':
    #     df[measuring_interval] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%m")

    # df['day'] = pd.to_datetime(df['created_at'], unit='s').dt.strftime("%a")
    df.drop(columns=['created_at'], inplace=True)

    df['votes'] = pd.to_numeric(df['votes'])

    df = df.groupby(['validator_name', 'delegator', 'year', 'month', 'week', 'date', 'day']).agg('sum').reset_index()
    # df = df.groupby(['validator_name', 'delegator', 'year', measuring_interval]).agg('sum').reset_index()

    try:
       print("Votes for " + validator_name[0] + ": Done - " + str(count+1) + " out of " + str(len_prep_address))
    except:
       print("An exception occurred - Possibly a new P-Rep without votes")

    count += 1

    return(df)

# threading
start = time()

all_votes = []
with ThreadPoolExecutor(max_workers=5) as executor:
    for k in range(len(prep_address)):
        all_votes.append(executor.submit(get_votes, prep_address[k], len_prep_address))

temp_df = []
for task in as_completed(all_votes):
    temp_df.append(task.result())

print(f'Time taken: {time() - start}')

# all votes per wallet
df = pd.concat(temp_df)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Voting Info Data -- by validator ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# concatenate them into a dataframe -- by validator_name
unique_date = df.drop_duplicates(['year', 'month', 'week', 'date', 'day'])[['year', 'month', 'week', 'date', 'day']].sort_values('date')

# get unique interval
unique_interval = df.drop_duplicates(measuring_interval)[[measuring_interval]].sort_values(measuring_interval).reset_index(drop=True)

df = df.groupby(['validator_name', 'delegator', measuring_interval]).agg('sum').reset_index()
df = df.sort_values(by=['validator_name', 'delegator', measuring_interval]).reset_index(drop=True)


# pivot wider & longer to get all the longitudinal data
df_wider = df.pivot_table(index=['validator_name', 'delegator'],
                          columns=[measuring_interval],
                          values='votes').reset_index()

df_longer = df_wider.melt(id_vars=['validator_name', 'delegator'], var_name=[measuring_interval], value_name='votes')
df_longer = df_longer.sort_values(by=['validator_name', 'delegator', measuring_interval, 'votes']).reset_index(drop=True)

df = []
df_wider = []

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# removing NaN to first non-NaN
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])[measuring_interval].cumcount()==0,'remove_this']= '1'
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])['votes'].apply(pd.Series.first_valid_index), 'remove_this'] = '2'
df_longer['remove_this'] = df_longer['remove_this'].ffill()
df_longer = df_longer[df_longer['remove_this'] != '1'].drop(columns='remove_this').reset_index(drop=True)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# getting the duration of interest (so that the data does not get cut off)
df_longer = df_longer[df_longer[measuring_interval] <= this_term]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# vote status -> voted, add cumulative votes & unvoted
df_longer['cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['votes'].cumsum()
df_longer['cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['cum_votes'].ffill()

# cumulative votes shifting to give proper vote/unvote status
df_longer['prev_cum_votes'] = df_longer.groupby(['delegator', 'validator_name'])['cum_votes'].shift()

# fill cumulative votes, make between -1e-6 and 1e-6 to zero
df_longer.loc[df_longer['votes'].between(-1e-6, 1e-6), 'votes'] = 0
df_longer.loc[df_longer['prev_cum_votes'].between(-1e-6, 1e-6), 'prev_cum_votes'] = 0
df_longer.loc[df_longer['cum_votes'].between(-1e-6, 1e-6), 'cum_votes'] = 0

# vote/unvote status
df_longer.loc[df_longer.groupby(['validator_name', 'delegator'])[measuring_interval].cumcount()==0,'vote_status_A']= 'voted'
df_longer.loc[df_longer['prev_cum_votes'].between(-1e-6, 1e-6) & ~np.isnan(df_longer['votes']), 'vote_status_A']= 'voted'
df_longer.loc[df_longer['cum_votes'].between(-1e-6, 1e-6) & ~np.isnan(df_longer['votes']), 'vote_status_B'] = 'unvoted'

# getting rid of non-needed rows (rows before first vote & after unvote)
remove_these = df_longer['cum_votes'].between(-1e-6, 1e-6) & df_longer['prev_cum_votes'].between(-1e-6, 1e-6)
df_longer = df_longer[~remove_these].drop(columns='prev_cum_votes')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# how many p-rep voted
## removing 'unvoted' for counting number of p-reps each voter voted per measuring_interval
a = df_longer['vote_status_B'] != 'unvoted'
count_voted_prep_per_measuring_interval = df_longer.copy()
count_voted_prep_per_measuring_interval['vote_status_A'] = count_voted_prep_per_measuring_interval.\
    groupby(['delegator'])['vote_status_A'].ffill()
count_voted_prep_per_measuring_interval = count_voted_prep_per_measuring_interval[a].\
    groupby(['delegator', measuring_interval]).\
    count()['vote_status_A'].reset_index().\
    rename(columns={'vote_status_A': 'how_many_prep_voted'})


# merge with df_longer
df_longer = pd.merge(df_longer,
                     count_voted_prep_per_measuring_interval,
                     on=['delegator', measuring_interval],
                     how='left')

count_voted_prep_per_measuring_interval = []

# just to have the number without decimals
def remove_decimal_with_int(df, inVar):
    df[inVar] = df[inVar].fillna(0).astype(int).astype(object).where(df[inVar].notnull())

# list to convert
lst = ['how_many_prep_voted']
for x in lst:
    remove_decimal_with_int(df_longer, x)

combined_df = df_longer[['validator_name', 'delegator', measuring_interval, 'how_many_prep_voted']]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Table for Count ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# for combined
term_change_comb = combined_df[combined_df[measuring_interval].isin(terms)]
this_term_change_comb = combined_df[combined_df[measuring_interval].isin([this_term])]



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## votes change this week

def insert_week(string, index):
    return string[:index] + ' date' + string[index:]


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
## number of P-Reps voted

# making bins to extract group
def bin_NumPReps(df):
    if df['how_many_prep_voted'] <= 10:
        val = df['how_many_prep_voted']
    elif 11 <= df['how_many_prep_voted'] <= 19:
        val = '11-19'
    elif 20 <= df['how_many_prep_voted'] <= 29:
        val = '20-29'
    elif 30 <= df['how_many_prep_voted'] <= 39:
        val = '30-39'
    elif 40 <= df['how_many_prep_voted'] <= 49:
        val = '40-49'
    elif 50 <= df['how_many_prep_voted'] <= 59:
        val = '50-59'
    elif 60 <= df['how_many_prep_voted'] <= 69:
        val = '60-69'
    elif 70 <= df['how_many_prep_voted'] <= 79:
        val = '70-79'
    elif 80 <= df['how_many_prep_voted'] <= 89:
        val = '80-89'
    elif 90 <= df['how_many_prep_voted'] <= 99:
        val = '90-99'
    elif df['how_many_prep_voted'] >= 100:
        val = '100'
    else:
        pass
        val = -1
    return val

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
### 'Spread Your Votes!' Participants ###

def raffle_tickets(df):
    if df.loc['how_many_prep_voted'] <= 10:
        val = 0
    elif 11 <= df['how_many_prep_voted'] <= 19:
        val = 1
    elif 20 <= df['how_many_prep_voted'] <= 29:
        val = 2
    elif 30 <= df['how_many_prep_voted'] <= 39:
        val = 3
    elif 40 <= df['how_many_prep_voted'] <= 49:
        val = 4
    elif 50 <= df['how_many_prep_voted'] <= 59:
        val = 5
    elif 60 <= df['how_many_prep_voted'] <= 69:
        val = 6
    elif 70 <= df['how_many_prep_voted'] <= 79:
        val = 7
    elif 80 <= df['how_many_prep_voted'] <= 89:
        val = 8
    elif 90 <= df['how_many_prep_voted'] <= 99:
        val = 9
    elif df['how_many_prep_voted'] >= 100:
        val = 10
    else:
        pass
        val = -1
    return val

# For transparency, Spread Your Votes data
# df_longer_this_term = df_longer[df_longer[measuring_interval].isin([this_term])]

# SYV_participants = df_longer_this_term[df_longer_this_term['how_many_prep_voted'] > 10]
SYV_participants = df_longer[df_longer['how_many_prep_voted'] > 10]

SYV_participants = SYV_participants[['validator_name', 'delegator', measuring_interval, 'votes', 'cum_votes', 'how_many_prep_voted']]
SYV_participants['NumPReps_bin'] = SYV_participants.apply(bin_NumPReps, axis=1)
SYV_participants['raffle_tickets'] = SYV_participants.apply(raffle_tickets, axis=1)

# getting the % of votes per P-Rep
SYV_participants['sum_votes'] = SYV_participants.groupby([measuring_interval, 'delegator'])['cum_votes'].transform('sum')
SYV_participants = SYV_participants.rename(columns={'cum_votes' : 'total_votes'})
SYV_participants['vote_percentages_per_prep'] = SYV_participants['total_votes']/SYV_participants['sum_votes']


# for google sheets (participant information)
SYV_participants_percentages = SYV_participants.drop(columns=['NumPReps_bin','votes']).\
    groupby([measuring_interval, 'delegator','sum_votes','how_many_prep_voted','raffle_tickets']).\
    agg(['min', 'max', 'median', 'mean']).sort_values(by='how_many_prep_voted', ascending=False).reset_index()

SYV_participants_percentages[('vote_percentages_per_prep', 'min')] = SYV_participants_percentages[('vote_percentages_per_prep', 'min')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'max')] = SYV_participants_percentages[('vote_percentages_per_prep', 'max')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'median')] = SYV_participants_percentages[('vote_percentages_per_prep', 'median')].astype(float).map("{:.5%}".format)
SYV_participants_percentages[('vote_percentages_per_prep', 'mean')] = SYV_participants_percentages[('vote_percentages_per_prep', 'mean')].astype(float).map("{:.5%}".format)

# this term
SYV_participants_percentages_this_term = SYV_participants_percentages[SYV_participants_percentages[measuring_interval].isin([this_term])]
SYV_participants_percentages_this_term.to_csv(os.path.join(resultsPath_interval, 'IIN_SpreadYourVotes_RaffleTickets_' + this_term + '.csv'), index=False)

SYV_participants_percentages = []
SYV_participants_percentages_this_term = []

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Lucky Draw ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# for lucky draw
SYV_participants_luckydraw_this_term = []
SYV_participants_luckydraw_this_term = SYV_participants[SYV_participants[measuring_interval].isin([this_term])]
SYV_participants_luckydraw_this_term = SYV_participants_luckydraw_this_term.groupby(['delegator',measuring_interval]).\
    head(SYV_participants_luckydraw_this_term['raffle_tickets'])[['delegator', measuring_interval, 'how_many_prep_voted', 'NumPReps_bin', 'raffle_tickets']]
SYV_participants_luckydraw_this_term = SYV_participants_luckydraw_this_term.sort_values(by='how_many_prep_voted', ascending=False)

# grand prize winning rate
grand_prize_winning_rate = SYV_participants_luckydraw_this_term['delegator'].value_counts(normalize=True)
grand_prize_winning_rate = grand_prize_winning_rate.reset_index(name='grand_prize_winning_rate').rename(columns={'index': 'delegator'})
grand_prize_winning_rate['grand_prize_winning_rate'] = grand_prize_winning_rate['grand_prize_winning_rate'].astype(float).map("{:.2%}".format)
SYV_participants_luckydraw_this_term = pd.merge(SYV_participants_luckydraw_this_term, grand_prize_winning_rate, on='delegator', how='left')


SYV_participants_overall = SYV_participants_luckydraw_this_term.\
    drop(columns={'how_many_prep_voted'}).\
    drop_duplicates(['delegator', measuring_interval, 'NumPReps_bin']).\
    groupby(['NumPReps_bin', 'raffle_tickets'])['delegator'].\
    agg('count').\
    reset_index().\
    rename(columns={'delegator':'count_participants'}).\
    sort_values(by='raffle_tickets', ascending=False)

SYV_participants_luckydraw_small_prize = SYV_participants_luckydraw_this_term.\
    drop_duplicates(['delegator',measuring_interval,'how_many_prep_voted','NumPReps_bin'])


# small prize winners
from datetime import datetime
try:
    from itertools import zip_longest
except ImportError:
    # Python 2
    from itertools import izip_longest as zip_longest

No_of_total_winners = 10

NumPReps_bin = SYV_participants_luckydraw_small_prize.\
    drop_duplicates(['NumPReps_bin']).\
    sort_values(by='raffle_tickets', ascending=False)[['NumPReps_bin']].\
    reset_index(drop=True)


small_prize_winners = []
for i in range(No_of_total_winners):

    small_prize_winners_temp = SYV_participants_luckydraw_small_prize.groupby('NumPReps_bin').\
        apply(lambda x: x.sample(n=1, random_state=datetime.now().microsecond)).reset_index(drop=True).\
        sort_values(by='raffle_tickets', ascending=False)

    small_prize_winners.append(small_prize_winners_temp)

small_prize_winners = pd.concat(small_prize_winners)
small_prize_winners = small_prize_winners.drop_duplicates('delegator').sort_values(by='raffle_tickets', ascending=False)


# assigning number of winners depending on total number of winners and number of brackets available
division_no = int(No_of_total_winners / len(NumPReps_bin))
remainder_no = No_of_total_winners % len(NumPReps_bin)
lists = [[division_no] * len(NumPReps_bin), [division_no] * remainder_no]
prize_division = pd.DataFrame({'default_winners': [sum(x) for x in zip_longest(*lists, fillvalue=0)]})

# logic to get first turn winner and leftovers
len_winners_group = small_prize_winners.groupby('raffle_tickets')['delegator'].\
    agg('count').reset_index().sort_values(by='raffle_tickets', ascending=False).reset_index(drop=True)
len_winners_group = pd.concat([len_winners_group, prize_division], axis=1)
len_winners_group['leftover'] = len_winners_group['delegator'] - len_winners_group['default_winners']
len_winners_group['leftover'] = np.where(len_winners_group['leftover'] < 0, len_winners_group['leftover'], 0)
len_winners_group['first_turn_winner'] = len_winners_group['default_winners'] + len_winners_group['leftover']

# first turn winners
first_turn_winner = len_winners_group[['raffle_tickets', 'first_turn_winner']]
first_turn_winner = small_prize_winners.merge(first_turn_winner, on='raffle_tickets', how='left')
first_turn_winner = (first_turn_winner.groupby('raffle_tickets', group_keys=False)
        .apply(lambda x: x.head(x['first_turn_winner'].iat[0]))).drop(columns='first_turn_winner')
first_turn_winner['prize_type'] = 'small_prize'
first_turn_winner['turn'] = 'first'


first_turn_winner_temp = first_turn_winner.\
    drop(columns={'delegator', 'how_many_prep_voted', 'turn'}).\
    groupby(['date', 'NumPReps_bin', 'raffle_tickets', 'grand_prize_winning_rate'])['prize_type'].\
    agg('count').\
    reset_index().\
    rename(columns={'prize_type':'count_winner'}).\
    sort_values(by='raffle_tickets', ascending=False)


first_turn_winner_total = pd.merge(first_turn_winner_temp, SYV_participants_overall, on=['NumPReps_bin', 'raffle_tickets'], how='outer')

first_turn_winner_total['small_prize_winning_rate'] = first_turn_winner_total['count_winner']/first_turn_winner_total['count_participants']
first_turn_winner_total['small_prize_winning_rate'] = first_turn_winner_total['small_prize_winning_rate'].astype(float).map("{:.2%}".format)


winner_output = first_turn_winner_total[['NumPReps_bin', 'raffle_tickets', 'count_participants','count_winner','grand_prize_winning_rate','small_prize_winning_rate']]

winner_output = winner_output.\
    rename(columns={'NumPReps_bin': 'No. of P-Reps Voted', 'raffle_tickets': 'No. of Raffle Tickets \n Per Wallet',
                    'count_participants': 'No. of participant(s)',  'count_winner': 'No. of winner(s)',
                    'grand_prize_winning_rate': 'Grand Prize \n Winning Rate', 'small_prize_winning_rate':'Small Prize  \n Winning Rate'})



import six

def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=12,
                     header_color='#40466e', row_colors=['black', 'black'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
        ax.set_title("'Spread Your Votes!'" + ' Participants', fontsize=15,
                     weight='bold', pad=30)
        plt.tight_layout()

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax

plt.style.use(['dark_background'])
render_mpl_table(winner_output, header_columns=0, col_width=2.5)

# saving
plt.savefig(os.path.join(resultsPath_interval, "Spread_your_votes_participants_" + this_term + ".png"))




# 2nd turn winner - leftovers will do re-draw
# second_turn = abs(sum(len_winners_group['leftover']))
# second_turn_winner = small_prize_winners[~small_prize_winners.delegator.isin(first_turn_winner.delegator)]
# second_turn_winner = second_turn_winner.sample(n=second_turn,
#                                                random_state=datetime.now().microsecond,
#                                                weights=second_turn_winner.raffle_tickets)
# second_turn_winner['prize_type'] = 'small_prize'
# second_turn_winner['turn'] = 'second'
#
# # appending 1st and 2nd turn first so that it can be 'ranked' by how many p-reps voted
# frst_second_turn_winner = first_turn_winner.\
#     append(second_turn_winner).\
#     sort_values(by=['how_many_prep_voted'], ascending=[False]).\
#     reset_index(drop=True)









#
# all_prize_winners = grand_prize_winner_details.\
#     append(frst_second_turn_winner).\
#     reset_index(drop=True)
#
# # total pool of money here
# total_prize_money = 500
# # all_prize_winners['winnings'] = np.where(all_prize_winners['turn'] == 'Wheel_of_Fortune',
# #                                          'US$' + str(int(total_prize_money/2)),
# #                                          'US$' + str(int(total_prize_money/2/10)))
#
# winnings = [int(total_prize_money/2),40,30,30,30,25,25,20,20,20,10] # manual
# winnings = ['US$ {:}'.format(item) for item in winnings]
# all_prize_winners['winnings'] = winnings




print(unique_date[-1:])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#
# df_agg = df_longer.groupby([measuring_interval,'validator_name'])['cum_votes']\
#     .agg(['sum','count'])\
#     .reset_index()
#
# df_agg['prep_ranking'] = df_agg.sort_values([measuring_interval, 'sum'], ascending=False)\
#     .groupby(measuring_interval)['sum'].rank(method='first', ascending=False).astype(int)
#
#
# icx_australia = df_agg[df_agg['validator_name'] == 'ICX Australia']
#
# icx_australia_early = icx_australia[:80].reset_index(drop=True)
#
# ## plotting for the progress of vote spreading
# total = icx_australia_early['prep_ranking']
#
#
# sns.set(style="ticks", rc={"lines.linewidth": 3})
# plt.style.use(['dark_background'])
# f, ax = plt.subplots(figsize=(18, 6))
# sns.barplot(x=measuring_interval, y='sum', data=icx_australia_early,
#             palette=sns.cubehelix_palette(len(total), start=.5, rot=-.75))
# h,l = ax.get_legend_handles_labels()
#
# ax.set_xlabel('days', fontsize=14, weight='bold', labelpad=10)
# ax.set_ylabel('Votes (ICX)', fontsize=14, weight='bold', labelpad=10)
# ax.set_title("History of ICX Australia", fontsize=14,
#              weight='bold')
# ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.2f}'. format(x/1000000) + ' M'))
#
#
# sns.despine(offset=5, trim=True)
# plt.tight_layout()
# ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="center")
# ax.grid(False)
# plt.xticks(fontsize=12)
# plt.yticks(fontsize=12)
#
# for i in range(len(total)):
#     p = ax.patches[i]
#     height = p.get_height()
#     ax.text(p.get_x() + p.get_width() / 2., height + height * 0.02,
#                 total[i],
#                 ha="center",
#             fontsize=10)
#
#
# plt.tight_layout()