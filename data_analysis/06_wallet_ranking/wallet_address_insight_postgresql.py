import concurrent.futures
import requests
import json
import pandas as pd
from functools import reduce
import numpy as np
import os
import psycopg2
from time import time
from datetime import date, datetime, timedelta
from time import time
from tqdm import tqdm


desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

workers = 8

start_block = 1
end_block = 100

today = datetime.utcnow().strftime('%Y-%m-%d')

currPath = os.getcwd()
if not "06_wallet_ranking" in currPath:
    projectPath = os.path.join(currPath, "06_wallet_ranking")
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


def collect_addresses():
    print("Starting data collection...")
    start = time()
    try:
        connection = psycopg2.connect(user="transcranial01",
                                    password="transcranial01",
                                    host="icon-analytics-main-prod.c8awcgz3p3lg.us-east-1.rds.amazonaws.com",
                                    port="5432",
                                    database="postgres")

        cursor = connection.cursor()

        # getting unique from_address
        # postgreSQL_Query = "SELECT * FROM public.wallet_address"
        # postgreSQL_Query = "SELECT block_number, from_address, to_address, timestamp FROM public.transactions;"
        # postgreSQL_Query = "SELECT block_number FROM public.transactions ORDER BY block_number DESC limit 100"


        # postgreSQL_Query = "REFRESH MATERIALIZED VIEW wallet_address"
        # cursor.execute(postgreSQL_Query)
        # print("Refreshing wallet_address database")
        # wallet_from_address = cursor.fetchall()

        # postgreSQL_Query = "DROP TABLE IF EXISTS temp_table; \
        #             CREATE TEMP TABLE temp_table AS\
        #             SELECT timestamp, s.to_address FROM public.transactions s\
        #             UNION ALL\
        #             SELECT timestamp, t.from_address FROM public.transactions t;\
        #             WITH added_row_number AS (SELECT *,\
        #                 ROW_NUMBER() OVER(PARTITION BY to_address ORDER BY timestamp) AS row_number\
        #               FROM temp_table)\
        #             SELECT timestamp, to_address as address\
        #             FROM added_row_number\
        #             WHERE row_number = 1 and to_address <> '';"
        postgreSQL_Query = "SELECT * FROM public.wallet_creation"
        cursor.execute(postgreSQL_Query)
        print("Selecting from_address")
        wallet_from_address = cursor.fetchall()



        # dataframe
        # wallet_address = pd.DataFrame(wallet_from_address, columns=['block_number', 'from_address', 'to_address', 'timestamp'])
        # wallet_address = pd.DataFrame(wallet_from_address, columns=['block_number'])
        wallet_address = pd.DataFrame(wallet_from_address, columns=['timestamp','address'])

        # print(wallet_address)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    print(f'Time taken: {time() - start}')
    print(f'Starting processing...')
    start = time()
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    ## cleaning

    # drop NaN
    wallet_address = wallet_address.dropna()

    # finding bad address -- e.g. no 'hx' prefix
    bad_address_idx = ~wallet_address['address'].str[:2].str.contains('hx|cx', case=False, regex=True, na=False)
    bad_address = wallet_address[bad_address_idx]

    # adding 'hx' prefix to the bad addresses
    fixed_address = pd.concat([bad_address['timestamp'], 'hx' + bad_address['address']], axis=1)
    wallet_address[bad_address_idx] = fixed_address

    # selecting wallets with 'hx' prefix
    # wallet_address = wallet_address[
    #     wallet_address['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

    # remove dups because sometimes there can be
    wallet_address = wallet_address.drop_duplicates().reset_index(drop=True)

    print(len(wallet_address))

    # remove addresses that are not 42 characters long
    wallet_address = wallet_address.drop(wallet_address[wallet_address['address'].str.len() != 42].index)

    # lower-case addresses in case of bug
    wallet_address['address'] = wallet_address['address'].str.lower()

    # how many wallets
    print(len(wallet_address))

    wallet_address.to_csv(os.path.join(dataPath, 'wallet_address_' + today + '.csv'), index=False)

    print(f'Time taken: {time() - start}')

    return wallet_address

df = collect_addresses()



def timestamp_to_date(df, timestamp, datetype, dateformat):
    df['digits'] = df[timestamp].astype(str).str.count('\d')

    df1 = df[df['digits'] == 19]
    df1[datetype] = pd.to_datetime(df1[timestamp]).dt.strftime(dateformat)

    df2 = df[df['digits'] == 16]
    df2[datetype] = pd.to_datetime(df2[timestamp] * 1000).dt.strftime(dateformat)

    removed_data_count = str(len(df.loc[(df['digits'] != 16) & (df['digits'] != 19)]))
    print("Removed data: " + removed_data_count)

    df = pd.concat([df1, df2]).drop(columns='digits').sort_values(by=[timestamp,'address']).drop(columns=timestamp).reset_index(drop=True)
    return df


# df = timestamp_to_date(df, 'timestamp', 'date', '%Y-%m-%d')
df = timestamp_to_date(df, 'timestamp', 'month', '%Y-%m')


df_hx = df[df['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]
df_cx = df[df['address'].str[:2].str.contains('cx', case=False, regex=True, na=False)]

df_hx_count = df_hx.groupby('month').count().reset_index()
df_cx_count = df_cx.groupby('month').count()


df_hx_count['cum_count'] = df_hx_count['address'].cumsum()


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

sns.set(style="ticks", rc={"lines.linewidth": 2})
plt.style.use(['dark_background'])
f, ax = plt.subplots(figsize=(12, 8))
sns.lineplot(x='month', y='cum_count', data=df_hx_count, palette=sns.color_palette('husl', n_colors=2))
h,l = ax.get_legend_handles_labels()


# ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1e6) + ' M'))
ymin, ymax = ax.get_ylim()
ymax_set = ymax*1.2
ax.set_ylim([ymin,ymax_set])
sns.despine(offset=10, trim=True)
plt.tight_layout()

ax.set_xlabel('Time', fontsize=14, weight='bold', labelpad=10)
ax.set_ylabel('Count', fontsize=14, weight='bold', labelpad=10)
ax.set_title('Cumulative wallet counts', fontsize=14, weight='bold', linespacing=1.5)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'. format(x/1000) + ' K'))


ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
plt.tight_layout()
