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


        postgreSQL_Query = "REFRESH MATERIALIZED VIEW wallet_address"
        cursor.execute(postgreSQL_Query)
        print("Refreshing wallet_address database")
        wallet_from_address = cursor.fetchall()

        postgreSQL_Query = "SELECT * FROM public.wallet_address"
        cursor.execute(postgreSQL_Query)
        print("Selecting from_address")
        wallet_from_address = cursor.fetchall()



        # dataframe
        # wallet_address = pd.DataFrame(wallet_from_address, columns=['block_number', 'from_address', 'to_address', 'timestamp'])
        # wallet_address = pd.DataFrame(wallet_from_address, columns=['block_number'])
        wallet_address = pd.DataFrame(wallet_from_address, columns=['address'])

        print(wallet_address)


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
    fixed_address = 'hx' + bad_address
    wallet_address[bad_address_idx] = fixed_address

    # selecting wallets with 'hx' prefix
    wallet_address = wallet_address[
        wallet_address['address'].str[:2].str.contains('hx', case=False, regex=True, na=False)]

    # remove dups because sometimes there can be
    wallet_address = wallet_address.drop_duplicates().reset_index(drop=True)

    # remove addresses that are not 42 characters long
    wallet_address = wallet_address.drop(wallet_address[wallet_address['address'].str.len() != 42].index)

    # lower-case addresses in case of bug
    wallet_address['address'] = wallet_address['address'].str.lower()

    # to series
    wallet_address = wallet_address.address

    # how many wallets
    len_wallet_address = len(wallet_address)

    wallet_address.to_csv(os.path.join(dataPath, 'wallet_address_' + today + '.csv'), index=False)

    print(f'Time taken: {time() - start}')

    return wallet_address

df = collect_addresses()