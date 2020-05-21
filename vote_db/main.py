import sqlite3, requests
from rpc import get_block, get_transaction_result

# Psuedocode

#  Open database
#  Initialize schema
#
#  For every block in blockchain:
#        Extract list of transactions.
#        
#        For every transaction in list:
#              Test if transaction --> cx0000000...0. Continue on to next transaction if not.
#              Test if transaction method is set_delegation
#              Test if transaction was successful. Discard if unsucessful and insert into db if successful.
#      
DB = "db/vote.db"
SCHEMA = "sql/schema.sql"
HEADERS = {'content-type': 'application/json'}
URL = "https://ctz.solidwallet.io/api/v3"
             


def main():

    # Open/create db and initialize the tables.
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    
    with open(SCHEMA, 'r') as f:
        for line in f:
            cursor.execute(line)
    
    connection.commit()
    connection.close()

    # Process blocks
    counter = 0
    while True:
        try:
            ## TODO
        except KeyError:
            ## TODO
            print(f"All blocks processed. Last block: {}", counter)
            break
        
            


def execute_sqlscript(scriptfile, db):
    connection = sqlite3.connect(DB)
    cursor = connection.cursor()
    
    with open(scriptfile, 'r') as f:
        for line in f:
            cursor.execute(line)
    
    connection.commit()
    connection.close()


if __name__ = __main__:
    main()