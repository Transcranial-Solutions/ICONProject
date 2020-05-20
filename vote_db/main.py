import sqlite3
from rpc import get_block, get_transaction_result

# Psuedocode
#
#
#  Open database
#  Initialize schema
#
#  For every block in blockchain:
#        Extract list of transactions.
#        
#        For every transaction in list:
#              Test if transaction --> cx0000000...0. Continue on to next transaction if not.
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






class vote_transaction:
    def __init__(block_timestamp = None, from_ = None, data = None, 
                 txhash = None, block = None, to = None, status = None):
        self.block_timestamp
        self.from_
        self.data
        self.txhash
        self.block
        self.to
        self.status

    def insert_into_db(db = "db/vote.db"):
        ## TODO

