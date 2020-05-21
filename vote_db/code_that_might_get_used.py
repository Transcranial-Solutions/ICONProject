    def insert_into_db(db = "db/vote.db"):
        pass
        ## TODO



def process_transaction_list(transactions):
    vote_transactions = []  
    del transactions[0]
    
    if not transactions:
        return vote_transactions

    for transaction in transactions:
        if transaction['to'] != "cx0000000000000000000000000000000000000000":
            continue

        if get_transaction_result()['results']['status'] == "0x1":
            vote_transaction.append((transaction['txhash'], transaction['from'],
                                     transaction['data']))

    return vote_transactions