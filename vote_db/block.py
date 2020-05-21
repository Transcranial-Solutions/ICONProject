from rpc import get_block, get_transaction_result, transaction_success
import pprint as pp


HEADERS = {'content-type': 'application/json'}
URL = "http://95.179.230.6:9000/api/v3"
#HEADERS = {'content-type': 'application/json'}
#URL = "https://ctz.solidwallet.io/api/v3"

class Block:
    def __init__(self, blocknr):
        block = get_block(blocknr)['result']
        self.blocknr = blocknr
        self.block_timestamp = block['time_stamp']
        self.transaction_list = block['confirmed_transaction_list']
        self.vote_transactions = []
        self.stake_transactions = []
    

    def extract_vote_transactions(self):
        transactions = iter(self.transaction_list)
        next(transactions)

        if not transactions:
            return None
        
        for transaction in transactions:
            if transaction['to'] != "cx0000000000000000000000000000000000000000":
                continue

            if not transaction_success(transaction['txHash']):
                continue

            if transaction['data']['method'] == "setDelegation":
                self.vote_transactions.append({'txhash': transaction['txHash'],
                                               'block': self.blocknr,
                                               'timestamp': self.block_timestamp,
                                               'from': transaction['from'],
                                               'data': transaction['data']})

    
    def extract_stake_transactions(self):
        transactions = iter(self.transaction_list)
        next(transactions)

        if not transactions:
            return None
        
        for transaction in transactions:
            if transaction['to'] != "cx0000000000000000000000000000000000000000":
                continue

            if not transaction_success(transaction['txHash']):
                continue

            if transaction['data']['method'] == "setStake":
                self.vote_transactions.append({'txhash': transaction['txHash'],
                                               'block': self.blocknr,
                                               'timestamp': self.block_timestamp,
                                               'from': transaction['from'],
                                               'data': transaction['data']})

    def write_transactions(self, db, transactions):
        ## TODO
        ## Insert transactions into specified database.
        ## Update last_processed_block in config file.
        ## Print to konsole <Block * processed --> n transactions interted into db
        pass


## Code that might be used.
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

## Testing to see if everything works
for i in range(18078482, 19078482):
    block = Block(i)
    block.extract_vote_transactions()
    print(f"Block: {i}")
    pp.pprint(f"Transactions: {block.vote_transactions}")