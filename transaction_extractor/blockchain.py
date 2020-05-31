


class Block:

    BLOCK_HEIGHT_KEY = b'block_height_key'
    BLOCK_HEIGHT_BYTES_LEN = 12
    V3_BLOCK_HEIGHT = 10324749
    V4_BLOCK_HEIGHT = 12640761


    def __init__(self, height, db):
        
        # Parse blockdata into block class.
        # More attributes should be added as needed.
        block = get_block(height, db)
        self.db = db
        self.height = height
        
        if height < V3_BLOCK_HEIGHT:
            self.transaction_list = block['transactions']
            self.block_timestamp = int(block['timestamp'], 16)
        else:
            self.transaction_list = block['confirmed_transaction_list']
            self.block_timestamp = block['time_stamp']



        def filter_transactions(self, transactions, delegation = False, staking = False,
                                claimiscore = False):
            
            # If no transactions in block -> return None.
            if not transactions:
                return None

            # store all keyword arguments.
            arguments = locals()
            del arguments[transactions]

            # Fill dictionary with transaction types to be filtered.
            filtered_transactions = {}
            for key, val in arguments:
                if val:
                    filtered_transactions[key] = []


            # Extract transactions according to filter criteria.
            for transaction in transactions:
                transaction = Transaction(transaction)

                if delegation:
                    if transaction.is_delegation():
                        if transaction_successful():
                            ## TODO
                            ## Append filtered_transactions.
                            pass

                
                if staking:
                    if transaction.is_staking():
                        if transaction_successful():
                            ## TODO
                            ## Append to filtered_transactions.
                            pass


                if claimiscore:
                    if transaction.is_claimiscore():
                        if transaction_successful():
                            ## TODO
                            ## Append to filtered_transactions.
                            pass
                
            return filtered_transactions



        def get_block(height, db):
            heightkey = BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
            blockhash = db.get(heightkey)
            block = json.loads(db.get(blockhash))
            return block



    
class Transaction:
    def __init__(self, transaction, db):
        ## TODO
        ## Need to find and read sourcecode/documentation
        ## for how blockstructure has changed for different versions.
        ## Then implement how to parse transaction based on the different versions.
        pass
    
    def is_delegation():
        ## TODO
        ## Return bool
        pass
    
    def is_staking():
        ## TODO
        ## Return bool
        pass

    def is_claimiscore():
        ## TODO
        ## Return bool
        pass

    def was_successful():
        ## TODO
        ## Return bool
        pass

    def write_transaction():
        ## TODO
        ## Return touple of transaction data. None if a category does not exist.
        pass

    def get_transaction_result(txhash, db):
        return json.loads(db.get(txhash))

    