


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



        def extract_all_transactions(transactions):
            
            extracted_transactions = []

            if not transactions:
                return None

            for transaction in transactions:
                transaction = Transaction(transaction)
                
                if transaction.was_successful():
                    extracted_transactions.append(transaction.write_transaction())

            return extracted_transactions


        def filter_transactions(self, transactions, delegation = False, staking = False,
                                claimiscore = False, iconbet = False):
            
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
                        if transaction.was_successful():
                            ## TODO
                            ## Append filtered_transactions.
                            pass

                
                if staking:
                    if transaction.is_staking():
                        if transaction.was_successful():
                            ## TODO
                            ## Append to filtered_transactions.
                            pass


                if claimiscore:
                    if transaction.is_claimiscore():
                        if transaction.was_successful():
                            ## TODO
                            ## Append to filtered_transactions.
                            pass
 
                
                if iconbet:
                    if transaction.is_iconbet():
                        if transaction.was_successful():
                            ## TODO
                            ## Append to filtered_transaction.
                            pass

            return filtered_transactions



        def get_block(height, db):
            heightkey = BLOCK_HEIGHT_KEY + height.to_bytes(BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
            blockhash = db.get(heightkey)
            block = json.loads(db.get(blockhash))
            return block



    
class Transaction:
    def __init__(self, transaction, db):
        
        try:
            self.from = transaction['from']
        except KeyError:
            self.from = None

        try:
            self.to = transaction['to']
        except KeyError:
            self.to = None

        try:
            self.value = transaction['value']
        except KeyError:
            self.value = None

        try:
            self.tx_timestamp = transaction['timestamp']
        except KeyError:
            self.tx_timestamp = None

        try:
            self.data_type = transaction['datatype']
        except KeyError:
            self.data_type = None

        try:
            self.data = transaction['data']
        except KeyError:
            self.data = None

        try:
            self.signature = transaction['signature']
        except KeyError:
            self.signature = None

        try:
            self.txhash = transaction['txhash']
        except:
            self.txhash = None


        ## TODO
        ## Need to find and read sourcecode/documentation
        ## for how blockstructure and transactionstructure has changed for different versions.
        ## Then implement how to parse transaction based on the different versions.
        pass
    

    def is_irep():
        if not is self.is_governance():
            return False

        if self.data['method'] == "setGovernanceVariables" and
           "irep" in self.data['params']:
           return True
        else:
            return False


    def is_delegation():
        if not self.is_governance():
            return False
        
        if self.data['method'] == "setDelegation":
            return True
        else:
            return False


    def is_staking():
        if not self.is_governance():
            return False
        
        if self.data['method'] == "setStake":
            return True
        else:
            return False

    def is_claimiscore():
        if not self.is_governance():
            return False
        
        if self.data['method'] == "claimIscore":
            return True
        else:
            return False


    def is_base():
        if self.data_type == 'base':
            return True
        else:
            return False


    def is_iconbet():
        ## TODO
        ## Return bool
        pass


    def is_governance():
        if self.to == 'cx0000000000000000000000000000000000000000':
            return True
        else:
            return False

    def was_successful():
        ## TODO
        ## Return bool
        pass


    def write_transaction():
        return (self.from, self.to, self.value, self.tx_timestamp, self.data_type,
                self.data, self.signature, self.txhash, self.blocktimestamp, self.block)

    def get_transaction_result(txhash, db):
        return json.loads(db.get(txhash))

    