from __future__ import annotations
import json
import plyvel


class Block:

    BLOCK_HEIGHT_KEY = b'block_height_key'
    BLOCK_HEIGHT_BYTES_LEN = 12
    V1_BLOCK_HEIGH = 0
    V3_BLOCK_HEIGHT = 10324749
    V4_BLOCK_HEIGHT = 12640761
    V5_BLOCK_HEIGHT = 14473622

    def __init__(self, height: int, db: Leveldb) -> Block:
        # Parse blockdata into block class.
        # More attributes should be added as needed.
        block = self.get_block(height, db)
        self.db = db
        self.height = height
        
        if height < self.V3_BLOCK_HEIGHT:
            self.transactions = block['confirmed_transaction_list']
            self.timestamp = block['time_stamp']
        else:
            self.transactions = block['transactions']
            self.timestamp = int(block['timestamp'], 16)

    
    def get_block(self, height, db):
        heightkey = self.BLOCK_HEIGHT_KEY + height.to_bytes(self.BLOCK_HEIGHT_BYTES_LEN, byteorder='big')
        blockhash = db.get(heightkey)
        if not blockhash:
            pass # What happends when block not present? 
        block = json.loads(db.get(blockhash))  # --> TypeError: Argument 'key' has incorrect type (expected bytes, got NoneType)
        return block


class Transaction:

    def __init__(self, transaction: dict, db: Leveldb, blockheight = None, blocktimestamp = None) -> Transaction:
        self.db = db

        # Set blockheight and block_timestamp.
        if blockheight:
            self.blockheight = blockheight
        else:
            self.blockheight = None
        
        if blocktimestamp:
            self.blocktimestamp = blocktimestamp
        else:
            self.blocktimestamp = None

        # Transaction version.
        try:
            self.version = transaction['version']
        except KeyError:
            self.version = "0x1"

        # Initialize transaction based on different versions.
        if self.version == "0x1":
            try:
                self.from_ = transaction['from']
            except KeyError:
                self.from_ = None
                
            try: 
                self.to = transaction['to']
            except KeyError:
                self.to = None

            try:
                self.value = transaction['value']
            except KeyError:
                self.value = None

            # No data and datatype in verison 1?
            self.datatype = None
            self.data = None
                 
            try:
                self.method = transaction['data']['method']
            except KeyError:
                self.method = None

            try:
                self.params = transaction['data']['params']
            except KeyError:
                self.params = None

            try:
                self.txhash = transaction['tx_hash']
            except KeyError:
                self.txhash = None


        elif self.version == "0x3":
            try:
                self.from_ = transaction['from']
            except KeyError:
                self.from_ = None

            try: 
                self.to = transaction['to']
            except KeyError:
                self.to = None
            
            try:
                self.value = transaction['value']
            except KeyError:
                self.value = None
            
            try:
                self.datatype = transaction['dataType']
            except KeyError:
                self.datatype = None
            
            try:
                self.data = transaction['data']
            except KeyError:
                self.data = None

            
            if not self.datatype == "message":
                try:
                    self.method = transaction['data']['method']
                except KeyError:
                    self.method = None
            
                try:
                    self.params = transaction['data']['params']
                except KeyError:
                    self.params = None
            else:
                self.method = None
                self.params = None

            try:
                self.txhash = transaction['txHash']
            except KeyError:
                self.txhash = None

        else:
            raise Exception(f"{self.version} not handled by class.")

        ## TODO
        ## Need to find and read sourcecode/documentation
        ## for how blockstructure and transactionstructure has changed for different versions.
        ## Compare to current design.

    def convert_units(self) -> None:
        ##TODO
        pass


    def is_from(self, from_: set) -> bool:
        if self.from_ in from_:
            return True
        else:
            return False


    def is_to(self, to: set) -> bool:
        if self.to in to:
            return True
        else:
            return False
        

    def has_datatype(self, datatypes: set) -> bool:
        if self.datatype in datatypes:
            return True
        else:
            return False


    def has_method(self, methods: set) -> bool:
        if self.method in methods:
            return True
        else:
            return False


    def has_parameter(self, parameters: set) -> bool:
        try:    
            for param in self.params.keys():
                if param in parameters:
                    return True
        except AttributeError:
            pass
        return False


    def was_successful(self) -> bool:
        if self.get_transaction_result()['result']['status'] == "0x1":
            return True
        else:
            return False


    ## Keep this?
    def fulfills_criteria(self, from_ = None, to = None, datatypes = None,
                          methods = None, params = None) -> bool:
        if from_:
            if not self.is_from(from_):
                return False
        if to:
            if not self.is_to(to):
                return False
        if datatypes:
            if not self.has_datatype(datatypes):
                return False    
        if methods:
            if not self.has_method(methods):
                return False
        if params:
            if not self.has_parameter(params):
                return False
        return True
        

    def get_transaction(self):
        return {"block": self.blockheight, "from": self.from_, "to": self.to, "value": self.value, "datatype": self.datatype,
                "data": self.data, "txhash": self.txhash, "blocktimestamp": self.blocktimestamp}

    
    def get_transaction_result(self):
        return json.loads(self.db.get(self.txhash.encode()))
