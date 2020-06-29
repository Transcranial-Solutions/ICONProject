import configparser


class TxFile:
    """
    The TxFile file storing blockchain transactions.
    It's used for creating a file, writing to the file, save the file settings,
    load the file settigs, removing a file etc.
    """

    def __init__(self, filename: str, inifile = None):
        self.inifile = inifile
        self.name = filename
        self.__fileobj = None
        self.__csvwriter = None
        self.from_ = []
        self.to = []
        self.datatypes = []
        self.methods = []
        self.params = []
        self.columns = None
        self.rules = None  
        self.firstblock = None
        self.lastblock = None 
        self.transactions = 0

    def load_config(self) -> None:
        """
        Load file settings from inifile.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)

        # Load rules.
        if config.has_option(self.name, "from"):
            self.from_ = json.loads(config[self.name]['from'])
        if config.has_option(self.name, "to"):
            self.to = json.loads(config.get(self.name, "to"))
        if config.has_option(self.name, "datatypes"):
            self.datatypes = json.loads(config[self.name]['datatypes'])
        if config.has_option(self.name, "methods"):
            self.methods = json.loads(config[self.name]['methods'])
        if config.has_option(self.name, "params"):
            self.params = json.loads(config[self.name]['params'])
        
        # Load columns.
        if config.has_option(self.name, "columns"):
            self.columns = json.loads(config[self.name]['columns'])

        # Load blocks.
        if config.has_option(self.name, "firstblock"):
            self.firstblock = int(config[self.name]['firstblock'])
        if config.has_option(self.name, "lastblock"):
            self.lastblock = json.loads(config[self.name]['lastblock'])
        if config.has_option(self.name, "transactions"):
            self.transactions = json.loads(config[self.name]['transactions'])

    def save_config(self) -> None:
        """
        Save file setting in inifile.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)

        if not config.has_section(self.name):
            config.add_section(self.name)

        # Save rules.
        config[self.name]['from'] = json.dumps(self.from_)
        config[self.name]['to'] = json.dumps(self.to)
        config[self.name]['datatypes'] = json.dumps(self.datatypes)
        config[self.name]['methods'] = json.dumps(self.methods)
        config[self.name]['params'] = json.dumps(self.params)

        # Save columns
        if self.columns:
            config[self.name]['columns'] = json.dumps(self.columns)

        # Save blocks.
        if self.firstblock:
            config[self.name]['firstblock'] = str(self.firstblock)
        if self.lastblock:
            config[self.name]['lastblock'] = str(self.lastblock)
        if self.transactions:
            config[self.name]['transactions'] = str(self.transactions)
        
        # Write to file.
        with open(self.inifile, 'w') as configfile:
            config.write(configfile)

    def delete_config(self):
        """
        Delete file settings from inifile.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)
        config.remove_section(self.name)

        with open(self.inifile, 'w') as configfile:
            config.write(configfile)
        
    def set_rules(self) -> None:
        """
        Set rules attribute.
        """
        rules = {}
        parser = configparser.ConfigParser()
        parser.read(self.inifile)
        rules['from_'] = set(json.loads(parser.get(self.name, 'from')))
        rules['to'] = set(json.loads(parser.get(self.name, 'to')))
        rules['datatypes'] = set(json.loads(parser.get(self.name, 'datatypes')))
        rules['methods'] = set(json.loads(parser.get(self.name, 'methods')))
        rules['params'] = set(json.loads(parser.get(self.name, 'params')))
        self.rules = rules

    def open(self, mode: str) -> None:
        """
        Opens the file in the output directory. Creates it if it does not exists.
        """
        if mode not in ['w', 'a']:
            raise NotImplementedError("Only append and write mode are supported.")

        self.__fileobj = open(OUTPUT + self.name, mode)
        self.__csvwriter = csv.writer(self.__fileobj)
    
    def close(self) -> None:
        """
        Close the file.
        """
        self.__fileobj.close()
        self.__fileobj = None

    
    def append_transaction(self, tx: list) -> None:
        """
        Extracts features from a transaction according to the 
        columns attribute and appends the resulting transaction to the file.
        Input:
           tx (list) - transaction with all features.
        Output:
           None
        """
        tx = [tx[column] for column in self.columns]
        self.__csvwriter.writerow(tx)


    def write_header_row(self) -> None:
        """
        Write header for csv file.
        """
        self.__csvwriter.writerow(self.columns)


    def clear_all_transactions(self):
        """
        Clears all transactions from the file.
        """
        if self.__fileobj:
            raise Exception("The file is already open. Close the file and try again.")
        
        open(OUTPUT + self.name, 'w').close()
