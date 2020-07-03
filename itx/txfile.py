import configparser
import textwrap
import json
import csv
import os


class TxFile:
    """
    The TxFile file storing blockchain transactions.
    It's used for creating a file, writing to the file, save the file settings,
    load the file settigs, removing a file etc.
    """

    def __init__(self, name = None, folder = None, inifile = None, from_ = [], to = [],
                 datatypes = [], methods = [], params = [], columns = None, firstblock = None,
                 lastblock = None, transactions= 0):
        self.name = name
        self.folder = folder
        self.inifile = inifile
        self.from_ = from_
        self.to = to
        self.datatypes = datatypes
        self.methods = methods
        self.params = params
        self.columns = columns
        self.firstblock = firstblock
        self.lastblock = lastblock
        self.transactions = 0

        self.rules = None

        self.__fileobj = None
        self.__csvwriter = None

        self.name

    def exists_in_config(self) -> bool:
        """
        Test if a file exists in configuration file.
        Return:
            bool  -  true if exists and false of not.
        """
    
        config = configparser.ConfigParser()
        config.read(self.inifile)
        sections = config.sections()

        if self.name in sections:
            return True
        else:
            return False

    def exists_in_output(self) -> bool:
        """
        Test if file exists in its location specified in the configuration file.
        Return:
            bool  -  true if exists and false if not.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)
        
        if os.path.exists(self.folder + self.name):
            return True
        else:
            return False

    def load_config(self) -> None:
        """
        Load file settings from inifile.
        """

        if not self.exists_in_config():
            raise FileNotFoundError("File does not exist in configuration file.")
        
        config = configparser.ConfigParser()
        config.read(self.inifile)

        # Load folder
        if config.has_option(self.name, "folder"):
            self.folder = config[self.name]["folder"]

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
            self.lastblock = int(config[self.name]['lastblock'])
        if config.has_option(self.name, "transactions"):
            self.transactions = int(config[self.name]['transactions'])

    def save_config(self) -> None:
        """
        Save file setting in inifile.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)

        if not config.has_section(self.name):
            config.add_section(self.name)

        # Save folder
        config[self.name]["folder"] = self.folder

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

    def delete_config(self) -> None:
        """
        Delete file settings from inifile.
        """
        config = configparser.ConfigParser()
        config.read(self.inifile)
        config.remove_section(self.name)

        with open(self.inifile, 'w') as configfile:
            config.write(configfile)

    def delete_file(self) -> None:
        """
        Delete file.
        """

        os.remove(self.folder + self.name)
        
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

    def create_file(self) -> None:
        """
        Create file in specified output folder.
        """
        open(self.folder + self.name, 'w').close()

    def open(self, mode: str) -> None:
        """
        Opens the file in the output directory. Creates it if it does not exists.
        """
        if mode not in ['w', 'a']:
            raise NotImplementedError("Only append and write mode are supported.")

        if not self.exists_in_output():
            raise FileNotFoundError("File does not exist in output folder specified in configuration file.")

        self.__fileobj = open(self.folder + self.name, mode)
        self.__csvwriter = csv.writer(self.__fileobj)
    
    def close(self) -> None:
        """
        Close the file.
        """
        self.__fileobj.close()
        self.__fileobj = None

    
    def append_transaction(self, tx: dict) -> None:
        """
        Extracts features from a transaction according to the 
        columns attribute and appends the resulting transaction to the file.
        Input:
           tx (dict) - transaction with all features.
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
        
        open(self.outputfolder + self.name, 'w').close()

    def print_status(self):
        """
        Print status of this transaction file.
        """
        #Test if file exists in output directory and config file.
        if not self.exists_in_config:
            raise FileNotFoundError("File not found in configuration file.")
        if not self.exists_in_output:
            raise FileExistsError("File not found in output folder specified by configuration file.")
        
        wrapper = textwrap.TextWrapper(width = 120, subsequent_indent=" " * 12)
        sep = " "
        
        print("")
        print(f"Name        : {self.name}")
        print(f"Folder      : {self.folder}")
        if self.from_:
            text = f"From        : {sep.join(self.from_)}"
            text = wrapper.wrap(text)
            for line in text:
                print(line)
        else:
            print(f"From        : No filter")
        if self.to:
            text = f"To          : {sep.join(self.to)}"
            text = wrapper.wrap(text)
            for line in text:
                print(line)
        else:
            print(f"To          : No filter")
        if self.datatypes:
            print(f"Datatypes   : {sep.join(self.datatypes)}")
        else:
            print(f"Datatypes   : No filter")
        if self.methods:
            print(f"Methods     : {sep.join(self.methods)}")
        else:
            print(f"Methods     : No filter")
        if self.params:
            print(f"Params      : {sep.join(self.params)}")
        else:
            print(f"Params      : No filter")
        if self.columns:
            print(f"Columns     : {sep.join(self.columns)}")
        print(f"Firstblock  : {self.firstblock}")
        print(f"Lastblock   : {self.lastblock}")
        print(f"Transactions: {self.transactions}")
        print("")
