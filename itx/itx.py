import argparse
import configparser
from blockchain import Block
from blockchain import Transaction
import csv
import json
import plyvel
import signal
import time
from threading import Timer, Thread
import datetime
import os
import sys
from tqdm import tqdm
import textwrap


COLUMNS = ["block", "from", "to", "value", "datatype", "data", "txhash", "blocktimestamp"]
INTERVAL = 30
CONFIG = "./data/itx.ini"

#Get default arguments from file.
config = configparser.ConfigParser()
config.read(CONFIG)
df_args = dict(config['DEFAULT'])

OUTPUT = df_args['output']
LEVELDB = df_args['leveldb']

def main():
    
    #Get default arguments from file.
    config = configparser.ConfigParser()
    config.read(CONFIG)
    df_args = dict(config['DEFAULT'])


    # Create parser object.
    parser = argparse.ArgumentParser(prog = "itx",
                                     usage = "python3 itx <operation> [options] <target>",
                                     description = "Tool for extracting transactions from " 
                                                   "ICON blockchain using filter criterias. ",
                                     add_help = True)

    #parser.add_argument('--help', '-h', action = "store_true")
    

    # Add subparser to main parser object.
    subparsers = parser.add_subparsers(title = "operations",
                                       prog = "python3")


    # Create parser for initialize.
    parser_initialize = subparsers.add_parser('init',
                                           usage = 'python3 itx.py initialize [options] <file>',
                                           help = 'Creates a file with specified name and saves extractions rules '
                                                  'for that file.',
                                           add_help = True)
    
    parser_initialize.add_argument('--file', type = str, required = True,
                                help = "Filename for transaction storage. Csv file.")
    
    parser_initialize.add_argument('--from', metavar = '<addr>', type = str, nargs = "+", dest = "from_",
                                action = CustomAction1, help = "Sending addresses.", default = [])

    parser_initialize.add_argument('--to', metavar = '<addr>', type = str, nargs = "+",
                                action = CustomAction1, help = "Recieving addresses.", default = [])

    parser_initialize.add_argument('--datatypes', metavar = '<datatypes>', type=str,
                                nargs = '+', help='Transaction datatypes.', default = [])

    parser_initialize.add_argument('--methods', metavar = '<methods>', type = str, nargs = "+",
                                help = "Transaction method called", default = [])

    parser_initialize.add_argument('--params', metavar = '<paramaters>', type = str, nargs = "+", 
                                help = 'Parameters in method call.', default = [])

    parser_initialize.add_argument('--columns', metavar = '<columns>', choices = COLUMNS, type = str, nargs = "+",
                                   help = 'Table structure in file.', default = COLUMNS)

    parser_initialize.set_defaults(func = initialize)

    # Create parser for extract.
    parser_extract = subparsers.add_parser('extract',
                                            usage = 'python3 itx.py extract [options] <file>',
                                            help = 'Extracts transactions from a specified block interval to the specified files. '
                                                   'Transactions are extracted as per the rules '
                                                   'specified when the file was initialized.',
                                            add_help = True)

    parser_extract.add_argument('--files', type = str, required = True, nargs = "+",
                                help = "File to store extracted transactions in.")
    
    parser_extract.add_argument('--blocks', type = int, metavar = "block", nargs = 2,
                                help = 'Block interval to extract transactions from.')
    
    parser_extract.set_defaults(func = extract)

    
    # Create parser for update command.
    parser_update = subparsers.add_parser('update', 
                                          usage = "python3 itx update [options] <file/s>",
                                          help = 'Update files to the specified blockheight. '
                                                 'Use this command if you have already used the '
                                                 'extraction command on the files. '
                                                 'Transactions are extracted as per the rules '
                                                 'specified when the file was initialized.',
                                          add_help = True)


    # Create parser for syncronize command.
    parser_syncronize = subparsers.add_parser('syncronize',
                                              usage='python3 itx.py syncronize [options] <file>',
                                              help='Keep files syncronized via RPC.',
                                              add_help=True)


    # Create parser for syncronize command.
    parser_remove = subparsers.add_parser('remove',
                                           usage = 'python3 itx.py remove [options] <file/s>',
                                           help = 'Remove specified files and their configuration.',
                                           add_help=True)
    
    parser_remove.add_argument('--files', type = str, nargs = "+", help = "Files to remove.")

    parser_remove.add_argument('--all', action = 'store_true', help = 'Remove all files and their configuration.')

    parser_remove.set_defaults(func = remove)

    # Create parser for status command.
    parser_status = subparsers.add_parser('status',
                                          usage='python3 itx.py status',
                                          help='Check status for all tracked files.',
                                          add_help=True)

    #parser_status.add_argument('--file', type = str, help = "File for status check")

    parser_status.set_defaults(func = status)
    # Custom helpfile
    #if namespace.help:
    #	with open('help_file.txt', 'r') as f:
    #		content = f.read()
    #       print(content)

    args = parser.parse_args()
    args.func(args)

def initialize(args):

    # Check if filetype specified.
    if not args.file.endswith(".csv"):
        print("Did you forget to specify filetype? Only .csv files are supported.")
        sys.exit(1)
    
    # Handle file already exists.
    filepath = OUTPUT + args.file
    if os.path.exists(filepath):
        while True:
            response = input(f"{args.file} already exists. Overwrite? (Y/n): ")
            if response in ["Y", "y", ""]:
                os.remove(filepath)
                break
            
            elif response in ["N", "n"]:
                print("Aborting program.")
                sys.exit(2)
            
            else:
                continue
    
    ## Add above code to class??
    # Initialize TxFile obj.
    txfile = TxFile(args.file, CONFIG)
   
    # Remove previous configuration.
    txfile.delete_config()

    # Set attributes from command line.
    txfile.from_ = args.from_
    txfile.to = args.to
    txfile.datatypes = args.datatypes
    txfile.methods = args.methods
    txfile.params = args.params
    txfile.columns = args.columns

    # Save to .ini file.
    txfile.save_config()

    # Create file in output folder and write header row.
    txfile.open('w')
    txfile.write_header_row()
    txfile.close()

def extract(args):
    plyveldb = plyvel.DB(LEVELDB, create_if_missing = False)

    # Check if all files exists.
    for file in args.files:
        if not os.path.exists(OUTPUT + file):
            raise FileNotFoundError(f"{file} does not exist.")
    
    # Prepare list of TxFile objects.
    txfiles = []
    for file in args.files:
        txfile = TxFile(file, inifile = CONFIG)
        txfile.load_config()
        txfile.firstblock = args.blocks[0]
        txfile.set_rules()
        txfile.open('a')
        txfiles.append(txfile)

    print("Extracting transactions...")
    ## Ask if user wants to proceed with extraction.
    #if proceed():
    #    print("Extraction transactions...")
    #else:
    #    print("Exiting program...")
    #    sys.exit(2)

    # Extract all transactions form each block.
    loop_broken = False
    counter = -1
    for block in tqdm(range(args.blocks[0], args.blocks[1] + 1), mininterval = 1, unit = "blocks"):
        try:
            block = Block(block, plyveldb)
        except TypeError:
            loop_broken = True
            break
        transactions = block.transactions
        
        # Test each transaction against rules.
        for transaction in transactions:
            transaction = Transaction(transaction, plyveldb, blockheight = block.height, blocktimestamp = block.timestamp)
            for txfile in txfiles:    
                if not transaction.fulfills_criteria(**txfile.rules):
                    continue
                if not transaction.was_successful():
                    continue

                # Write to file if all tests passed
                txfile.append_transaction(transaction.get_transaction())
                txfile.transactions += 1
        counter += 1

    if loop_broken:
        print(f"Block {block} not found in database. Ending extraction ...")

    # Update config and close files 
    for txfile in txfiles:
        txfile.lastblock = txfile.firstblock + counter
        txfile.save_config()
        txfile.close() 
    plyveldb.close()
    
    # Endreport
    # endreport()

def update():
    ## TODO
    pass

def remove(args) -> None:
    """
    Remove file from output directory and clear configurations for that file itx.ini.
    """
    config = configparser.ConfigParser()
    config.read(CONFIG)

    if args.all:
        files = os.listdir(OUTPUT)
        for file in files:
            os.remove(OUTPUT + file)
        for file in config.sections():
            config.remove_section(file)

    else:
        for file in args.files:
            os.remove(OUTPUT + file)
            config.remove_section(file)
    
    with open(CONFIG, 'w') as f:
        config.write(f)


def syncronize():
    ## TODO
    pass


def status(args) -> None:
    """
    Print status of all tracked files.
    """
    config = configparser.ConfigParser()
    config.read(CONFIG)
    files = config.sections()
    wrapper = textwrap.TextWrapper(width = 120, subsequent_indent=" " * 12)
    sep = " "

    for file in files: 
        file = TxFile(file, CONFIG)
        file.load_config()
        print(f"Name        : {file.name}")
        if file.from_:
            text = f"From        : {sep.join(file.from_)}"
            text = wrapper.wrap(text)
            for line in text:
                print(line)
        else:
            print(f"From        : No filter")
        if file.to:
            text = f"To          : {sep.join(file.to)}"
            text = wrapper.wrap(text)
            for line in text:
                print(line)
        else:
            print(f"To          : No filter")
        if file.datatypes:
            print(f"Datatypes   : {sep.join(file.datatypes)}")
        else:
            print(f"Datatypes   : No filter")
        if file.methods:
            print(f"Methods     : {sep.join(file.methods)}")
        else:
            print(f"Methods     : No filter")
        if file.params:
            print(f"Params      : {sep.join(file.params)}")
        else:
            print(f"Params      : No filter")
        if file.columns:
            print(f"Columns     : {sep.join(file.columns)}")
        print(f"Firstblock  : {file.firstblock}")
        print(f"Lastblock   : {file.lastblock}")
        print(f"Transactions: {file.transactions}")
        print("")


def proceed() -> bool:
    """
    Ask to prooceed with extraction or not
    """
    while True:
        response = input("::Proocced with extraction ([y]/n)?")
        if response in ["Y", "y", ""]:
            return True
        elif response in ["N", "n"]:
            return False
        else:
            continue
        

class GracefulExiter():

    def __init__(self):
        self.state = False
        signal.signal(signal.SIGINT, self.change_state)

    def change_state(self, signum, frame):
        self.state = True

    def exit(self):
        return self.state


class ProgressTracker(Thread):

    def __init__(self, start_block, end_block, report_interval = 60):
        Thread.__init__(self)
        self.start_time = time.time()
        self.start_block = start_block
        self.end_block = end_block
        self.block_counter = 0
        self.transaction_counter = 0
        self.blockheight_last_report = start_block
        self.report_interval = report_interval


    def run(self):
        while True:
            time.sleep(self.report_interval)
            self.report_progress()
            self.blockheight_last_report = self.block_counter


    def report_progress(self):
        print("Progress report")
        print("---------------")
        print(f"Runtime:                           {self.runtime()}")
        print(f"Speed:                             {self.speed()} b/s")
        print(f"Blocks:                            {self.block_counter}/{self.end_block}  ")
        print(f"Transactions:                      {self.transaction_counter}  ")
        print(f"Eta:                               {self.eta()}  ")


    def report_summary(self):
        print("End report")
        print("---------------")
        print(f"Finnished in:                      {self.runtime()}")
        print(f"Blocks processed:                  {self.start_block}-{self.start_block + self.block_counter}")
        print(f"Transactions written to file:      {self.transaction_counter}")


    def runtime(self):
        return datetime.timedelta(seconds(time.time() - self.start_time))


    def speed(self):
        return round((self.block_counter - self.blockheight_last_report) / 60)


    def eta(self):
        return datetime.timedelta(seconds(args.last-block - self.block_counter) / self.speed())


# Check if file was entered as argument.
# Return list.
class CustomAction1(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values[0].endswith(".txt"):
            values_2 = []
            with open(values[0], 'r') as fileobj:
                for line in fileobj:
                    values_2.append(line.rstrip())
            setattr(namespace, self.dest, values_2)
        else:
            setattr(namespace, self.dest, values)


# Convert argument to set
class CustomAction2(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, set(values))


if __name__ == '__main__':
    main()
