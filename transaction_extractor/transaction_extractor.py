import argparse
from blockchain import Block
from blockchain import Transaction
import csv
import plyvel
import signal
import time
from threading import Timer, Thread
import datetime
import os
import sys


COLUMNS = []
INTERVAL = 30    

def main():
    
    #Get default arguments from file.
    config = configparser.ConfigParser()
    config.read('itx.ini')
    df_args = dict(config['defaults'])


    # Create parser object.
    parser = argparse.ArgumentParser(prog = "itx",
                                     usage = "python3 itx <operation> [options] [target]",
                                     description = "Tool for extracting transactions from " 
                                                   "ICON blockchain using filter criterias. "
                                                   "Transactions of different filtercriteria will "
                                                   "be written to seperate .csv files.",
                                     add_help = True)

    parser.add_argument('--help', '-h', action = "store_true")
    

    # Add subparser to main parser object.
    subparsers = parser.add_subparsers(title = "operations",
                                       description = "BESKRIVNING AV OPERATORS",
                                       prog = "python3",
                                       metavar = "operations available",
                                       add_help = True)


    # Create parser for extract.
    parser_extract = subparsers.add_parser('extract',
                                            usage = 'python3 itx.py extract [options]',
                                            help = 'Extract transactions.',
                                            add_help = True)
    
    parser_extract.add_argument('--blocks', metavar = '<start> <end>', type = int, nargs = 2,
                                help = "Blocks to extract transactions from"))

    parser_extract.add_argument('--leveldb', metavar = '<path>', type = str, nargs = 1,
                                help = "Blocks to extract transactions from"))
    
    parser_extract.add_argument('--filter', choices = ["delegation", "claimiscore", "staking", 'iconbet'], type=str, 
                                nargs = '+', help='Transactions to extract from the blockchain', default = None)
    
    parser_extract.add_argument('--output-type', choices = ["csv", "sqlite3"], type = str, default = df_args['output-type']

    parser_extract.add_argument('--output', metavar = 'path', type = str, nargs = 1, default = df_args['output'],
                                 help = 'Output folder')
    
    parser_extract.set_defaults(func = extract)

    
    # Create parser for update command.
    parser_update = subparsers.add_parser('update', 
                                          usage = "python3 itx update [options] file/s",
                                          help = 'Update files to specified block',
                                          add_help = True)


    # Create parser for syncronize command.
    parser_syncronize = subparsers.add_parser('syncronize',
                                              usage='python3 itx.py extract [options]',
                                              help='Keep files syncronized via RPC',
                                              add_help=True)


    # Create parser for status command.
    parser_status = subparsers.add_parser('status',
                                          usage='python3 itx.py status',
                                          help='Check status for all tracked files.',
                                          add_help=True)

    
    # Custom helpfile
    #if namespace.help:
    #	with open('help_file.txt', 'r') as f:
    #		content = f.read()
    #       print(content)

    args = parser.parse_args()


def extract(args):

    # Open local node database.
    plyveldb = plyvel.DB(args.leveldb, create_if_missing = False)


    # Thread to keep track of and report progress. Will report progress every 60 seconds. Exits when program exits.
    progress_tracker = ProgressTracker(args.blocks[0], args.blocks[1], interval = INTERVAL)
    progress_tracker.daemon = True


    if args.output-type == "csv":

        if not args.filter:
            
            # Check if file exists --> handle it.
            print("Making sure file does not exist...")
            file = output + "all_transactions.csv"

            if os.path.exists(file):
                
                while True:
                    reponse = input("File {file} already exists. Overwrite? (Y/n)")

                    if response in ["Y", "y"]:
                        os.remove(file)
                        break

                    elif reponse in ["N", "n"]:
                        print("Aborting program..")
                        sys.exit(1)

                    else:
                        continue


            # Open file
            all_transactions = open(file, 'a')
            writer = csv.writer(all_transactions)
            writer.writerow(COLUMNS)


            # Print summary before start of extraction
            print("\n")
            print(f"Blocks: {args.blocks[0]} {args.blocks[0]}")
            print(f"All transactions --> {file}")
            print("\n")
            
            while True:
                response = input("::Proocced with extraction ([y]/n)?")
                
                if response in ["Y", "y", ""]:
                    print("Starting extraction...")
                    break
                
                if response in ["N", "n"]:
                    sys.exit(2)


            # Start progresstrackerthread
            progress_tracker.start()


            # Extract transactions
            blocks = range(args.blocks[0], args.blocks[1] + 1)
            for block in blocks:  
                block = Block(block, plyveldb)
                transactions = block.extract_all_transactions(block.transaction_list)
                
                # Write transactions to csv files.
                for transaction in transactions:
                    writer.writerow(transaction)
                    progress_tracker.transaction_counter += 1        

                progress_tracker.block_counter += 1
                #Implement break here if ctrl-c pressed.

            # Close file
            all_transactions.close()

        
        # If filter specified
        if args.filter:


            # Handle case where files already exists.
            print("Making sure files does not exist...")

            for filename in args.filter:
                file = args.output + filename + ".csv"
                
                if os.path.exists(file):

                    while True:
                        response = input(f"File {file} already exists. Overwrite? (Y/n)")

                        if response in ["Y", "y"]:
                            os.remove(file)
                            break

                        elif reponse in ["N", "n"]:
                            print("Aborting program...")
                            sys.exit(1)

                        else:
                            continue


            # Keyword arguments to be passed to filtermethod in blockclass.
            filter_criteria = {}
            for criteria in args.filter:
                filter_criteria[criteria] = True


            # Create csv files. Store open fileobjects as values in a dictionary. Filtercriteria is key.
            file_objects = {}
            for criteria in args.filter:
                file_objects[criteria] = open(args.outout + criteria + ".csv", 'a')
                writer = csv.writer(file_objects['citeria'])
                writer.writerow(COLUMNS)


            # Print summary before start of extraction
            print("\n")
            print(f"Blocks: {args.blocks[0]} {args.blocks[0]}")
            
            for key, val in file_objects:
                print(f"{key} --> {val.name}") 
            print("\n")
            
            while True:
                response = input("::Proocced with extraction ([y]/n)?")
                if response in ["Y", "y", ""]:
                    print("Starting extraction...")
                    break
                if response in ["N", "n"]:
                    sys.exit(2)


            # Progress thread.
            progress_tracker.start()


            # Loop through all blocks and extract transactions according to filter criteria
            blocks = range(args.blocks[0], args.blocks[1] + 1)
            for block in blocks:
                block = Block(block, plyveldb)
                filtered_transactions = block.filter_transactions(block.transaction_list, **filter_criteria)


                # Write transactions to csv files.
                for key, val in filtered_transactions:
                    if val:
                        writer = csv.writer(fileobjects[key])
                        for transaction in val:
                            writer.writerow(transaction)
                            progress_tracker.transaction_counter += 1        
                    else:
                        continue
                progress_tracker.block_counter += 1
                #Implement break here if ctrl-c pressed.


            # Close files
            for fileobject in file_objects.values():
                fileobject.close()
    
    

def update():
    ## TODO


def syncronize():
    ## TODO
    pass


def status():
    ## TODO
    pass


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
        self.end_block = end-block
        self.block_counter = 0
        self.transaction_counter = 0
        self.blockheight_last_report = start_block
        self.report_interval = report_interval

    def run(self):
        while True:
            time.sleep(self.report_interval)
            self.report_progress()
            self.blockheight_last_report = self.block_counter


    def report_progress():
        print("Progress report")
        print("---------------")
        print(f"Runtime:       {self.runtime()}")
        print(f"Speed:         {self.speed()} b/s")
        print(f"Blocks:        {self.block_counter}/{self.end_block}  ")
        print(f"Transactions:  {self.transaction_counter}  ")
        print(f"Eta:           {self.eta()}  ")


    def runtime():
        return datetime.timedelta(seconds(time.time() - self.start_time))


    def speed():
        return round((self.block_counter - self.blockheight_last_report) / 60)


    def eta():
        return datetime.timedelta(seconds(args.last-block - self.block_counter) / self.speed())


"""Psuedocode extract function

Perhaps have "if not filter" and "if filter" as outermost if instead of "if csv option" when implemeting sqlite3.
This would result in less repeat of code.

if csv option:
    If not filter:
        1. Check if file exists --> handle.
        2. Create file.
        3. Write column.
        4. Repeat until last block:
              1. getblock.
              2. extract transaction.
              3. write transaction to file.
        5. close file.
        6. Write to config file last block processed for all files.
        7. Endreport.

    if filter:
        1. Check if files exists --> handle
        2. Create files.
        3. Write columns.
        4. Repeat until last block:
              1. getblock.
              2. extract transactions according to filter.
              3. write transaction to file.
        5. close files.
        6. Write to config file last block processed for all files.
        7. Endreport.

if sqlite3 option:
    If not filter:
        1. Check if file exists --> handle.
        2. Create file.
        3. Write column.
        4. Repeat until last block:
              1. getblock.
              2. extract transaction.
              3. write transaction to file.
        5. close file.
        6. Write to config file last block processed for all files.
        7. Endreport.

    if filter:
        1. Check if files exists --> handle
        2. Create files.
        3. Write columns.
        4. Repeat until last block:
              1. getblock.
              2. extract transactions according to filter.
              3. write transaction to file.
        5. close files.
        6. Write to config file last block processed for all files.
        7. Endreport.
"""


if __name__ == '__main__':
    main()
