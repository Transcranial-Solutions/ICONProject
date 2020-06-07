import argparse
from blockchain import Block
from blockchain import Transaction
import csv
import plyvel
import signal
import time
from threading import Timer, Thread
import datetime

        

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


    if args.extract:
        
    
    # Thread to keep track of and report progress. Will report progress every 60 seconds. Exits when program exits.
    progress_tracker = ProgressTracker(args.first-block, args.last-block, interval = 30)
    progress_tracker.daemon = True
    progress_tracker.start()
    
    
    # Keyword arguments to be passed to filtermethod in blockclass.
    filter_criteria = {}
    for criteria in args.filter:
        filter_criteria[criteria] = True
   
   
    # Create csv files. Store open fileobjects as values in a dictionary. Filtercriteria is key.
    file_objects = {}
    for criteria in arg.filter:
        file_objects[criteria] = open(args.outout + criteria + ".csv", 'a')
        writer = csv.writer(file_objects['citeria'])
        writer.writerow("[columns]")



    # Loop through all blocks and extract transactions according to filter criteria
    blocks = range(args.first-block, args.last-block + 1)
    for block in blocks:
        block = Block(block)
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

    
    # Close all fileobjects
    for val in fileobjects.values():
        val.close()
        


def write_to_csv():
    ## TODO
    pass



def syncronize():
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





if __name__ == '__main__':
    main()
