import argparse
from blockchain import Block
from blockchain import Transaction
import plyvel
import signal
import time
from threading import Timer
import datetime

        

def main():
    # Handle command line arguments.
    parser = argparse.ArgumentParser(description="Tool for extracting transactions from " 
                                                  "ICON blockchain using filter criterias. "
                                                  "Transactions of different filtercriteria will "
                                                  "be written to seperate .csv files.")
    
    parser.add_argument('--leveldb', metavar = 'path', type = str, nargs = 1,
                        help='Path to your local node\'s leveldb.')
    parser.add_argument('--first-block', metavar = 'start', type = int, nargs = 1,
                        help='First block to extract transactions from')
    parser.add_argument('--last-block', metavar = 'end', type = int, nargs = 1,
                        help='Last block to extract transactions from')
    parser.add_argument('--filter', choices = ["delegation", "claimiscore", "staking", 'iconbet'], type=str, 
                        nargs = '+', help='Transactions to extract from the blockchain')
    parser.add_argument('--output', metavar = 'path', type = str, nargs = 1, default = "./output/",
                        help = 'Output folder')
    parser.add_argument('--syncronize', action = 'store_true',
                        help = "Syncronize previously written csv files up to current block height")
     args = parser.parse_args()

    
    # Keep track of and report progress  Idea --> maybe make a class to handle this?
    start_time = time.time()
    block_counter = 0
    transaction_counter = 0
    last_progress_report = 0
    timer = Time(60, report_progress)

    # Keyword arguments to be passed to filtermethod in blockclass.
    filter_criteria = {}
    for criteria in args.filter:
        filter_criteria[criteria] = True
   
   
    # Create csv files --> list of fileobjects?
    ## TODO
    
    


    # Loop through all blocks and extract transactions according to filter criteria
    blocks = range(args.first-block, args.last-block + 1)
    counter = 0
    for block in blocks:
        block = Block(block)
        filtered_transactions = block.filter_transactions(block.transaction_list, **filter_criteria)

        # Write transactions to csv files.
        # TODO


def write_to_csv():
    ## TODO
    pass

def report_progress():
    runtime = time.time() - start_time
    speed = (block_counter - lastblock) / 60
    eta = ((args.last-block - block_counter) / speed)

    print("Progress report")
    print("---------------")
    print(f"Runtime:       {datetime.timedelta(seconds(runtime)}")
    print(f"Speed:         {speed} b/s")
    print(f"Blocks:        {block_counter}/{args.last-block}  ")
    print(f"Transactions:  {transaction_counter}  ")
    print(f"Eta:           {datetime.timedelta(seconds(eta)}  ")

    last_progress_report = block_counter
    timer = Time(60, report_progress)

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



if __name__ == '__main__':
    main()