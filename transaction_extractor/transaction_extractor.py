import argparse
from blockchain import Block
from blockchain import Transaction
import plyvel
import signal
import time
from threading import Timer, Thread
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

    
    # Thread to keep track of and report progress. Will report progress every 60 seconds. Exits when program exits.
    progress_tracker = ProgressTracker(args.first-block, args.last-block, interval = 60)
    progress_tracker.daemon = True
    progress_tracker.start()
    

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
            time.sleep(60)
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
