import argparse
from blockchain import Block
from blockchain import Transaction
import plyvel
import signal

        

def main():

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
    print(args)
    
    
    

    ## TODO
    ## Create csv files --> list of fileobjects?
    
    # Loop through all blocks and extract transactions according to filter criteria
    blocks = range(args.first-block, args.last-block + 1)
    counter = 0
    for block in blocks:
        block = Block(block)
        filtered_transactions = block.filter_transactions(block.transaction_list, **args.filter)

        ## Create and 

    ## Process blocks
    #counter = 0
    #while True:
    #    try:
    #        ## TODO
    #    except KeyError:
    #        ## TODO
    #        print(f"All blocks processed. Last block: {}", counter)
    #        break
        
            



def write_to_csv():
    ## TODO
    pass

def report_progress():
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



if __name__ == '__main__':
    main()