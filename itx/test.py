from blockchain import Block, Transaction
import plyvel
import json
import sys

def main():
    db = plyvel.DB("/home/ted/Iconnode/data/mainnet/.storage/db_31.208.165.65:7100_icon_dex", create_if_missing=False)
    i = 0
    while True:
        block = Block(int(sys.argv[1])+i, db)
        print(block.height)
        print(block.transactions)

        transactions = block.transactions
        if transactions:
            for number in range(len(transactions)):
                transaction = Transaction(transactions[number], db, blockheight = block.height, blocktimestamp = block.timestamp)
                print(transaction.was_successful())
                print(transaction.txhash)
                if transaction.fulfills_criteria(to = set(["cx0000000000000000000000000000000000000000"])):
                    sys.exit(0)
                print(transaction.value)
                print(transaction.method)
                print(transaction.get_transaction())
        i += 1





if __name__=='__main__':
    main()