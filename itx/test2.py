from blockchain import Block, Transaction
import plyvel
import json
import sys

def main():
    print("First")
    db = plyvel.DB("/home/ted/Iconnode/data/mainnet/.storage/db_31.208.165.65:7100_icon_dex", create_if_missing=False)
    print(Block())
    print(Block(0, db).find_last_block())


if __name__=='__main__':
    main()