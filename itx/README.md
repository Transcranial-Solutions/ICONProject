# ITX - Icon Transaction Extractor

## About
This is a command line tool for extracting transactions from ICON blockchain using filtering rules. You create files and for each file you specify extraction rules. Transaction which fulfills the rules of a particular file will be written to that file.

More advanced features will be added when a basic working product has been achieved.

Ideas for future features:
 - Syncronize and keep previously written csv files up to date with the current block height.
 - Option to use RPC to extract transactions instead of a local leveldb database.
 - Option to output results as sqlite databases instead of csv files.


## Limitations
The first version will require a local copy of the blockchain database. This can be achived in two ways. Either you need to run a citizen node on you local machine, or you could download a snapshot of the blockchain (maintained by Icon Foundation). The first option would be more suitable if you would like to keep your extracted transaction data up to date over time. The second option would suffice if you just want some transaction data up to the current point in time.


## Usage
## Step 1 - Accuire a local copy of the blockchain database.

Here you have two options. Either set up a local citizen node or download a snapshot of the blockchain.

# Option 1 - run local citizen node
Follow the instruction on https://www.icondev.io/docs/quickstart. Note that a citizen node does not requirem P-rep registartion.

# Option 2 - download a snapshot
1. Copy the latest snapshot from this list of snapshots: https://s3.ap-northeast-2.amazonaws.com/icon-leveldb-backup/MainctzNet/backup_list

2. Replace "backup_list" in the previous url with the snapshot you copied.

3. Paste the url into your browser or use a tool such as wget to download.