# ITX - Icon Transaction Extractor

## About
This is a command line tool for extracting transactions from ICON blockchain using filter criterias. Filters could be anything from staking, delegation, claimiscore or transactions associated with a particular decentralized application such as Iconbet. Transactions belonging to different filtering criteria will be written to separate csv files.

More advanced features will be added when a basic working product has been achieved.

Ideas for future features:
 - Syncronize and keep previously written csv files up to date with the current block height.
 - Option to use RPC to extract transactions instead of a local leveldb database.
 - Option to output results as sqlite databases instead of csv files.


## Limitations
The first version will require a local copy of the blockchain database. You could achive this in two ways. Either you need to run a citizen node on you local machine, or you could download a snapshot of the blockchain (maintained by Icon Foundation). The first option would be more suitable if you would like to keep your extracted transaction data up to data over time. The second option would suffice if you just want some transaction data up to the current point in time.
