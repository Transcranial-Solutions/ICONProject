# ITX - Icon Transaction Extractor

## About
This is a tool for extracting transactions from ICON blockchain using filter criterias. Filters could be anything from staking, delegation, claimiscore or transactions associated with a particular decentralized application such as Iconbet. Transactions belonging to different filtering criteria will be written to separate csv files.

More advanced features will be added when a basic working product has been achieved.

Ideas for future features:
 - Syncronize and keep previously written csv files up to date with the current block height.
 - Option to use RPC to extract transactions instead of a local leveldb database.
 - Output results as sqlite databases instead of csv files.

