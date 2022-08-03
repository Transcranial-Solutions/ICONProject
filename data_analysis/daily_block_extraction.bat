call python E:\GitHub\ICONProject\data_analysis\10_token_transfer\token_transfer_community.py

wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/block_extraction_community.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_extraction_ctznode.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_analysis_ctznode.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_analysis_ctznode_weekly.py

call python E:\GitHub\ICONProject\data_analysis\08_transaction_data\tx_summary_compiler.py

pause