call python E:\GitHub\ICONProject\data_analysis\10_token_transfer\token_transfer.py

wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/block_extraction.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_extraction_ctznode.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_analysis_ctznode.py
wsl.exe ~/IconProject/.analysis/bin/python ~/IconProject/transaction_data/transaction_analysis_ctznode_weekly.py

pause