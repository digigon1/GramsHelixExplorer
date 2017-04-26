py download_csv.py &&^
py parse_files.py &&^
py join_inputs.py &&^
cd "wallets" &&^
py ../get_data.py &&^
cd "output" &&^
py ../../join_outputs.py &&^
py ../../sort_wallets.py &&^
py ../../match_wallets.py