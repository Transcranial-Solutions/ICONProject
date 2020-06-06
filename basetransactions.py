import csv
import os
import requests
import json
from datetime import datetime
import pytz


# RPC settings
HEADERS = {'content-type': 'application/json'}
URL = "http://95.179.230.6:9000/api/v3"

# Constants
STARTBLOCK = 10364000       # ~ date for decentralization
INTERVAL = 43200            # ~ block in one day
COLUMNS = ("block", "irep", "rrep", "totaldelegation", "value", "timestamp")
LOOP = 1000000000000000000  # one icx

def main():

    # Create a new file
    if os.path.exists("basetransactions.csv"):
         os.remove("basetransactions.csv")
   
    fileobj = open("basetransactions.csv", 'a')
    writer = csv.writer(fileobj)
    writer.writerow(COLUMNS)

    # Loop through blocks at specified interval
    counter = STARTBLOCK
    while True:
        try:
            block = get_block(counter)
        except KeyError:
            print(f"Could not find block {counter}. End of blockchain reached?")
            break

        base_tx_data = block['confirmed_transaction_list'][0]['data']
        
        # Conversions done here if you want to edit.
        row = (block['height'],
               round(int(base_tx_data['prep']['irep'], 16)) / LOOP,
               int(base_tx_data['prep']['rrep'], 16) / LOOP,
               int(base_tx_data['prep']['totalDelegation'], 16) / LOOP,
               int(base_tx_data['prep']['value'], 16) / LOOP,
               datetime.fromtimestamp(block['time_stamp']/1000000, tz = pytz.timezone('Europe/Helsinki')))
        
        writer.writerow(row)
        print(f"Block {counter} processed.")
        counter = counter + INTERVAL

    fileobj.close()
    print(f"Last processed block: {counter - INTERVAL}.")
        


def get_block(blocknr):
    blocknr = hex(blocknr)
    payload = {
                "jsonrpc": '2.0',
                "id": '12',
                "method": "icx_getBlockByHeight",
                "params": {"height": blocknr}
              }
    response = requests.post(URL, data=json.dumps(payload), headers=HEADERS)
    response = response.json()['result']
    return response


if __name__ == '__main__':
    main()