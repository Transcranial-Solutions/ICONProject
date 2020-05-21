import requests
import json


HEADERS = {'content-type': 'application/json'}
URL = "https://ctz.solidwallet.io/api/v3" 


def get_block(blocknr):
    blocknr = hex(blocknr)
    payload = {
                "jsonrpc": '2.0',
                "id": '12',
                "method": "icx_getBlockByHeight",
                "params": {"height": blocknr}
              }
    response = requests.post(URL, data=json.dumps(payload), headers=HEADERS)
    response = response.json()
    return response


def get_transaction_result(txhash):
    payload = {
                "jsonrpc": "2.0",
                "method": "icx_getTransactionResult",
                "id": 1234,
                "params": {"txHash": txhash}
 	          }
    response = requests.post(URL, data=json.dumps(payload), headers=HEADERS)
    response = response.json()
    return response


def get_last_block():
    payload = {
                "jsonrpc": "2.0",
                "method": "icx_getLastBlock",
                "id": 1232
              }
    response = requests.post(URL, data=json.dumps(payload), headers=HEADERS)
    response = response.json()
    return response

def transaction_success(txhash):
    payload = {
                "jsonrpc": "2.0",
                "method": "icx_getTransactionResult",
                "id": 1234,
                "params": {"txHash": txhash}
 	          }
    
    response = requests.post(URL, data=json.dumps(payload), headers=HEADERS)
    response = response.json()
    txstatus = response['result']['status']

    if txstatus:
        return True
    else:
        return False