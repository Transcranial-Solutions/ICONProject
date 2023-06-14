#########################################################################
## Project: Update seed_addr with top 22 P-Rep                         ##
## Date: Jan 2022                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

import pandas as pd
import os
import requests
import json
import random

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

currPath = os.getcwd()
projectPath = os.path.join(currPath, "06_wallet_ranking")
if not os.path.exists(projectPath):
    os.mkdir(projectPath)

jsonPath = "I:\\IconNode\\data\\1\\"


## Seed Addresses ##
url = "http://18.118.232.28:9000"
cid = requests.get(f"{url}/admin/chain").json()[0]["cid"]
seed_dict = requests.get(f"{url}/admin/chain/{cid}").json()
p2p_ifo = seed_dict["module"]["network"]["p2p"]
merged_p2p_info = {**p2p_ifo["roots"], **p2p_ifo["seeds"]}
address_dict = {}
for node_address, ip_address in zip(merged_p2p_info.values(), merged_p2p_info.keys()):
    if ":" in ip_address:
        ip_address = ip_address.split(":")[0]
    address_dict[node_address] = ip_address
# print(address_dict)

top_22_ip = list(address_dict.values())[0:21]
top_22_ip = [s + ':7100' for s in top_22_ip]

# including random shuffling
random.shuffle(top_22_ip)

top_22_ip_str = ','.join(top_22_ip)


with open(jsonPath + 'config.json') as f:
    nodejson = json.load(f)

nodejson['seed_addr'] = top_22_ip_str

with open(jsonPath + 'config.json', 'w') as f:
    json.dump(nodejson, f)
