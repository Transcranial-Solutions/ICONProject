#########################################################################
## Project: P-Rep vote status via from chain                           ##
## Date: February 2022                                                 ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# This is for 'ICON in Numbers' series.
# Daily snapshot of delegation for icon in numbers series.

# import library
import json
# import ast
import pandas as pd
import os
from typing import List
from iconsdk.builder.call_builder import CallBuilder
from iconsdk.icon_service import IconService
from iconsdk.providers.http_provider import HTTPProvider
from datetime import date, datetime, timedelta
import numpy as np
from typing import Union


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',10)

# making path for saving
currPath = '/home/tono/ICONProject/data_analysis/'
inPath = os.path.join(currPath, "output")
outDataPath = os.path.join(inPath, "prep_votes")
if not os.path.exists(outDataPath):
    os.mkdir(outDataPath)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

## value extraction function
def extract_values(obj, key):
  # Pull all values of specified key from nested JSON
  arr = []

  def extract(obj, arr, key):

    # Recursively search for values of key in JSON tree
    if isinstance(obj, dict):
      for k, v in obj.items():
        if isinstance(v, (dict, list)):
          extract(v, arr, key)
        elif k == key:
          arr.append(v)
    elif isinstance(obj, list):
      for item in obj:
        extract(item, arr, key)
    return arr

  results = extract(obj, arr, key)
  return results


URI = "https://ctz.solidwallet.io"
SYSTEM_ADDRESS = "cx0000000000000000000000000000000000000000"

today = datetime.utcnow() #- timedelta(days=1)
day_today = today.strftime("%Y-%m-%d")
day1 = today.strftime("%Y_%m_%d")
this_year = day_today[0:4]

class PRep(object):
  def __init__(self, rank: int, name: str, address: str, delegation: int, bond: int):
    self.rank = rank
    self.name = name
    self.address = address
    self.delegation = delegation
    self.bond = bond
    self.power = min(bond * 20, delegation + bond)

  def bond_ratio(self) -> float:
    if self.bond == 0:
      return 0
    else:
      return self.bond * 100 / (self.delegation + self.bond)

  # def __repr__(self):
  #   return "{\n" \
  #          f"\trank: {self.rank}\n\tname: {self.name}\n\taddress: {self.address}\n" \
  #          f"\tdelegation:\t{self.delegation:-30d}\n\tbond:\t\t{self.bond:-30d}\n\tpower:\t\t{self.power:-30d}\n" \
  #          f"\tbond ratio: {self.bond_ratio()}\n" \
  #          "}"

  # def __str__(self):
  #   return json.dumps(dict(self), ensure_ascii=False)

  def __repr__(self):
    # return "{"f"'rank': {self.rank}, 'name': {self.name}, 'address': {self.address}, 'delegation':{self.delegation:-30d}, 'bond':{self.bond:-30d}, 'power':{self.power:-30d}, 'bond ratio': {self.bond_ratio()}" "}"
    return "{"f"rank: {self.rank}, name: {self.name}, address: {self.address}, delegation:{self.delegation:-30d}, bond:{self.bond:-30d}, power:{self.power:-30d}, bond ratio: {self.bond_ratio()}" "}"

class PRepBond(object):
  def __init__(self):
    self.icon_service = IconService(HTTPProvider(URI, 3))
    self.main_preps: List[PRep] = []
    self.sub_preps: List[PRep] = []
    self.all_preps: List[PRep] = []
    self.candidate_preps: List[PRep] = []
    self.bonded_preps: List[PRep] = []

  def _query(self, method: str, params: dict = {}, to: str = SYSTEM_ADDRESS):
    call = CallBuilder() \
      .to(to) \
      .method(method) \
      .params(params) \
      .build()
    return self.icon_service.call(call)

  def get_preps_df(self):
    main_prep_address = extract_values(self._query("getMainPReps")["preps"], 'address')
    sub_preps_address = extract_values(self._query("getSubPReps")["preps"], 'address')
    all_preps_address = extract_values(self._query("getPReps")["preps"], 'address')
    candidates_address = set(all_preps_address).difference(set(main_prep_address)).difference(set(sub_preps_address))
    
    # candidate_preps = [i for i in all_preps if i['address'] in candidates_address]
    
    dict_main_prep_address = {i:'main' for i in main_prep_address}
    dict_sub_prep_address = {i:'sub' for i in sub_preps_address}
    dict_candidate_address = {i:'candidate' for i in candidates_address}
    
    dict_all_preps_address = {**dict_main_prep_address,
                              **dict_sub_prep_address,
                              **dict_candidate_address}
    
    df_all_preps_address = pd.DataFrame(dict_all_preps_address.items(),
                                        columns=['address','prep_type'])
    return df_all_preps_address
    
    

  def run(self):
    # self.main_preps.clear()
    # self.sub_preps.clear()
    self.all_preps.clear()
    # self.candidate_preps.clear()
    
    # main_preps = self._query("getMainPReps")["preps"]
    # sub_preps = self._query("getSubPReps")["preps"]
    self.all_preps = self._query("getPReps")["preps"]
    
    # main_prep_address = extract_values(main_preps, 'address')
    # sub_preps_address = extract_values(sub_preps, 'address')
    # all_preps_address = extract_values(all_preps, 'address')
    # candidates_address = set(all_preps_address).difference(set(main_prep_address)).difference(set(sub_preps_address))
    
    # candidate_preps = [i for i in all_preps if i['address'] in candidates_address]

    # for rank, prep in enumerate(main_preps):
    #   resp = self._query(method="getPRep", params={"address": prep["address"]})
    #   p = PRep(rank + 1, prep["name"], prep["address"], int(resp["delegated"], 0), int(resp["bonded"], 16))
    #   self.main_preps.append(p)

    # offset = len(self.main_preps) + 1
    # for rank, prep in enumerate(sub_preps):
    #   resp = self._query(method="getPRep", params={"address": prep["address"]})
    #   p = PRep(offset + rank, prep["name"], prep["address"], int(resp["delegated"], 0), int(resp["bonded"], 16))
    #   self.sub_preps.append(p)
      
    # offset2 = len(self.main_preps) + len(self.sub_preps) + 1
    # for rank, prep in enumerate(candidate_preps):
    #   resp = self._query(method="getPRep", params={"address": prep["address"]})
    #   p = PRep(offset2 + rank, prep["name"], prep["address"], int(resp["delegated"], 0), int(resp["bonded"], 16))
    #   self.candidate_preps.append(p)

    # self.bonded_preps.extend(self.main_preps)
    # self.bonded_preps.extend(self.sub_preps)
    # self.bonded_preps.extend(self.candidate_preps)
    # self.bonded_preps.sort(key=lambda prep: prep.power, reverse=True)
    self.bonded_preps.extend(self.all_preps)

  # def report(self):
  #   print("### Main P-Reps bond status")
  #   self.summary(self.main_preps)
  #   print()
  #   print("### Sub P-Reps bond status")
  #   self.summary(self.sub_preps)
  #   print()
  #   print("### Candidate P-Reps bond status")
  #   self.summary(self.candidate_preps)
  #   print()
  #   print("### New P-Reps sorted by power")
  #   for prep in self.bonded_preps:
  #     print(prep)

  # @staticmethod
  # def summary(preps: list):
  #   zero_bond: List[PRep] = []
  #   for prep in preps:
  #     if prep.bond == 0:
  #       zero_bond.append(prep)
  #   print(f"Bond: {len(preps) - len(zero_bond)}, Non-Bond: {len(zero_bond)}")
  #   print(f"Following {len(zero_bond)} P-Reps need to bond")
  #   for prep in zero_bond:
  #     print(prep)

class MyEncoder(json.JSONEncoder):
  def default(self, obj):
    return obj.__dict__

if __name__ == '__main__':
  p = PRepBond()
  p.run()

  def loop_to_icx(loop):
    icx = loop / 1000000000000000000
    return (icx)

  def votes_and_bond():
    prep_df = p.get_preps_df()
    my_list = p.bonded_preps
    my_dict = MyEncoder().encode(my_list)
    my_dict = json.loads(my_dict)
    df = pd.DataFrame(my_dict)

    df['delegation'] = loop_to_icx(df['delegated'].apply(int, base=0))
    df['bond'] = loop_to_icx(df['bonded'].apply(int, base=16))
    df['power'] = loop_to_icx(df['power'].apply(int, base=0))
    df['irep'] = loop_to_icx(df['irep'].apply(int, base=0))

    df['grade'] = df['grade'].apply(int, base=0)
    df['status'] = df['status'].apply(int, base=0)
    df['penalty'] = df['penalty'].apply(int, base=0)
    df['irepUpdateBlockHeight'] = df['irepUpdateBlockHeight'].apply(int, base=0)
    df['lastHeight'] = df['lastHeight'].apply(int, base=0)
    df['totalBlocks'] = df['totalBlocks'].apply(int, base=0)
    df['validatedBlocks'] = df['validatedBlocks'].apply(int, base=0)
    
    df = pd.merge(df, prep_df, how='outer', on='address')
    df = df.sort_values(by=['power','delegation','name'], ascending=False)
    df['rank'] = df.index+1
    return df

  df = votes_and_bond()
  

  # saving data
  savePath = os.path.join(outDataPath, this_year)
  df.to_csv(os.path.join(savePath, 'prep_top_100_votes_and_bond_' + day1 + '.csv'), index=False)

  # loading data to merge with prep votes
  # prep_votes = pd.read_csv(os.path.join(outDataPath, 'prep_votes_' + day1 + '.csv'))

  # merged_df = pd.merge(prep_votes, df, how='left', left_on='validator', right_on='address').drop(columns=['name','address'])
  # merged_df['delegation'] = np.where(merged_df['delegation'].isnull(), merged_df['cum_votes_update'], merged_df['delegation'])
  # merged_df = merged_df.fillna(0)
  # merged_df = merged_df.sort_values(by=['bond', 'delegation'], ascending=False)
  # merged_df['rank'] = np.arange(len(merged_df)) + 1
  # merged_df = merged_df.drop(columns=['cum_votes_update'])

  # merged_df.to_csv(os.path.join(outDataPath, 'prep_votes_final_' + day1 + '.csv'), index=False)

  print(' ### Done getting P-Rep vote information. ### ')
