
#########################################################################
## Project: ICON Voting Behaviour                                      ##
## Date: May 2020                                                      ##
## Author: Tono / Sung Wook Chung (Transcranial Solutions)             ##
## transcranial.solutions@gmail.com                                    ##
##                                                                     ##
## This programme is distributed in the hope that it will be useful,   ##
## but WITHOUT ANY WARRANTY, without even the implied warranty of      ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the       ##
## GNU General Public License for more details.                        ##
#########################################################################

# getting the right logo link
# need github api token for this

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")

# import from github -- need token
from github.MainClass import Github
g = Github('PUT_YOUR_TOKEN_HERE')

repo = g.get_repo("Transcranial-Solutions/ICONProject")

# making proper logo web address & name using logo
file_list = repo.get_contents("vote_analysis/output/logos")
split_file_list = lambda x: str(x).split('"')[1]
file_list = list(map(split_file_list, file_list))

split_logo_list = lambda x: str(x).split('/')[-1].split('.')[0]
logo_list = list(map(split_logo_list, file_list))

github_dir_path = 'https://raw.githubusercontent.com/Transcranial-Solutions/ICONProject/master/'
web_list = [github_dir_path + s for s in file_list]

# dataframe - name by logo and web address
logo_df = pd.DataFrame({'name_by_logo': logo_list, 'logo': web_list})
logo_df = logo_df.iloc[logo_df.logo.str.lower().argsort()].reset_index(drop=True)

# load prep information data
prep_df = pd.read_csv(os.path.join(inPath, 'icon_prep_details.csv'))
prep_df = prep_df.iloc[prep_df.name.str.lower().argsort()].reset_index(drop=True)


## fuzzy matching logo name with prep name
# from fuzzywuzzy import fuzz
from fuzzywuzzy import process
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    s = df_2[key2].tolist()
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
    df_1['matches'] = m
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    return df_1

# fuzzy matched (high threshold)
fuzzy_df = fuzzy_merge(prep_df, logo_df, 'name', 'name_by_logo', threshold=90).drop(columns=['logo'])
fuzzy_df = fuzzy_df.rename(columns={'matches': 'name_by_logo'})

# check if match not successful
index_missing = fuzzy_df.index[fuzzy_df['name_by_logo'] == ''].tolist()
print(index_missing)

fuzzy_df[fuzzy_df.index.isin(index_missing)]
logo_df[logo_df.index.isin(index_missing)]

# replace with ordered value from prep_df
fuzzy_df.loc[fuzzy_df.index.isin(index_missing), 'name_by_logo'] = logo_df.loc[fuzzy_df.index.isin(index_missing), 'name_by_logo']

# merge back and remove unnecessary column
final_df = pd.merge(fuzzy_df, logo_df, how="left", on="name_by_logo").drop(columns=['name_by_logo'])

# save
final_df.to_csv(os.path.join(inPath, 'logo_find_github_fixed.csv'), index=False)
