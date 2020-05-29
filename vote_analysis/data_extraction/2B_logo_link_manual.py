
#########################################################################
## Project: ICON Vote Data Visualisation                               ##
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
# need to extract table using html_table -- manual

import pandas as pd
import os
desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)

# making path for loading
currPath = os.getcwd()
inPath = os.path.join(currPath, "output")

# read logo file
logo_find_csv = pd.read_csv(os.path.join(inPath, 'logo_find_github.csv'))

# rename links to logo
logo_find_csv = logo_find_csv.rename(columns={"Links": "logo", "Name": "filename"})

# find those that are image extensions
logo_find = logo_find_csv[logo_find_csv['filename'].str.contains(".png|.jpg", na=False)].reset_index(drop=True)

# replace some strings for downloadable links
logo_find = logo_find.replace({'logo': r'github.com'}, {'logo': 'raw.githubusercontent.com'}, regex=True)
logo_find = logo_find.replace({'logo': r'blob/'}, {'logo': ''}, regex=True)


# read prep details
prep_df = pd.read_csv(os.path.join(inPath, 'icon_prep_details.csv'))

# get prep name and sort alphabetically
prep_name = prep_df.iloc[prep_df.name.str.lower().argsort()][['name']].reset_index(drop=True)

logo_find = logo_find.iloc[logo_find.filename.str.lower().argsort()].reset_index(drop=True)

logo_find = pd.concat([prep_name, logo_find], axis=1)

logo_find = logo_find.drop(columns=['filename'])

# save
logo_find.to_csv(os.path.join(inPath, 'logo_find_github_fixed.csv'), index=False)
