
from pathlib import Path
import pandas as pd
import numpy as np
import json

project_path = Path.cwd()
project_path = Path('E:/GitHub/ICONProject/data_analysis/08_transaction_data')
results_path = project_path.joinpath('results')

summary_file = 'compiled_tx_summary.csv'
df = pd.read_csv(results_path.joinpath(summary_file), low_memory=False)

df = df[df['to'].notna()]

df = df[~df['to'].str.startswith('hx')]


df = df.groupby('to')['group'].value_counts().reset_index().drop(columns=['count'])
df = df[df['group'] != 'System']
df = df.reset_index(drop=True)

# wrong historic data
df = df[~((df['to'] == 'cxa0af3165c08318e988cb30993b3048335b94af6c') & (df['group'] == 'Optimus'))]

new_row = pd.DataFrame({'to': ['cx21e94c08c03daee80c25d8ee3ea22a20786ec231',
                               'cx8dc674ce709c518bf1a6058a2722a59f94b6fb7f',
                               'cxe0252e6c1fe4040412811d83d13979e335287e45',
                               'cxa07f426062a1384bdd762afa6a87d123fbc81c75'
                               ], 
                        'group': ['balanced_router',
                                  'balanced_staked_lp',
                                  'balanced_boosted_baln',
                                  'xCall',
                                  
                                  ]
                        })

df = pd.concat([df, new_row], ignore_index=True)


# grouping
df['group'] = np.where(df['group'].str.lower().str.startswith('btp'), 'BTP', df['group'])
df['group'] = np.where(df['group'].str.contains('baln', case=False), 'Balanced', df['group'])
df['group'] = np.where(df['group'].str.lower().str.startswith('balanced'), 'Balanced', df['group'])
df['group'] = np.where(df['group'].str.contains('Blobble'), 'Blobble', df['group'])
df['group'] = np.where(df['group'].str.contains('CODE METAL', case=False), 'CODE METAL', df['group'])
df['group'] = np.where(df['group'].str.startswith('CPF'), 'CPF', df['group'])
df['group'] = np.where(df['group'].str.startswith('CPS'), 'CPS', df['group'])
df['group'] = np.where(df['group'].str.startswith('Gang'), 'GangstaBet', df['group'])
df['group'] = np.where(df['group'].str.startswith('Inanis'), 'Inanis', df['group'])
df['group'] = np.where(df['group'].str.contains('Karma', case=False), 'Karma', df['group'])
df['group'] = np.where(df['group'].str.lower().str.startswith('optimus'), 'Optimus', df['group'])
df['group'] = np.where(df['group'].str.contains('FRAMD', case=False), 'FRAMD', df['group'])
df['group'] = np.where(df['group'].str.lower().str.startswith('iDoge'), 'iDoge', df['group'])


# Identify 'to' values that have multiple groups, excluding 'unknown_cx'
valid_to_values = df[df['group'] != 'unknown_cx']['to'].value_counts()
valid_to_values = valid_to_values[valid_to_values > 1].index.tolist()

# Filter out rows where 'group' is 'unknown_cx' but 'to' is not in the list of valid 'to' values
df_filtered = df[~((df['group'] == 'unknown_cx') & (~df['to'].isin(valid_to_values)))]



group_dict = df.set_index('to').to_dict()
group_dict = group_dict.get('group')


group_dict = dict(sorted(group_dict.items(), key=lambda item: item[1]))

# Specify the filename
filename = project_path.joinpath('contract_grouping.json')

# Writing JSON data
with open(filename, 'w') as file:
    json.dump(group_dict, file)

print(f"Data has been saved to {filename}")