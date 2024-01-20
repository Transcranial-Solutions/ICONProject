
from pathlib import Path
import pandas as pd
import numpy as np
import json

project_path = Path.cwd()
results_path = project_path.joinpath('results')

summary_file = 'compiled_tx_summary.csv'
df = pd.read_csv(results_path.joinpath(summary_file), low_memory=False)

df = df[df['to'].notna()]

df = df[~df['to'].str.startswith('hx')]


df = df.groupby('to')['group'].value_counts().reset_index().drop(columns=['count'])
df = df[df['group'] != 'System']
df = df.reset_index(drop=True)

df['group'] = np.where(df['group'].str.lower().str.startswith('btp'), 'BTP', df['group'])
df['group'] = np.where(df['group'].str.contains('baln', case=False), 'Balanced', df['group'])
df['group'] = np.where(df['group'].str.contains('Blobble'), 'Blobble', df['group'])
df['group'] = np.where(df['group'].str.contains('CODE METAL', case=False), 'CODE METAL', df['group'])
df['group'] = np.where(df['group'].str.startswith('CPF'), 'CPF', df['group'])
df['group'] = np.where(df['group'].str.startswith('CPS'), 'CPS', df['group'])
df['group'] = np.where(df['group'].str.startswith('Gang'), 'GangstaBet', df['group'])
df['group'] = np.where(df['group'].str.startswith('Inanis'), 'Inanis', df['group'])
df['group'] = np.where(df['group'].str.contains('Karma', case=False), 'Karma', df['group'])
df['group'] = np.where(df['group'].str.startswith('Optimus'), 'Optimus', df['group'])
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
filename = 'contract_grouping.json'

# Writing JSON data
with open(filename, 'w') as file:
    json.dump(group_dict, file)

print(f"Data has been saved to {filename}")