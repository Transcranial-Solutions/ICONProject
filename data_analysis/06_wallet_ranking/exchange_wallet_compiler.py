import pandas as pd
from pathlib import Path
from tqdm import tqdm

analysis_path = Path('.').resolve()

projectPath = analysis_path.joinpath('06_wallet_ranking')
dataPath = projectPath.joinpath('results')

# partial_match = ['2023_01', '2023_02', '2023_03']
# subfolders = [subfolder for subfolder in dataPath.iterdir() if (any(match_str in subfolder.name for match_str in partial_match))]

partial_match = ['2021_', '2022_', '2023_']
subfolders = [subfolder for subfolder in dataPath.iterdir() if (any(match_str in subfolder.name for match_str in partial_match))]

all_df = []
for subfolder in tqdm(subfolders, desc="Doing each folder", colour='magenta', position=0, leave=True):

    try:
        data_file = [f for f in subfolder.glob('*.csv') if f.is_file()][0]
    except:
        pass
    exchange_df = pd.read_csv(data_file)
    exchange_df['date'] = subfolder.stem

    exchange_df = exchange_df.drop(['staking_but_not_delegating'], axis=1, errors='ignore')

    all_df.append(exchange_df)

all_df = pd.concat(all_df).reset_index(drop=True)
all_df.to_csv(dataPath.joinpath('exchange_data_compiled.csv'))