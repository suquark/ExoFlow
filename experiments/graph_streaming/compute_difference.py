import os
import numpy as np
import pandas as pd
import tqdm

from common import DUMP_OUTPUT_PATH, BATCH_UPDATE_INTERVAL, N_PARTITIONS

names = ["uid", "influence"]

for i in tqdm.trange(8, 18):
    n = i * BATCH_UPDATE_INTERVAL
    batch_update = os.path.join(DUMP_OUTPUT_PATH, f"batch_update_{n}.csv")
    bu_df = pd.read_csv(batch_update, names=names)
    online_dfs = []
    for j in range(N_PARTITIONS):
        online_dfs.append(
            pd.read_csv(
                os.path.join(DUMP_OUTPUT_PATH, f"online_update_{j}_{n}.csv"),
                names=names,
            )
        )
    ou_df = pd.concat(online_dfs)
    joined = bu_df.merge(ou_df, how="outer", on="uid").fillna(0)
    # print(bu_df.shape[0], ou_df.shape[0],
    #   np.unique(ou_df[['uid']].to_numpy().flatten()).shape)
    # print(bu_df.sort_values(by=['influence']))
    # print(ou_df.sort_values(by=['influence']))
    # print(joined)
    bu_infl = joined["influence_x"].to_numpy()
    ou_infl = joined["influence_y"].to_numpy()
    diff = np.mean(np.abs(bu_infl - ou_infl))
    with open("difference.txt", "a") as f:
        f.write(f"{diff}\n")
    print(diff)
