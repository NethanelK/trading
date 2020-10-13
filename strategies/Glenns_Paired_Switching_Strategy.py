import json
import pandas as pd
import numpy as np
from datetime import datetime

# Glenn's Paired Switching Strategy:


tic1 = ""
tic2 = ""


def set_label(r):
    if r[f"3_month_{tic1}_change"] < r[f"3_month_{tic2}_change"]:
        return tic2
    elif r[f"3_month_{tic2}_change"] < r[f"3_month_{tic1}_change"]:
        return tic1
    return tic1


def get_return(r):
    if r.ETF_this_month == tic1:
        return r[f'per_change_{tic1}']
    elif r.ETF_this_month == tic2:
        return r[f'per_change_{tic2}']
    return 0


def get_return_adj(r):
    if r.ETF_this_month == tic1:
        return r[f'per_change_{tic1}_adj']
    elif r.ETF_this_month == tic2:
        return r[f'per_change_{tic2}_adj']
    return 0


def set_strategey(path):
    df1 = pd.read_csv(f"data/{tic1}.txt")
    df2 = pd.read_csv(f"data/{tic2}.txt")

    df = pd.merge(df1, df2, on="date", how="left")

    df[f"3_month_{tic1}"] = df[tic1].shift(3)
    df[f"3_month_{tic2}"] = df[tic2].shift(3)

    df[f"3_month_{tic1}_change"] = (df[tic1] / df[f"3_month_{tic1}"]) - 1
    df[f"3_month_{tic2}_change"] = (df[tic2] / df[f"3_month_{tic2}"]) - 1

    df[f'per_change_{tic1}'] = df[tic1].pct_change()
    df[f'per_change_{tic2}'] = df[tic2].pct_change()

    df[f'per_change_{tic1}_adj'] = df[f'adj_close_{tic1}'].pct_change()
    df[f'per_change_{tic2}_adj'] = df[f'adj_close_{tic2}'].pct_change()
    df1.pop(f'adj_close_{tic1}')
    df2.pop(f'adj_close_{tic2}')

    df_test = pd.DataFrame()
    df_test[f"3_month_{tic1}_change"] = df[f"3_month_{tic1}_change"]
    df_test[f"3_month_{tic2}_change"] = df[f"3_month_{tic2}_change"]
    df_test['ETF_next_month'] = df_test.apply(set_label, axis=1)
    df["ETF_next_month"] = df_test['ETF_next_month']
    df["ETF_this_month"] = df['ETF_next_month'].shift(1)

    df_test["ETF_this_month"] = df["ETF_this_month"]

    df_test[f'per_change_{tic1}'] = df[f'per_change_{tic1}']
    df_test[f'per_change_{tic2}'] = df[f'per_change_{tic2}']

    df_test[f'per_change_{tic1}_adj'] = df[f'per_change_{tic1}_adj']
    df_test[f'per_change_{tic2}_adj'] = df[f'per_change_{tic2}_adj']
    df_test['return'] = df_test.apply(get_return, axis=1)
    df_test['return_adj'] = df_test.apply(get_return_adj, axis=1)
    df["return_adj"] = df_test['return_adj']
    df["return"] = df_test['return']

    df.to_csv(f"{path}/data_after_processing/GPS.txt", index=False)
    df = df[["date", "ETF_this_month", "ETF_next_month", "return", "return_adj"]]
    df.columns = ["date", "ETF_this_month_GPS",
                  "ETF_next_month_GPS", "return_GPS", "return_adj_GPS"]
    df.to_csv(f"{path}/results/GPS.txt", index=False)

    # arr = df.values[-1].tolist()
    # t1 = arr[1]
    # n1 = arr[2]
    # return_all = arr[3]

    print("Glenn's Paired Switching Strategy is ready")

    # dict_data = {"ETFS_this_month": [t1], "ETFS_next_month": [
    #     n1], "this_month_return": return_all}
    # return dict_data


def build_strategey(ticks, path):
    global tic1, tic2
    tic1 = ticks[0]
    tic2 = ticks[1]
    return set_strategey(path)
