import json
import pandas as pd
import numpy as np
from datetime import datetime
import os


working_tic1 = ""
working_tic2 = ""

tic1 = ""
tic2 = ""

tic3 = ""
tic4 = ""

tic5 = ""
tic6 = ""


def set_label(r):
    if r[working_tic1] > r[f"6_month_{working_tic1}_max"]:
        return working_tic1
    elif r[working_tic1] < r[f"12_month_{working_tic1}_low"]:
        return working_tic2
    else:
        return None


def set_label_tick_5(r):
    if r[working_tic1] < r[f"6_month_{working_tic1}_low"]:
        return working_tic2
    elif r[working_tic1] > r[f"12_month_{working_tic1}_max"]:
        return working_tic1
    else:
        return None


def get_return(r):
    if r.ETF_this_month == working_tic1:
        return r[f'per_change_{working_tic1}']
    elif r.ETF_this_month == working_tic2:
        return r[f'per_change_{working_tic2}']
    return 0


def get_return_adj(r):
    if r.ETF_this_month == working_tic1:
        return r[f'per_change_{working_tic1}_adj']
    elif r.ETF_this_month == working_tic2:
        return r[f'per_change_{working_tic2}_adj']
    return 0


def set_strategey(path):
    global working_tic1, working_tic2
    working_tic1 = tic1
    working_tic2 = tic2

    for i in range(2):
        df1 = pd.read_csv(f"data/{working_tic1}.txt")
        df2 = pd.read_csv(f"data/{working_tic2}.txt")

        df1[f'per_change_{working_tic1}_adj'] = df1[f'adj_close_{working_tic1}'].pct_change(
        )
        df2[f'per_change_{working_tic2}_adj'] = df2[f'adj_close_{working_tic2}'].pct_change(
        )
        df1.pop(f'adj_close_{working_tic1}')
        df2.pop(f'adj_close_{working_tic2}')

        df = pd.merge(df1, df2, on="date", how="left")

        df[f"12_month_{working_tic1}_low"] = df[working_tic1].rolling(
            12).min().shift(1)
        df[f"6_month_{working_tic1}_max"] = df[working_tic1].rolling(
            6).max().shift(1)

        df[f'per_change_{working_tic1}'] = df[working_tic1].pct_change()
        df[f'per_change_{working_tic2}'] = df[working_tic2].pct_change()

        df_test = pd.DataFrame()
        df_test[f"12_month_{working_tic1}_low"] = df[f"12_month_{working_tic1}_low"]
        df_test[f"6_month_{working_tic1}_max"] = df[f"6_month_{working_tic1}_max"]
        df_test[working_tic1] = df[working_tic1]
        df_test['ETF_next_month'] = df_test.apply(set_label, axis=1)
        df["ETF_next_month"] = df_test['ETF_next_month']
        df['ETF_next_month'] = df['ETF_next_month'].ffill()

        df["ETF_this_month"] = df['ETF_next_month'].shift(1)
        df_test["ETF_this_month"] = df["ETF_this_month"]

        df_test[f'per_change_{working_tic1}'] = df[f'per_change_{working_tic1}']
        df_test[f'per_change_{working_tic2}'] = df[f'per_change_{working_tic2}']
        df_test[f'per_change_{working_tic1}_adj'] = df[f'per_change_{working_tic1}_adj']
        df_test[f'per_change_{working_tic2}_adj'] = df[f'per_change_{working_tic2}_adj']

        df_test['return_adj'] = df_test.apply(get_return_adj, axis=1)
        df_test['return'] = df_test.apply(get_return, axis=1)
        df["return_adj"] = df_test['return_adj']
        df["return"] = df_test['return']

        df.to_csv(
            f"{path}/data_after_processing/SACAM_{i+1}.txt", index=False)
        df = df[["date", "ETF_this_month",
                 "ETF_next_month", "return", "return_adj"]]
        df.to_csv(f"strategies/SACAM_{i+1}.txt", index=False)

        working_tic1 = tic3
        working_tic2 = tic4

    working_tic1 = tic5
    working_tic2 = tic6

    df1 = pd.read_csv(f"data/{working_tic1}.txt")
    df2 = pd.read_csv(f"data/{working_tic2}.txt")

    df1[f'per_change_{working_tic1}_adj'] = df1[f'adj_close_{working_tic1}'].pct_change()
    df2[f'per_change_{working_tic2}_adj'] = df2[f'adj_close_{working_tic2}'].pct_change()

    df1.pop(f'adj_close_{working_tic1}')
    df2.pop(f'adj_close_{working_tic2}')

    df = pd.merge(df1, df2, on="date", how="left")

    df[f"12_month_{working_tic1}_max"] = df[working_tic1].rolling(
        12).max().shift(1)
    df[f"6_month_{working_tic1}_low"] = df[working_tic1].rolling(
        6).min().shift(1)

    df[f'per_change_{working_tic1}'] = df[working_tic1].pct_change()
    df[f'per_change_{working_tic2}'] = df[working_tic2].pct_change()

    df_test = pd.DataFrame()
    df_test[f"12_month_{working_tic1}_max"] = df[f"12_month_{working_tic1}_max"]
    df_test[f"6_month_{working_tic1}_low"] = df[f"6_month_{working_tic1}_low"]
    df_test[working_tic1] = df[working_tic1]
    df_test['ETF_next_month'] = df_test.apply(set_label_tick_5, axis=1)
    df["ETF_next_month"] = df_test['ETF_next_month']
    df['ETF_next_month'] = df['ETF_next_month'].ffill()

    df["ETF_this_month"] = df['ETF_next_month'].shift(1)
    df_test["ETF_this_month"] = df["ETF_this_month"]

    df_test[f'per_change_{working_tic1}_adj'] = df[f'per_change_{working_tic1}_adj']
    df_test[f'per_change_{working_tic2}_adj'] = df[f'per_change_{working_tic2}_adj']
    df_test[f'per_change_{working_tic1}'] = df[f'per_change_{working_tic1}']
    df_test[f'per_change_{working_tic2}'] = df[f'per_change_{working_tic2}']
    df_test['return_adj'] = df_test.apply(get_return_adj, axis=1)
    df_test['return'] = df_test.apply(get_return, axis=1)
    df["return_adj"] = df_test['return_adj']
    df["return"] = df_test['return']

    df.to_csv(f"{path}/data_after_processing/SACAM_3.txt", index=False)
    df = df[["date", "ETF_this_month", "ETF_next_month", "return", "return_adj"]]
    df.to_csv(f"strategies/SACAM_3.txt", index=False)

    df1 = pd.read_csv(f"strategies/SACAM_1.txt")
    df1.columns = ['date', f'ETF_this_month_SACAM_1',
                   f'ETF_next_month_SACAM_1', f'return_SACAM_1', "return_adj_SACAM_1"]
    df2 = pd.read_csv(f"strategies/SACAM_2.txt")
    df2.columns = ['date', f'ETF_this_month_SACAM_2',
                   f'ETF_next_month_SACAM_2', f'return_SACAM_2', "return_adj_SACAM_2"]
    df3 = pd.read_csv(f"strategies/SACAM_3.txt")
    df3.columns = ['date', f'ETF_this_month_SACAM_3',
                   f'ETF_next_month_SACAM_3', f'return_SACAM_3', "return_adj_SACAM_3"]

    df4 = pd.merge(df1, df2, on="date", how="left")

    df = pd.merge(df4, df3, on="date", how="left")
    df["return_SACAM"] = (df["return_SACAM_1"] +
                          df["return_SACAM_2"] + df["return_SACAM_3"]) / 3

    df["return_adj_SACAM"] = (
        df["return_adj_SACAM_1"] + df["return_adj_SACAM_2"] + df["return_adj_SACAM_3"]) / 3

    df.to_csv(f"{path}/results/SACAM.txt", index=False)
    os.remove("strategies/SACAM_1.txt")
    os.remove("strategies/SACAM_2.txt")
    os.remove("strategies/SACAM_3.txt")

    # arr = df.values[-1].tolist()
    # t1 = arr[1]
    # t2 = arr[4]
    # t3 = arr[7]
    # n1 = arr[2]
    # n2 = arr[5]
    # n3 = arr[8]
    # return_all = arr[10]

    print("Stokens Active Combined Asset Monthly is ready")

    # dict_data = {"ETFS_this_month": [t1, t2, t3], "ETFS_next_month": [
    #     n1, n2, n3], "this_month_return": return_all}
    # return dict_data


def build_strategey(ticks, path):
    global tic1, tic2, tic3, tic4, tic5, tic6
    tic1 = ticks[0]
    tic2 = ticks[1]
    tic3 = ticks[2]
    tic4 = ticks[3]
    tic5 = ticks[4]
    tic6 = ticks[5]

    return set_strategey(path)
