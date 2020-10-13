import json
import pandas as pd
import numpy as np
from datetime import datetime


# aggressive
tic1 = ""
tic2 = ""
tic3 = ""
tic4 = ""

# defensive
tic5 = ""
tic6 = ""
tic7 = ""

working_tic = ""


def set_label(r):
    if r[f"13612_{tic1}"] < 0 or r[f"13612_{tic2}"] < 0 or r[f"13612_{tic3}"] < 0 or r[f"13612_{tic4}"] < 0:
        max_num = max(r[f"13612_{tic5}"],
                      r[f"13612_{tic6}"], r[f"13612_{tic7}"])
        if max_num == r[f"13612_{tic5}"]:
            return tic5
        elif max_num == r[f"13612_{tic6}"]:
            return tic6
        else:
            return tic7
    else:
        max_num = max(r[f"13612_{tic1}"], r[f"13612_{tic2}"],
                      r[f"13612_{tic3}"], r[f"13612_{tic4}"])
        if max_num == r[f"13612_{tic1}"]:
            return tic1
        elif max_num == r[f"13612_{tic2}"]:
            return tic2
        elif max_num == r[f"13612_{tic3}"]:
            return tic3
        else:
            return tic7


def get_return(r):
    global working_tic
    arr = [tic1, tic2, tic3, tic4, tic5, tic6, tic7]
    for i in range(len(arr)):
        working_tic = arr[i]
        if r.ETF_this_month == working_tic:
            return r[f'per_change_{working_tic}']
    return 0


def get_return_adj(r):
    global working_tic
    arr = [tic1, tic2, tic3, tic4, tic5, tic6, tic7]
    for i in range(len(arr)):
        working_tic = arr[i]
        if r.ETF_this_month == working_tic:
            return r[f'per_change_{working_tic}_adj']
    return 0


def set_stratege(path):

    global working_tic

    arr = [tic1, tic2, tic3, tic4, tic5, tic6, tic7]
    for i in range(len(arr)):
        working_tic = arr[i]
        df = pd.read_csv(f"data/{working_tic}.txt")
        df[f'per_change_{working_tic}_adj'] = df[f'adj_close_{working_tic}'].pct_change(
        )
        df.pop(f'adj_close_{working_tic}')
        df[f"1_month_{working_tic}"] = df[working_tic].shift(1)
        df[f"3_month_{working_tic}"] = df[working_tic].shift(3)
        df[f"6_month_{working_tic}"] = df[working_tic].shift(6)
        df[f"12_month_{working_tic}"] = df[working_tic].shift(12)

        df[f"1_month_{working_tic}_change"] = (
            df[working_tic] / df[f"1_month_{working_tic}"]) - 1
        df[f"3_month_{working_tic}_change"] = (
            df[working_tic] / df[f"3_month_{working_tic}"]) - 1
        df[f"6_month_{working_tic}_change"] = (
            df[working_tic] / df[f"6_month_{working_tic}"]) - 1
        df[f"12_month_{working_tic}_change"] = (
            df[working_tic] / df[f"12_month_{working_tic}"]) - 1

        df[f"13612_{working_tic}"] = (df[f"12_month_{working_tic}_change"] + df[f"6_month_{working_tic}_change"]
                                      * 2 + df[f"3_month_{working_tic}_change"] * 4 + df[f"1_month_{working_tic}_change"] * 12) / 4
        df[f'per_change_{working_tic}'] = df[working_tic].pct_change()
        df.to_csv(f"{path}/data_after_processing/VAAA_{i+1}.txt")

    df = pd.read_csv(f"{path}/data_after_processing/VAAA_1.txt")
    df = df[["date", f"13612_{tic1}",
             f'per_change_{tic1}', f'per_change_{tic1}_adj']]

    for i in range(1, 7):
        df1 = pd.read_csv(f"{path}/data_after_processing/VAAA_{i+1}.txt")
        df1 = df1[["date", f"13612_{arr[i]}",
                   f'per_change_{arr[i]}', f'per_change_{arr[i]}_adj']]
        df = pd.merge(df, df1, on="date", how="left")

    df_test = df.copy()
    df_test = df_test.fillna(0)
    df_test['ETF_next_month'] = df_test.apply(set_label, axis=1)
    df["ETF_next_month"] = df_test['ETF_next_month']
    df['ETF_next_month'] = df['ETF_next_month'].ffill()
    df["ETF_this_month"] = df['ETF_next_month'].shift(1)
    df_test["ETF_this_month"] = df["ETF_this_month"]
    df_test['return'] = df_test.apply(get_return, axis=1)
    df_test['return_adj'] = df_test.apply(get_return_adj, axis=1)
    df["return_adj"] = df_test['return_adj']
    df["return"] = df_test['return']

    df.to_csv(f"{path}/data_after_processing/VAAA.txt")

    df = df[["date", "ETF_this_month", "ETF_next_month", "return", "return_adj"]]
    df.columns = ["date", "ETF_this_month_VAAA",
                  "ETF_next_month_VAAA", "return_VAAA", "return_adj_VAAA"]

    df.to_csv(f"{path}/results/VAAA.txt", index=False)

    # arr = df.values[-1].tolist()
    # t1 = arr[1]
    # n1 = arr[2]
    # return_all = arr[3]

    print("Vigilant Asset Allocation Aggressive is ready")

    # dict_data = {"ETFS_this_month": [t1], "ETFS_next_month": [
    #     n1], "this_month_return": return_all}

    # return dict_data


def build_strategey(ticks, path):
    global tic1, tic2, tic3, tic4, tic5, tic6, tic7
    tic1 = ticks[0]
    tic2 = ticks[1]
    tic3 = ticks[2]
    tic4 = ticks[3]
    tic5 = ticks[4]
    tic6 = ticks[5]
    tic7 = ticks[6]

    return set_stratege(path)
