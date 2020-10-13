import os
import json
import pandas as pd
import numpy as np
from datetime import datetime


def sortStats(stats, percentage):

    ETFS_this_month = {}
    ETFS_next_month = {}
    this_month_return = 0

    for i, stat in enumerate(stats):
        per = percentage[i] / len(stat["ETFS_this_month"])
        this_month = stat["ETFS_this_month"]
        next_month = stat["ETFS_next_month"]
        month_return = stat["this_month_return"]
        this_month_return += month_return * per

        for etf in this_month:
            if etf in ETFS_this_month:
                ETFS_this_month[etf] += per
            else:
                ETFS_this_month[etf] = per

        for etf in next_month:
            if etf in ETFS_next_month:
                ETFS_next_month[etf] += per
            else:
                ETFS_next_month[etf] = per

    return ETFS_this_month, ETFS_next_month, this_month_return

def get_num_of_trades(df_history, keys_list):

    dict_strategy = {}
    sum_orders = 0
    for key in keys_list:
        columns_to_go_over = []
        for col in df_history.columns:
            if key in col:
                columns_to_go_over.append(col)

        orders = 0
        for col in columns_to_go_over:
            list_of_values = df_history[f"{col}"].to_list()
            last_diff = list_of_values[0]
            for i in list_of_values:
                if i != last_diff:
                    orders += 1
                last_diff = i
        dict_strategy[f"strategy_{key}_orders"] = orders
        sum_orders += orders

    dict_strategy["all_orders"] = sum_orders

    return dict_strategy


def create_directory(directory_path):
    if os.path.exists(directory_path):
        return None
    else:
        try:
            os.makedirs(directory_path)
        except:
            # in case another machine created the path meanwhile !:(
            return None
        return directory_path


def get_stats(path, percentage, allTicks):

    stats = []

    for key in allTicks.keys():
        df_temp = pd.read_csv(path + f"/results/{key}.txt")
        ETFS_this_month_places = []
        ETFS_next_month_places = []
        return_not_adj_place = 0

        ETFS_this_month = []
        ETFS_next_month = []
        return_not_adj = 0

        for i, value in enumerate(df_temp.columns):
            if "ETF_this_month" in value:
                ETFS_this_month_places.append(i)
            elif "ETF_next_month" in value:
                ETFS_next_month_places.append(i)
            elif "return" in value and "adj" not in value:
                return_not_adj_place = i

        arr = df_temp.values[-1].tolist()

        for i, value in enumerate(arr):

            if i in ETFS_this_month_places:
                ETFS_this_month.append(value)
            if i in ETFS_next_month_places:
                ETFS_next_month.append(value)
            if i == return_not_adj_place:
                return_not_adj = value

        dict_data = {"strategy": key, "ETFS_this_month": ETFS_this_month,
                     "ETFS_next_month": ETFS_next_month, "this_month_return": return_not_adj}
        stats.append(dict_data)

    return stats


def create_results_df(path, percentage, allTicks):

    dfs = []
    ticks = []
    col_names_for_this_month_etfs = ['date']
    for i in allTicks.keys():
        df_temp = pd.read_csv(path + f"/results/{i}.txt")
        dfs.append(df_temp)
        ticks += allTicks[i]
        for col_name in df_temp.columns:
            if "ETF_this_month" in col_name:
                col_names_for_this_month_etfs.append(col_name)

    df = dfs[0]

    for df_i in dfs[1:]:
        df = pd.merge(df, df_i, on="date", how="left")

    df["return_not_adj"] = 0
    df["return_adj"] = 0
    for count, key in enumerate(allTicks.keys()):
        df["return_not_adj"] = df["return_not_adj"] + \
            (df[f"return_{key}"] * percentage[count])
        df["return_adj"] = df["return_adj"] + \
            (df[f"return_adj_{key}"] * percentage[count])

    df = df.loc[28:]

    col_names_for_this_month_etfs.append('return_not_adj')
    col_names_for_this_month_etfs.append('return_adj')

    df_history = df.copy()
    df_history = df_history[col_names_for_this_month_etfs]

    df_history['date'] = pd.to_datetime(df_history['date']).dt.to_period('M')

    print(f"\n{get_num_of_trades(df_history, list(allTicks.keys()))}")

    list_of_adj = df_history["return_adj"].tolist()
    list_of_not_adj = df_history["return_not_adj"].tolist()

    start_point = df_history.values[0][0]

    num = 100
    for i in list_of_adj:
        num = num * i + num
    print(f"\nadj return from {start_point}: ", num / 100)

    num = 100
    for i in list_of_not_adj:
        num = num * i + num
    print(f"\nnot adj return from {start_point}: ", num / 100)

    df_history.to_csv(path + "/results/results_history.csv")
    df.to_csv(path + "/results/results_full.csv")
