from strategies import createStrategies
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from utils.utils import *

# Glenn's Paired Switching Strategy:

# original SPY, im using VOO

st_1_tic1 = "VOO"
st_1_tic2 = "TLT"

# Stoken's Active Combined Asset (ACA) - Monthly

# original SPY, im using VOO

st_2_tic1 = "VOO"
st_2_tic2 = "IEF"
st_2_tic3 = "VNQ"
st_2_tic4 = "IEF"
st_2_tic5 = "GLD"
st_2_tic6 = "TLT"

# Vigilant Asset Allocation - Aggressive:

# original SPY, EEM, CASH, im using VOO, IEMG, SHY

# aggressive
st_3_tic1 = "VOO"
st_3_tic2 = "IEMG"
st_3_tic3 = "VEA"
st_3_tic4 = "AGG"

# defensive
st_3_tic5 = "SHY"
st_3_tic6 = "TLT"
st_3_tic7 = "LQD"


# Glenn's Paired Switching Strategy as test:

st_test_1 = "VNQ"
st_test_2 = "VEA"

# define strategies and percentage


percentage = [0.4, 0.4, 0.2]

allTicks = {
    "GPS": [st_1_tic1, st_1_tic2],
    "SACAM": [st_2_tic1, st_2_tic2, st_2_tic3, st_2_tic4, st_2_tic5, st_2_tic6],
    "VAAA": [st_3_tic1, st_3_tic2, st_3_tic3, st_3_tic4, st_3_tic5, st_3_tic6, st_3_tic7],
}

strategies = list(allTicks.keys())

def run():

    uniqe_id = "regular"

    name = strategies[0]
    for st in strategies[1:]:
        name += "_" + st

    name += "/" + uniqe_id

    for pr in percentage:
        name += "_" + str(int(pr * 100))

    path = f"strategy_results/{name}"

    create_directory(path + "/data_after_processing")
    create_directory(path + "/results")

    createStrategies.create(path, allTicks)

    create_results_df(path, percentage, allTicks)

    stats = get_stats(path, percentage, allTicks)

    for stat in stats:
        print(f"\n{stat}")

    ETFS_this_month, ETFS_next_month, this_month_return = sortStats(
        stats, percentage)

    print("\nETFS_this_month: ", ETFS_this_month)
    print("\nETFS_next_month: ", ETFS_next_month)
    print("\nthis_month_return: ", this_month_return)


run()
