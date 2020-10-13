import json
import pandas as pd
import pandas_datareader.data as web
import numpy as np
from datetime import datetime
import os
from pandas.tseries.offsets import MonthEnd


def get_stock_daily(ticker):
    df = web.get_data_yahoo(ticker, interval='d', start='2010-01-01')

    print(ticker)
    df.to_csv(ticker + ".csv")
    df = pd.read_csv(ticker + ".csv")
    os.remove(ticker + ".csv")

    df = df[["Date", 'Close', 'Adj Close']]

    df.columns = ["date", f"{ticker}_D", f'adj_close_{ticker}_D']
    if not df["date"].is_unique:
        df = df.drop([(df.shape[0]) - 2])

    df.to_csv(f"../data/{ticker}_D.txt", index=False)


def get_stock_month(ticker):
    df = web.get_data_yahoo(ticker, interval='m', start='2010-01-01')

    print(ticker)
    df.to_csv(ticker + ".csv")
    df = pd.read_csv(ticker + ".csv")
    os.remove(ticker + ".csv")

    df = df[["Date", 'Close', 'Adj Close']]

    df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d") + MonthEnd(1)

    df.columns = ["date", f"{ticker}", f'adj_close_{ticker}']
    if not df["date"].is_unique:
        df = df.drop([(df.shape[0]) - 2])

    df.to_csv(f"../data/{ticker}.txt", index=False)


tickers = ["SPY", "TLT", "VOO", "IEF", "GLD", "VNQ",
           "EEM", "IEMG", "VEA", "AGG", "SHY", "LQD", "SPXL", "TMF"]

for ticker in tickers:
    get_stock_month(ticker)
