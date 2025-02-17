from time import time

import pandas as pd
import requests
import yfinance as yf
from loguru import logger


def get_top_cryptos() -> pd.DataFrame:
    """Fetches the top cryptocurrencies from CoinPaprika API (No API Key Required).

    Returns:
        list: List of top cryptocurrency tickers.
    """
    url = "https://api.coinpaprika.com/v1/tickers"
    response = requests.get(url)
    data = response.json()
    dftop = pd.DataFrame(data)
    dftop["ticker"] = dftop["symbol"] + "-USD"
    return dftop


def get_yahoo_ticker_data(ticker_pair: str = "BTC-USD") -> pd.DataFrame:
    """Fetches historical OHLCV data for a given ticker pair using yfinance.

    Args:
        ticker_pair (str, optional): _description_. Defaults to "BTC-USD".

    Returns:
        pd.DataFrame: DataFrame containing the historical OHLCV data for the given ticker pair.
    """
    ticker = yf.Ticker(ticker_pair)
    df = ticker.history(period="max")
    df = df.reset_index()
    df["Ticker"] = ticker_pair
    columns = ['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    return df[columns]


if __name__ == "__main__":
    start = time()
    dftop = get_top_cryptos()
    ticker_list = dftop["ticker"].tolist()
    # ticker_list = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD", "DOGE-USD", "SOL-USD", "MATIC-USD", "BNB-USD", "ADA-USD", "DOT-USD"]

    for ticker in ticker_list:
        logger.info(f"Fetching data for {ticker}")
        df = get_yahoo_ticker_data(ticker)
        df.to_parquet(f"cache/{ticker}.snap.parquet", index=False)

    logger.info(f"Finished fetching and saving data for {len(ticker_list):,} pairs in {time() - start:.2f} seconds")
