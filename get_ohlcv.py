import os
from datetime import datetime
from pathlib import Path
from time import time

import pandas as pd
import requests
import yfinance as yf
from loguru import logger
from tqdm import tqdm


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
    columns = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    return df[columns]


def should_update_file(file_path: Path) -> bool:
    """Check if the file was updated in the last 24 hours."""
    if not file_path.exists():
        return True  # If file doesn't exist, we need to update it.

    last_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    now = datetime.now()
    time_difference = now - last_modified_time
    return time_difference.total_seconds() > 86400  # 24 hours


if __name__ == "__main__":
    DIR = Path("cache")
    DIR.mkdir(exist_ok=True)

    start = time()
    dftop = get_top_cryptos()
    ticker_list = dftop["ticker"].tolist()
    # ticker_list = ["BTC-USD", "ETH-USD", "LTC-USD", "XRP-USD", "DOGE-USD", "SOL-USD", "MATIC-USD", "BNB-USD", "ADA-USD", "DOT-USD"]

    for ticker in tqdm(ticker_list):
        local_path = DIR / f"{ticker}.snap.parquet"
        needs_update = should_update_file(local_path)
        if needs_update:
            logger.info(f"Fetching data for {ticker}")
            df = get_yahoo_ticker_data(ticker)
            df.to_parquet(local_path, index=False)
        else:
            logger.debug(f"Skipping {ticker} as it was updated recently")
            continue

    logger.info(f"Finished fetching and saving data for {len(ticker_list):,} pairs in {time() - start:.2f} seconds")
