import sqlite3
from pathlib import Path
from typing import List, Union

import pandas as pd
import polars as pl
from loguru import logger
from tqdm import tqdm


DB_PATH = "ohlcv.db"

# def load_df(files: List[Union[str, Path]]) -> pl.DataFrame:
#     df = pl.concat([pl.read_parquet(p) for p in tqdm(files)])
#     return df


def load_df(files: List[Union[str, Path]]) -> pl.DataFrame:
    df = pd.concat([pd.read_parquet(p) for p in tqdm(files)])
    return df


def connect_db():
    """Connects to SQLite database."""
    return sqlite3.connect(DB_PATH)


def drop_and_create_table():
    """Drops existing OHLCV table and recreates it."""
    with connect_db() as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ohlcv")  # Drop existing table
        cur.execute(
            """
        CREATE TABLE ohlcv (
            Ticker TEXT,
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume REAL,
            PRIMARY KEY (Ticker, Date)
        )
        """
        )
        conn.commit()


def load_df(files: List[Union[str, Path]]) -> pd.DataFrame:
    """Loads all Parquet files into a single DataFrame."""
    df = pd.concat([pd.read_parquet(p) for p in tqdm(files)], ignore_index=True)
    return df


def insert_ohlcv_data(df):
    """Inserts OHLCV data into SQLite database, avoiding duplicates."""
    with connect_db() as conn:
        df.to_sql("ohlcv", conn, if_exists="append", index=False, method="multi", chunksize=1_000)


def upload_df_sqlite(df: pd.DataFrame):
    """Main function to load Parquet data and insert it into SQLite."""
    logger.info("Starting SQLite sync...")
    drop_and_create_table()
    insert_ohlcv_data(df)
    logger.info(f"Inserted {len(df):,} rows into SQLite.")


if __name__ == "__main__":
    DIR = Path("cache") / "ohlcv"
    DIR = Path("cache") / "full"

    local_files = list(DIR.glob("*.snap.parquet"))
    df = load_df(local_files)
    upload_df_sqlite(df)
