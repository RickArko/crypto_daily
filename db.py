from pathlib import Path
from typing import List, Union

import polars as pl
from loguru import logger
from tqdm import tqdm


DIR = Path("cache") / "ohlcv"
local_files = list(DIR.glob("*.snap.parquet"))

def load_df(files: List[Union[str, Path]]) -> pl.DataFrame:
    df = pl.concat([pl.read_parquet(p) for p in tqdm(files)])
    return df

df = load_df(local_files)