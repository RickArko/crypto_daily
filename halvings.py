import datetime

import pandas as pd
import requests


def get_halving_map() -> dict:
    """Returns a dictionary of Bitcoin halving block numbers with their estimated dates."""
    url = "https://api.blockchair.com/bitcoin/stats"
    response = requests.get(url).json()
    current_block = response["data"]["blocks"]

    halving_blocks = [210000 * i for i in range(1, 50)]  # Up to 50 halvings for future-proofing
    genesis_date = datetime.datetime(2009, 1, 3)
    block_time = 10  # Avg 10 minutes per block

    halving_dates = {block: genesis_date + datetime.timedelta(minutes=block * block_time) for block in halving_blocks}
    return halving_dates


def get_bitcoin_issuance() -> pd.DataFrame:
    """Generates a DataFrame with daily Bitcoin issuance history."""
    halving_dates = get_halving_map()
    halving_blocks = sorted(halving_dates.keys())

    # Initial block reward
    reward = 50
    data = []

    for i in range(len(halving_blocks) - 1):
        start_date = halving_dates[halving_blocks[i]]
        end_date = halving_dates[halving_blocks[i + 1]]

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        daily_issuance = reward * 6 * 24  # Blocks per day * block reward

        for date in date_range:
            data.append({"Date": date, "issuance": daily_issuance})

        reward /= 2  # Halving event

    df = pd.DataFrame(data)
    df["Date"] = df["Date"].dt.normalize()
    return df


if __name__ == "__main__":
    issuance_df = get_bitcoin_issuance()
    issuance_df.to_csv("cache/bitcoin-issuance.snap.parquet", index=False)
    print(issuance_df.head())
