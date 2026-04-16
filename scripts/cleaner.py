import pandas as pd

def cleaner(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = remove_duplicate(df)
    df = combine_timestamp(df)
    return df


def remove_duplicate(df: pd.DataFrame) -> pd.DataFrame:
    duplicates = df[df.duplicated(keep=False)]
    print(duplicates)

    df = df.drop_duplicates()
    return df


def combine_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(
    df["log_date"] + " " + df["log_time"] + "." + df["log_millis"],
    errors="coerce"
)

    df = df.drop(columns=['log_date', 'log_time'])
    df = df.dropna(subset=['timestamp'])
    df = df.sort_values(['timestamp', 'original_order']).reset_index(drop=True)

    return df


   

    



