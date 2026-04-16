import pandas as pd
import numpy as np


def clean_units(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "exp_weight_kg",
        "act_weight_kg",
        "tare_weight_kg",
        "line_speed_mps",
        "print_quality_pct"
    ]

    df[cols] = df[cols].replace(r'[^0-9.]', '', regex=True).astype(float)
    df["articles"] = pd.to_numeric(df["articles"]).astype("Int64")

    return df


# Netto-Produktgewicht
def add_product_weight(df: pd.DataFrame) -> pd.DataFrame:
    df["product_weight_kg"] = (
        df["act_weight_kg"] - df["tare_weight_kg"]
    )
    return df


# DIFF = EXP − (ACT − TARE)
def add_weight_diff(df: pd.DataFrame) -> pd.DataFrame:
    df["weight_diff"] = (
        df["exp_weight_kg"] - (df["act_weight_kg"] - df["tare_weight_kg"])
    )
    return df


# absolute Differenz
def add_abs_weight_diff(df: pd.DataFrame) -> pd.DataFrame:
    df["abs_weight_diff"] = df["weight_diff"].abs()
    return df


# relative Differenz (nur für Analyse)
def add_weight_diff_ratio(df: pd.DataFrame) -> pd.DataFrame:
    df["weight_diff_ratio"] = (
        df["abs_weight_diff"] /
        df["exp_weight_kg"].replace(0, np.nan)
    ).round(3)
    return df


# Kickout laut Log-Event
def add_is_kot(df: pd.DataFrame) -> pd.DataFrame:
    df["is_kot"] = (
    (df["event"] == "Overweight detected") |
    (df["event"] == "Overweight detected+")
).astype(int)
    return df


# Kickout laut Generator-Regel (mit Toleranz gegen Rundungsfehler)
def add_is_kot_regel(df: pd.DataFrame) -> pd.DataFrame:
    TOL = 0.012
    EPS = 1e-6  # kleine Toleranz für Floating Point

    df["is_kot_regel"] = (
        df["abs_weight_diff"] >= (df["exp_weight_kg"] * TOL - EPS)
    ).astype(int)

    return df

# wie weit der Wert von der erlaubten Grenze entfernt ist.
# distance_to_threshold =abs_weight_diff − (exp_weight_kg × 0.012)
def add_distance_to_threshold(df: pd.DataFrame) -> pd.DataFrame:
    TOL = 0.012
    df["distance_to_threshold"] = (
        df["abs_weight_diff"] - (df["exp_weight_kg"] * TOL)
    )
    return df

"""
Zuordnung über Zeit
korrektes Shipment (größter Timestamp < Stop)
Flag setzen (stop_after = 1)
Reason vom Stop
nur ein Stop pro Shipment speichern"""

def map_stop_to_previous_shipment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["timestamp", "original_order"]).reset_index(drop=True)

    shipment_end = df[
        (df["event"] == "Package processed successfully") &
        (df["shipment_id"].notna())
    ][["timestamp", "shipment_id"]].copy()

    stop_rows = df[
        df["is_stop"] == 1
    ][["timestamp", "stop_reason"]].copy()

    stop_map = pd.merge_asof(
        stop_rows,
        shipment_end,
        on="timestamp",
        direction="backward"
    )

    stop_map["stop_after"] = 1
    stop_map = stop_map.rename(columns={"stop_reason": "stop_reason_after"})

    stop_map = stop_map.dropna(subset=["shipment_id"])
    stop_map = stop_map.drop_duplicates(subset=["shipment_id"], keep="first")

    return stop_map[["shipment_id", "stop_after", "stop_reason_after"]]

def add_is_stop(df: pd.DataFrame) -> pd.DataFrame:
    df["is_stop"] = df["event"].eq("Production line stopped").astype(int)
    return df

def add_stop_reason(df: pd.DataFrame) -> pd.DataFrame:

    df["stop_reason"] = (
        df["log_message"]
        .str.extract(r"(?:as of|for)\s(.*?)(?:\.|$)", expand=False)
    )

    return df
 