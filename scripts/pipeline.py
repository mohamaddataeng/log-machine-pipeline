from scripts.load import load_files
from scripts.cleaner import cleaner
from scripts.parser import parse_file
from scripts.normalize import normalize_log_message, normalize_thread
from scripts.feature_eng import (
    clean_units,
    add_product_weight,
    add_weight_diff,
    add_abs_weight_diff,
    add_weight_diff_ratio,
    add_is_kot,
    add_distance_to_threshold,
    add_is_stop, 
    add_stop_reason,
    map_stop_to_previous_shipment
)
from scripts.aggregate import aggregate_shipments
from pathlib import Path
import pandas as pd


def run_pipeline(data_path: Path):
    files = load_files(data_path)
    print(files)

    df_list = []

    COLUMN_ORDER_RAW = [
    "timestamp",
    "severity",
    "thread",
    "source",
    "event",
    "log_message",
    "shipment_id",
    "station",
    "box_barcode",
    "articles",
    "exp_weight_kg",
    "act_weight_kg",
    "tare_weight_kg",
    "print_quality_pct",
    "line_speed_mps",
    "slam",
    "iso_grade",
    "pckg_problem_found",
    "is_stop",
    "stop_reason"
    "map_stop_to_previous_shipment"
]
    COLUMN_ORDER_AGG = [
    "shipment_id",
    "timestamp",
    "severity",
    "thread",
    "source",
    "event",
    "station",
    "box_barcode",
    "articles",
    "exp_weight_kg",
    "act_weight_kg",
    "tare_weight_kg",
    "product_weight_kg",
    "abs_weight_diff",
    "print_quality_pct",
    "line_speed_mps",
    "slam",
    "iso_grade",
    "pckg_problem_found",
    "weight_diff",
    "weight_diff_ratio",
    "distance_to_threshold",
    "is_kot",
    "stop_after",
    "stop_reason_after"
   
]
    for file in files:
        df = parse_file(file)

        print("after parse:", df.shape)
        before = len(df)

        # cleaner
        df = cleaner(df)
        after = len(df)
        print("Nach cleaner:", after, "| Verlust:", before - after)
        before = after

        # normalize
        df = normalize_log_message(df)
        df = normalize_thread(df)
        after = len(df)
        print("Nach normalize:", after, "| Verlust:", before - after)
        before = after

        #Debug check
        df_test = df[df["shipment_id"] == "Sp260318092583"]
        print(df_test[["timestamp", "event"]])

        # features
        df = clean_units(df)
        df = add_product_weight(df)
        df = add_weight_diff(df)
        df = add_abs_weight_diff(df)
        df = add_weight_diff_ratio(df)
        df = add_is_kot(df)
        df = add_distance_to_threshold(df)
        df = add_is_stop(df)
        df = add_stop_reason(df)
        df_stop_map = map_stop_to_previous_shipment(df)
        print(df_stop_map.head(10))
        print(df_stop_map.shape)

        after = len(df)
        print("Nach features:", after)

        
        print(df.columns.tolist())
        # RAW sichern 
        df_raw = df.copy()

        
        # Aggregation separat
        df_agg = aggregate_shipments(df)
        df_raw = df_raw.drop(columns=["original_order", "log_millis"])
        df_agg = df_agg.merge(df_stop_map, on="shipment_id", how="left")

        df_agg["stop_after"] = df_agg["stop_after"].fillna(0).astype(int)
        df_agg["stop_reason_after"] = df_agg["stop_reason_after"].fillna("")
        print(df_agg[["stop_after"]].value_counts())

    

        # NUR DICTIONARY speichern
        df_list.append({
            "raw": df_raw,
            "agg": df_agg
        })

    if not df_list:
        raise ValueError("No objects to concatenate")

    # Trennen
    df_raw_all = pd.concat([x["raw"] for x in df_list], ignore_index=True)
    df_agg_all = pd.concat([x["agg"] for x in df_list], ignore_index=True)

    # agg-Daten sicherstellen
    df_ml = df_agg_all.copy()
    df_ml[["pckg_problem_found", "is_kot" ]] = None

    # Reihenfolge der Spalten festlegen
    df_raw_all = df_raw_all[[col for col in COLUMN_ORDER_RAW if col in df_raw_all.columns]]
    df_agg_all = df_agg_all[[col for col in COLUMN_ORDER_AGG if col in df_agg_all.columns]]
    df_ml = df_ml[[col for col in COLUMN_ORDER_AGG if col in df_ml.columns]]
    return df_raw_all, df_agg_all, df_ml