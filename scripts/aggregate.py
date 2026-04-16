import pandas as pd

def aggregate_shipments(df: pd.DataFrame) -> pd.DataFrame:
    # Wichtig: Reihenfolge stabil halten
    df = df.sort_values(["timestamp", "original_order"])

    # Aggregation (ohne event!)
    result = (
        df.groupby("shipment_id")
        .agg({
            "timestamp": "first",
            "severity": "first",
            "thread": "first",
            "source": "first",
            "station": "first",
            "box_barcode": "first",
            "articles": "first",
            "exp_weight_kg": "first",
            "act_weight_kg": "first",
            "tare_weight_kg": "first",
            "product_weight_kg": "first",
            "abs_weight_diff": "first",
            "print_quality_pct": "first",
            "line_speed_mps": "first",
            "slam": "first",
            "iso_grade": "first",
            "pckg_problem_found": "max",
            "weight_diff": "first",
            "weight_diff_ratio": "first",
            "distance_to_threshold": "first",
            "is_kot": "max",
        
        })
        .reset_index()
    )

    # echtes letztes Event holen
    last_event = (
        df.dropna(subset=["shipment_id"])
          .groupby("shipment_id")
          .tail(1)[["shipment_id", "event"]]
    )

    # mergen
    result = result.merge(last_event, on="shipment_id", how="left")

    return result