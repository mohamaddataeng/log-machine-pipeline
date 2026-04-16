from pathlib import Path
from scripts.pipeline import run_pipeline
import pandas as pd

# -----------------------
# CONFIG
# -----------------------
DATA_PATH = Path("data")


# -----------------------
# RUN PIPELINE
# -----------------------
df_raw, df_agg, df_ml = run_pipeline(DATA_PATH)

print(df_agg.shape)
print("\nPipeline erfolgreich ausgeführt\n")


# -----------------------
# TEST 1: Basic Stats
# -----------------------
print("\n=== TEST 1: Basic Stats ===")
print("Rows RAW:", len(df_raw))
print("Rows AGG:", len(df_agg))
print("Unique shipments RAW:", df_raw["shipment_id"].nunique(dropna=True))
print("Unique shipments AGG:", df_agg["shipment_id"].nunique(dropna=True))


# -----------------------
# TEST 2: Bulk Reihenfolge Check
# (ohne Overweight detected)
# -----------------------
print("\n=== TEST 2: Bulk Reihenfolge Check ===")

errors = []

shipment_ids = df_raw["shipment_id"].dropna().unique()

for sid in shipment_ids:
    df_test = df_raw[df_raw["shipment_id"] == sid].sort_values("timestamp")

    events = (
        df_test["event"]
        .dropna()
        .loc[lambda x: x != "Overweight detected"]
        .tolist()
    )

    if all(e in events for e in [
        "Label printed",
        "Verification successful",
        "Package processed successfully"
    ]):
        i1 = events.index("Label printed")
        i2 = events.index("Verification successful")
        i3 = events.index("Package processed successfully")

        if not (i1 < i2 < i3):
            errors.append(sid)

print(f"Geprüfte Shipments: {len(shipment_ids)}")
print(f"Fehlerhafte Reihenfolge: {len(errors)}")

if errors:
    print("Beispiele (erste 10):", errors[:10])
else:
    print("Alles korrekt")


# -----------------------
# TEST 3: NULL Check
# -----------------------
print("\n=== TEST 3: NULL Check ===")
print(df_raw.isna().sum())


# -----------------------
# TEST 4: Plausibility Check
# -----------------------
print("\n=== TEST 4: Plausibility Check ===")
print("Negative Gewichte:", (df_raw["act_weight_kg"] < 0).sum())
print("Print Quality > 100:", (df_raw["print_quality_pct"] > 100).sum())
print("Print Quality < 0:", (df_raw["print_quality_pct"] < 0).sum())


# -----------------------
# TEST 5: Aggregation Check
# -----------------------
print("\n=== TEST 5: Aggregation Check ===")
dup = df_agg["shipment_id"].duplicated().sum()
print("Duplicate shipment_id in AGG:", dup)

if dup > 0:
    dup_ids = df_agg[df_agg["shipment_id"].duplicated()]["shipment_id"]
    print("Doppelte shipment_ids:")
    print(dup_ids.head(10))


# -----------------------
# TEST 6: Stop-Zeilen in RAW
# -----------------------
print("\n=== TEST 6: Stop-Zeilen in RAW ===")
print("Anzahl Stop-Zeilen RAW:", (df_raw["is_stop"] == 1).sum())

stop_cols = ["timestamp", "event", "log_message", "shipment_id"]
stop_rows = df_raw[df_raw["is_stop"] == 1][stop_cols].head(10)

print(stop_rows)


# -----------------------
# TEST 7: shipment_id bei Stop-Zeilen
# Erwartung: Stop-Zeilen haben meist keine shipment_id
# -----------------------
print("\n=== TEST 7: shipment_id bei Stop-Zeilen ===")
stop_with_sid = df_raw[(df_raw["is_stop"] == 1) & (df_raw["shipment_id"].notna())]
print("Stop-Zeilen MIT shipment_id:", len(stop_with_sid))

if len(stop_with_sid) > 0:
    print(stop_with_sid[["timestamp", "event", "shipment_id", "log_message"]].head(10))
else:
    print("OK: Stop-Zeilen haben keine shipment_id")


# -----------------------
# TEST 8: stop_reason_after Konsistenz
# -----------------------
print("\n=== TEST 8: stop_reason_after Konsistenz ===")

bad_1 = df_agg[
    (df_agg["stop_after"] == 1) &
    (df_agg["stop_reason_after"].fillna("") == "")
]

bad_0 = df_agg[
    (df_agg["stop_after"] == 0) &
    (df_agg["stop_reason_after"].fillna("") != "")
]

print("stop_after=1 aber kein Grund:", len(bad_1))
print("stop_after=0 aber Grund vorhanden:", len(bad_0))

if len(bad_1) > 0:
    print("\nBeispiele bad_1:")
    print(bad_1[["shipment_id", "event", "stop_after", "stop_reason_after"]].head(10))

if len(bad_0) > 0:
    print("\nBeispiele bad_0:")
    print(bad_0[["shipment_id", "event", "stop_after", "stop_reason_after"]].head(10))


# -----------------------
# TEST 9: stop_after Verteilung in AGG
# -----------------------
print("\n=== TEST 9: stop_after Verteilung ===")
print(df_agg["stop_after"].value_counts(dropna=False))


# -----------------------
# TEST 10: Mapping-Logik prüfen
# stop_after=1 -> im RAW sollte nach diesem Shipment irgendwann ein Stop kommen
# -----------------------
print("\n=== TEST 10: Mapping-Logik Stichprobe ===")

sample_stop_shipments = df_agg[df_agg["stop_after"] == 1]["shipment_id"].head(5).tolist()

for sid in sample_stop_shipments:
    print(f"\n--- Shipment {sid} ---")
    df_test = df_raw[df_raw["shipment_id"] == sid].sort_values("timestamp")
    print(df_test[["timestamp", "event", "shipment_id"]].tail(5))

    if not df_test.empty:
        last_ts = df_test["timestamp"].max()

        next_cols = ["timestamp", "event", "shipment_id"]
        if "is_stop" in df_raw.columns:
            next_cols.append("is_stop")

        next_logs = (
            df_raw[df_raw["timestamp"] >= last_ts]
            .sort_values("timestamp")[next_cols]
            .head(10)
        )

        print("\nLogs ab Shipment-Ende:")
        print(next_logs)


# -----------------------
# TEST 11: Event im AGG prüfen
# -----------------------
print("\n=== TEST 11: Event-Verteilung in AGG ===")
print(df_agg["event"].value_counts(dropna=False).head(20))


print("\n=== TEST 14 (FIXED): stop_after wirklich korrekt? ===")

errors = []

# nur Shipments mit stop_after = 1 prüfen
test_shipments = df_agg[df_agg["stop_after"] == 1]["shipment_id"]

for sid in test_shipments:
    df_test = df_raw[df_raw["shipment_id"] == sid].sort_values("timestamp")

    if df_test.empty:
        continue

    last_ts = df_test["timestamp"].max()

    # gibt es danach einen Stop?
    stop_after = df_raw[
        (df_raw["timestamp"] >= last_ts) &
        (df_raw["is_stop"] == 1)
    ]

    if stop_after.empty:
        errors.append(sid)

print("Fehlerhafte Shipments:", len(errors))

if errors:
    print("Beispiele:", errors[:10])
else:
    print("OK: Alle stop_after sind korrekt gemappt")
# -----------------------
# TEST 13: Spaltencheck
# -----------------------
print("\n=== TEST 13: Spaltencheck ===")
print("RAW columns:")
print(df_raw.columns.tolist())

print("\nAGG columns:")
print(df_agg.columns.tolist())




# Pipeline laufen lassen
"""df_raw, df_agg = run_pipeline(Path("data"))

print("\n=== BASIC CHECK ===")
print("stop_after vorhanden:", "stop_after" in df_agg.columns)
print("stop_reason_after vorhanden:", "stop_reason_after" in df_agg.columns)

print("\n=== VALUE COUNTS ===")
print(df_agg["stop_after"].value_counts())

print("\n=== CHECK: stop_after = 1 ohne Reason (FEHLER) ===")
error_1 = df_agg[(df_agg["stop_after"] == 1) & (df_agg["stop_reason_after"] == "")]
print("Fehleranzahl:", len(error_1))

print("\n=== CHECK: stop_after = 0 mit Reason (FEHLER) ===")
error_2 = df_agg[(df_agg["stop_after"] == 0) & (df_agg["stop_reason_after"] != "")]
print("Fehleranzahl:", len(error_2))

print("\n=== SAMPLE: stop_after = 1 ===")
print(df_agg[df_agg["stop_after"] == 1][
    ["shipment_id", "stop_after", "stop_reason_after"]
].head(10))

print("\n=== SAMPLE: stop_after = 0 ===")
print(df_agg[df_agg["stop_after"] == 0][
    ["shipment_id", "stop_after", "stop_reason_after"]
].head(10))"""

