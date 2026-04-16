import sqlite3
import pandas as pd
from pathlib import Path


def save_to_sqlite(
    df: pd.DataFrame,
    db_path: str = "database/label_machine.db", 
    table_name: str = "shipments"
):
    """Speichert ein DataFrame in SQLite
     (alle Spalten bleiben erhalten)
    """

    # Ordner sicherstellen
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    # 🔹 ALLE Spalten speichern
    df.to_sql(
        table_name,
        conn,
        if_exists="replace",
        index=False
    )

    cursor = conn.cursor()

    # sinnvolle Indizes
    if "shipment_id" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_shipment ON {table_name}(shipment_id)")

    if "timestamp" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_time ON {table_name}(timestamp)")

    if "station" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_station ON {table_name}(station)")

    if "is_kot" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_kot ON {table_name}(is_kot)")

    if "event" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_event ON {table_name}(event)")

    if "line_speed_mps" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_speed ON {table_name}(line_speed_mps)")
    
    if "stop_after" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_stop_after ON {table_name}(stop_after)")

    if "stop_reason_after" in df.columns:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_stop_reason ON {table_name}(stop_reason_after)")
    conn.commit()
    conn.close()