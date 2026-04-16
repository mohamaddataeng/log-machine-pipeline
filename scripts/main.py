from scripts.pipeline import run_pipeline
from pathlib import Path
from db.sqlite import save_to_sqlite
import sqlite3


def main():
    # Pipeline
    df_raw, df_agg, df_ml = run_pipeline(Path("data"))
    print(df_agg["is_kot"].head())
    print(df_ml["is_kot"].head())

    # Excel Export 
    df_agg.to_excel(r"notebooks\shipments.xlsx", index=False)
    df_raw.to_excel(r"notebooks\logs_raw.xlsx", index=False)
    df_ml.to_excel(r"notebooks\shipments_ml.xlsx", index=False)

    # CSV Export
    df_agg.to_csv(r"notebooks\shipments.csv", index=False)
    df_raw.to_csv(r"notebooks\logs_raw.csv", index=False)
    df_ml.to_csv(r"notebooks\shipments_ml.csv", index=False)


    # Debug (nur auf agg)
    #print(df_agg["is_kot"].value_counts())
    #print(df_agg.shape)
    #print(df_agg.head(10))
    #print(df_agg.info())

    # SQLite speichern
    save_to_sqlite(df_raw, table_name="logs_raw")
    save_to_sqlite(df_agg, table_name="shipments")
    save_to_sqlite(df_ml, table_name="shipments_ml")

    # Test
    conn = sqlite3.connect("database/label_machine.db")

    #print("Shipments:", conn.execute("SELECT COUNT(*) FROM shipments").fetchone())
    #print("Shipments_ml:", conn.execute("SELECT COUNT(*) FROM shipments_ml").fetchone())
    #print("Logs Raw:", conn.execute("SELECT COUNT(*) FROM logs_raw").fetchone())

    conn.close()


if __name__ == "__main__":
    main()