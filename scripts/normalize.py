import pandas as pd
import re


def normalize_log_message(df: pd.DataFrame) -> pd.DataFrame:

    df = df.assign(
        event=df["log_message"].str.split(" | ", regex=False).str[0].str.split(r"\s(?:as of|for)\s").str[0],
        shipment_id=df["log_message"].str.extract(r"SHIPMENT_ID=(\S+)"),
        station=df["log_message"].str.extract(r"Station=(\S+)"),
        box_barcode=df["log_message"].str.extract(r"BOX_BARCODE=(\S+)"),
        articles=df["log_message"].str.extract(r"Articles=(\S+)"),
        exp_weight_kg=df["log_message"].str.extract(r"EXP=(\S+)"),
        act_weight_kg=df["log_message"].str.extract(r"ACT=(\S+)"),
        tare_weight_kg=df["log_message"].str.extract(r"TARE=(\S+)"),
        #kot_pct=df["log_message"].str.extract(r"KOT=(\S+)"),
        #limit_kg=df["log_message"].str.extract(r"LIMIT=(\S+)"),
        print_quality_pct=df["log_message"].str.extract(r"PrintQuality=(\S+)"),
        line_speed_mps=df["log_message"].str.extract(r"LineSpeed=(\S+)"),
        slam=df["log_message"].str.extract(r"SLAM=(\S+)"),
        iso_grade=df["log_message"].str.extract(r"ISO/IEC15416 Grade=(\S+)"),
        pckg_problem_found=df["log_message"].str.extract(r"PckgProblFound=(\S+)")


    )

    return df

def normalize_thread(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        thread_label = df["thread"].str.extract(r"(LabelThread-\d)")
    )
    return df