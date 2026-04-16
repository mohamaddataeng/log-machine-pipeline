# Package processed successfully | SHIPMENT_ID=Sp260301208307 | Station=Station-01 | EXP=1.898kg ACT=1.99534kg TARE=0.11kg KOT=0.012% | Articles=19 | BOX_BARCODE=BC_MEDIUM200
import pandas as pd
from pathlib import Path
import re

def parse_file(file_path: Path):
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    if file_path.is_dir():
        raise IsADirectoryError(file_path)

    parser_map = {".csv": parse_csv, ".log": parse_txt, ".txt": parse_txt}
    suffix = file_path.suffix.lower()

    if suffix not in parser_map:
        raise ValueError(f"Unsupported file type: {suffix}")

    parser_function = parser_map[suffix]

    return parser_function(file_path)


def parse_csv(file_path: Path):
    return pd.read_csv(file_path)


def parse_txt(file_path: Path):
    with open(file_path, encoding="utf-8") as file:
        lines = file.readlines()
        df = pd.DataFrame(lines, columns=["message"])
        df["original_order"] = df.index

    parsed = df["message"].apply(parse_log_line)

    df_parsed = pd.DataFrame(parsed.tolist(), columns=[
    "log_date",
    "log_time",
    "log_millis",
    "severity",
    "thread",
    "source",
    "log_message"
])
    df = pd.concat([df, df_parsed], axis=1)
    df.drop(columns=["message"], inplace=True)

    return df


def parse_log_line(line):

    LOG_PATTERN = re.compile(
        r'(\d{4}-\d{2}-\d{2}) '
        r'(\d{2}:\d{2}:\d{2}),(\d+)\s+'
        r'(INFO|WARN|ERROR|DEBUG)\s+'
        r'\[(.*?)\]\s+'
        r'(.*?):\s+(.*)'
    )

    match = LOG_PATTERN.search(line)

    if not match:
        print("NO MATCH:", line)

    if match:
        return match.groups()

    return (None, None, None, None, None, None, None)




