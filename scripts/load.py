import pandas as pd
from pathlib import Path

def load_files(file_path: Path):
    files = []
    for file in file_path.rglob('*'):
        if file.is_file():
            files.append(file)

    return files

