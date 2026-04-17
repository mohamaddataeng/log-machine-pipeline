# Log Machine Pipeline

## Overview
Data pipeline for processing machine log files into structured shipment-level datasets.

## Features
- Parse log files (CSV / TXT)
- Clean and normalize data
- Feature engineering (e.g. weight differences, KOT detection)
- Aggregate data on shipment level
- Store results in SQLite database

## Tech Stack
- Python
- pandas
- numpy
- SQLite

## Pipeline
Raw Logs → Parsing → Cleaning → Feature Engineering → Aggregation → Database

## Example Features
- `weight_diff`: deviation between expected and actual weight  
- `is_kot`: overweight detection  
- `distance_to_threshold`: deviation from tolerance  

## Run
```bash
python main.py