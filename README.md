# Log Machine Pipeline

## Overview
Data pipeline and exploratory analysis for machine log data to enable shipment-level insights and anomaly detection in logistics processes.

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

Perfekt 👍 hab die Dateien.

Ich mache dir nur die README-Ergänzung (kurz & sauber):

👉 Füge das in dein README ein:
## Exploratory Data Analysis (EDA)

In addition to the data pipeline, exploratory analysis was performed using Jupyter Notebooks to better understand the behavior of the system.

### Analyses performed:
- **Print Quality Analysis**
  - Distribution and anomalies in print quality
  - Detection of low-quality outputs

- **Shift Change Analysis**
  - Identification of production patterns during shift changes
  - Time-based behavior of the system

- **Station Analysis**
  - Performance comparison between stations
  - Detection of station-specific issues

### Purpose
The EDA supports:
- validation of the pipeline outputs
- identification of patterns and anomalies
- deeper understanding of production behavior
## Run
```bash
python main.py