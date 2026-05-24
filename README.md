# Data Pipeline Agent

Automated ETL (Extract, Transform, Load) pipeline with multi-source support.

## Features
- Multi-source: CSV, JSON, SQLite extraction
- Smart transforms: deduplicate, rename, filter
- Export: CSV or JSON output
- Metrics tracking: counts, errors, timing

## Usage
```bash
python3 pipeline.py config.json
```

## Config Example
```json
{
  "name": "user-etl",
  "sources": [{"type": "csv", "path": "users.csv"}],
  "output": "clean_data.json"
}
```

## Built With
- Hermes Agent + Cursor
- MiMo + GPT series models
