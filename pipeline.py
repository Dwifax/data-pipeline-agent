#!/usr/bin/env python3
"""Data Pipeline Agent - Automated ETL with multi-source extraction."""
import json, csv, sqlite3, sys, hashlib, logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pipeline")

class Pipeline:
    """ETL Pipeline: Extract -> Transform -> Load."""
    def __init__(self, name="default"):
        self.name = name
        self.sources = []
        self.transforms = []
        self.metrics = {"start": None, "end": None, "extracted": 0, "transformed": 0, "errors": 0}
    
    def add_csv(self, path, delimiter=","):
        self.sources.append(("csv", path, delimiter))
        return self
    
    def add_json(self, path, key="data"):
        self.sources.append(("json", path, key))
        return self
    
    def add_sqlite(self, db, query):
        self.sources.append(("sqlite", db, query))
        return self
    
    def extract(self):
        records = []
        for stype, *args in self.sources:
            try:
                if stype == "csv":
                    path, delim = args
                    with open(path, newline="") as f:
                        records.extend(list(csv.DictReader(f, delimiter=delim)))
                    log.info(f"CSV: {path} -> {len(records)} rows")
                elif stype == "json":
                    path, key = args
                    data = json.loads(Path(path).read_text())
                    items = data if isinstance(data, list) else data.get(key, [])
                    records.extend(items)
                    log.info(f"JSON: {path} -> {len(items)} items")
                elif stype == "sqlite":
                    db, query = args
                    conn = sqlite3.connect(db)
                    conn.row_factory = sqlite3.Row
                    rows = [dict(r) for r in conn.execute(query)]
                    records.extend(rows)
                    conn.close()
                    log.info(f"SQLite: {db} -> {len(rows)} rows")
            except Exception as e:
                log.error(f"Extract error ({stype}): {e}")
                self.metrics["errors"] += 1
        self.metrics["extracted"] = len(records)
        return records
    
    def deduplicate(self, records, key_fields):
        seen = set()
        result = []
        for r in records:
            h = hashlib.md5(json.dumps({k: r.get(k) for k in key_fields}, sort_keys=True).encode()).hexdigest()
            if h not in seen:
                seen.add(h)
                result.append(r)
        return result
    
    def rename_fields(self, records, mapping):
        return [{mapping.get(k, k): v for k, v in r.items()} for r in records]
    
    def filter_rows(self, records, predicate):
        return [r for r in records if predicate(r)]
    
    def export_csv(self, records, path):
        if not records: return
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=records[0].keys())
            w.writeheader()
            w.writerows(records)
        log.info(f"Exported {len(records)} rows to {path}")
    
    def export_json(self, records, path):
        Path(path).write_text(json.dumps(records, indent=2, ensure_ascii=False))
        log.info(f"Exported {len(records)} records to {path}")
    
    def run(self, output_path=None):
        self.metrics["start"] = datetime.now().isoformat()
        log.info(f"Pipeline '{self.name}' started")
        records = self.extract()
        self.metrics["transformed"] = len(records)
        self.metrics["end"] = datetime.now().isoformat()
        if output_path:
            if output_path.endswith(".csv"):
                self.export_csv(records, output_path)
            else:
                self.export_json(records, output_path)
        log.info(f"Pipeline complete: {self.metrics}")
        return {"metrics": self.metrics, "data": records}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pipeline.py <config.json>")
        sys.exit(1)
    config = json.loads(Path(sys.argv[1]).read_text())
    p = Pipeline(config.get("name", "default"))
    for src in config.get("sources", []):
        if src["type"] == "csv": p.add_csv(src["path"])
        elif src["type"] == "json": p.add_json(src["path"])
        elif src["type"] == "sqlite": p.add_sqlite(src["db"], src["query"])
    result = p.run(config.get("output"))
    print(json.dumps(result["metrics"], indent=2))
