"""Bounded previews from Parquet mirrors of committed Delta layers."""
from pathlib import Path
import duckdb
from backend.pipelines.warehouse import TABLES

LAYERS = {'Bronze': ['bronze'], 'Silver': ['silver', 'quarantine'], 'Gold': TABLES}


def inspect_table(run_path: str, name: str, limit: int = 100):
    if name not in ['bronze', 'silver', 'quarantine'] + TABLES:
        raise ValueError('Unknown lakehouse table')
    if not 1 <= limit <= 500:
        raise ValueError('Preview limit must be between 1 and 500')
    glob = str(Path(run_path) / 'parquet' / name / '*.parquet')
    with duckdb.connect() as con:
        count = con.execute('SELECT count(*) FROM read_parquet(?)', [glob]).fetchone()[0]
        schema = con.execute('DESCRIBE SELECT * FROM read_parquet(?)', [glob]).fetchdf()
        preview = con.execute('SELECT * FROM read_parquet(?) LIMIT ?', [glob, limit]).fetchdf()
    return count, schema, preview


def rejected_csv(run_path: str) -> bytes:
    glob = str(Path(run_path) / 'parquet' / 'quarantine' / '*.parquet')
    with duckdb.connect() as con:
        return con.execute('SELECT * FROM read_parquet(?)', [glob]).fetchdf().to_csv(index=False).encode('utf-8')
