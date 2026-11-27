"""Publish only fully validated DuckDB snapshots from Spark Parquet exports."""
from pathlib import Path
import os
import duckdb
from backend.config import ROOT

TABLES = ['fact_transactions', 'dim_product', 'dim_customer', 'dim_date', 'agg_daily_sales', 'agg_country_sales', 'agg_product_sales']


def build_warehouse(run_path: Path) -> Path:
    target = run_path / 'warehouse.staging.duckdb'
    with duckdb.connect(str(target)) as con:
        for table in TABLES:
            glob = str(run_path / 'parquet' / table / '*.parquet')
            con.execute(f'CREATE TABLE {table} AS SELECT * FROM read_parquet(?)', [glob])
        con.execute((ROOT / 'backend' / 'sql' / 'analytics.sql').read_text(encoding='utf-8'))
    return target


def verify_warehouse(path: Path, stats: dict) -> list[dict]:
    checks = []
    def check(name, condition):
        checks.append({'check': name, 'passed': bool(condition)})
    with duckdb.connect(str(path), read_only=True) as con:
        fact = con.execute('SELECT count(*) FROM fact_transactions').fetchone()[0]
        check('Bronze = Silver + quarantine', stats['input_count'] == stats['accepted_count'] + stats['rejected_count'])
        check('Fact = accepted Silver', fact == stats['accepted_count'])
        check('Source row IDs unique', con.execute('SELECT count(*)=count(DISTINCT row_id) FROM fact_transactions').fetchone()[0])
        for dim, key in [('dim_product', 'product_key'), ('dim_customer', 'customer_key'), ('dim_date', 'date_key')]:
            check(f'{dim} keys unique', con.execute(f'SELECT count(*)=count(DISTINCT {key}) FROM {dim}').fetchone()[0])
            check(f'{dim} foreign keys resolve', con.execute(f'SELECT count(*) FROM fact_transactions f LEFT JOIN {dim} d USING({key}) WHERE d.{key} IS NULL').fetchone()[0] == 0)
        total = con.execute('SELECT coalesce(sum(gross_amount-return_amount),0) FROM fact_transactions').fetchone()[0]
        for name in ['agg_daily_sales', 'agg_country_sales', 'agg_product_sales']:
            value = con.execute(f'SELECT coalesce(sum(net_sales),0) FROM {name}').fetchone()[0]
            check(f'{name} net revenue reconciles', value == total)
    failed = [c['check'] for c in checks if not c['passed']]
    if failed:
        raise ValueError('Warehouse validation failed: ' + ', '.join(failed))
    return checks


def finalize(path: Path) -> Path:
    target = path.with_name('warehouse.duckdb')
    os.replace(path, target)
    return target
