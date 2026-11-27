"""Run with python -m pipelines.run --source <xlsx> [--sample-rows N]."""
import argparse
import json
import logging
import os
from pathlib import Path
import sys
import time
import uuid
from config import DATA, DEFAULT_SOURCE, spark_session
from pipelines.ingestion import download_source, ingest
from pipelines.validation import validate
from pipelines.transformation import gold_tables
from pipelines.warehouse import build_warehouse, verify_warehouse, finalize
from pipelines.metadata import writer_lock, update_run, stage, now, publish, runs


def execute(source: Path | None = None, sample_rows: int | None = None, download: bool = False,
            data: Path = DATA, run_id: str | None = None) -> dict:
    run_id = run_id or uuid.uuid4().hex
    run_path = data / 'runs' / run_id
    if not all(c.isalnum() or c == '-' for c in run_id):
        raise ValueError('Invalid run ID')
    with writer_lock(data):
        # Holding the OS lock proves previous running records have no live writer.
        for previous in runs(data):
            if previous['status'] == 'running' and previous['run_id'] != run_id:
                update_run(data, previous['run_id'], status='interrupted', ended_at=now(), error='Writer exited before completing this run')
        run_path.mkdir(parents=True, exist_ok=False)
        logs = data / 'logs'
        logs.mkdir(exist_ok=True)
        logger = logging.getLogger(f'pipeline.{run_id}')
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(logs / f'{run_id}.log', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(handler)
        start = time.monotonic()
        session = None
        update_run(data, run_id, source=str(source or 'UCI download'), sample_rows=sample_rows,
                   started_at=now(), status='running', run_path=str(run_path), pid=os.getpid())
        try:
            with stage(data, run_id, 'ingest', logger) as s:
                if download:
                    source = download_source(data)
                source = Path(source or DEFAULT_SOURCE).resolve()
                if not source.is_file():
                    raise FileNotFoundError(f'Excel source not found: {source}. Select an existing file or use --download.')
                session = spark_session()
                bronze, digest = ingest(session, source, run_path, run_id, sample_rows)
                s['row_count'] = bronze.count()
                update_run(data, run_id, source=str(source), source_hash=digest, input_count=s['row_count'])
            with stage(data, run_id, 'validate/clean', logger) as s:
                silver, quarantine, stats = validate(bronze)
                silver.write.format('delta').mode('overwrite').save(str(run_path / 'silver'))
                quarantine.write.format('delta').mode('overwrite').save(str(run_path / 'quarantine'))
                # Parquet mirrors serve bounded previews without starting another JVM in the UI.
                for name, df in [('bronze', bronze), ('silver', silver), ('quarantine', quarantine)]:
                    df.write.mode('overwrite').parquet(str(run_path / 'parquet' / name))
                s['row_count'] = stats['accepted_count']
                update_run(data, run_id, stats_json=json.dumps(stats), **{k: stats[k] for k in ['accepted_count', 'rejected_count', 'flagged_count']})
            with stage(data, run_id, 'transform', logger) as s:
                silver = session.read.format('delta').load(str(run_path / 'silver'))
                tables = gold_tables(session, silver)
                s['row_count'] = silver.count()
            with stage(data, run_id, 'gold', logger) as s:
                for name, df in tables.items():
                    df.write.format('delta').mode('overwrite').save(str(run_path / 'gold' / name))
                    session.read.format('delta').load(str(run_path / 'gold' / name)).write.mode('overwrite').parquet(str(run_path / 'parquet' / name))
                s['row_count'] = stats['accepted_count']
            with stage(data, run_id, 'warehouse', logger):
                warehouse = build_warehouse(run_path)
            with stage(data, run_id, 'verify/publish', logger) as s:
                stats['checks'] = verify_warehouse(warehouse, stats)
                # Confirm physically persisted layers before switching the published pointer.
                actual_quarantine = session.read.format('delta').load(str(run_path / 'quarantine')).count()
                if stats['input_count'] != silver.count() + actual_quarantine:
                    raise ValueError('Persisted Bronze/Silver/quarantine counts do not reconcile')
                warehouse = finalize(warehouse)
                result = {'run_id': run_id, 'snapshot_path': str(warehouse), 'run_path': str(run_path),
                          'published_at': now(), 'sample_rows': sample_rows, 'stats': stats}
                (run_path / 'quality.json').write_text(json.dumps(stats, indent=2), encoding='utf-8')
                s['row_count'] = stats['accepted_count']
            result['duration'] = time.monotonic()-start
            publish(data, result)
            logger.info(json.dumps({'status': 'succeeded', 'snapshot': str(warehouse)}))
            return result
        except Exception as exc:
            logger.exception('Pipeline failed')
            update_run(data, run_id, status='failed', ended_at=now(), duration=time.monotonic()-start, error=str(exc)[:8000])
            raise
        finally:
            if session:
                try:
                    session.stop()
                except Exception:
                    logger.exception('Spark shutdown failed')
            logger.removeHandler(handler)
            handler.close()


def main() -> int:
    parser = argparse.ArgumentParser(description='CommerceFlow Spark/Delta ETL')
    parser.add_argument('--source', type=Path)
    parser.add_argument('--download', action='store_true')
    parser.add_argument('--sample-rows', type=int)
    parser.add_argument('--run-id')
    args = parser.parse_args()
    if args.sample_rows is not None and args.sample_rows < 1:
        parser.error('--sample-rows must be positive')
    try:
        result = execute(**vars(args))
        print(json.dumps(result))
        return 0
    except Exception as exc:
        print(f'Pipeline failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
