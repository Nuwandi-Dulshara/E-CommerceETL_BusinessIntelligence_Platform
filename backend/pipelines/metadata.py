"""SQLite operational records and an OS-released cross-process writer lock."""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sqlite3
import time


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(data: Path) -> sqlite3.Connection:
    data.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(data / 'metadata.sqlite', timeout=20)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA journal_mode=WAL')
    con.executescript('''
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, source TEXT, source_hash TEXT, sample_rows INTEGER,
            started_at TEXT, ended_at TEXT, status TEXT, stage TEXT, duration REAL,
            input_count INTEGER, accepted_count INTEGER, rejected_count INTEGER, flagged_count INTEGER,
            error TEXT, snapshot_path TEXT, run_path TEXT, stats_json TEXT, pid INTEGER);
        CREATE TABLE IF NOT EXISTS stages (
            run_id TEXT, stage TEXT, started_at TEXT, ended_at TEXT, status TEXT,
            duration REAL, row_count INTEGER, error TEXT, PRIMARY KEY(run_id,stage));
        CREATE TABLE IF NOT EXISTS publication (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
    ''')
    return con


def update_run(data: Path, run_id: str, **values) -> None:
    allowed = {'source', 'source_hash', 'sample_rows', 'started_at', 'ended_at', 'status', 'stage', 'duration',
               'input_count', 'accepted_count', 'rejected_count', 'flagged_count', 'error', 'snapshot_path',
               'run_path', 'stats_json', 'pid'}
    if not values.keys() <= allowed:
        raise ValueError('Invalid run fields')
    with connect(data) as con:
        con.execute('INSERT OR IGNORE INTO runs(run_id) VALUES (?)', (run_id,))
        con.execute(f'UPDATE runs SET {",".join(k+"=?" for k in values)} WHERE run_id=?', (*values.values(), run_id))


def runs(data: Path) -> list[dict]:
    with connect(data) as con:
        return [dict(r) for r in con.execute('SELECT * FROM runs ORDER BY started_at DESC')]


def stages(data: Path, run_id: str) -> list[dict]:
    with connect(data) as con:
        return [dict(r) for r in con.execute('SELECT * FROM stages WHERE run_id=? ORDER BY started_at', (run_id,))]


@contextmanager
def writer_lock(data: Path):
    data.mkdir(parents=True, exist_ok=True)
    handle = (data / 'pipeline.lock').open('a+b')
    if os.fstat(handle.fileno()).st_size == 0:
        handle.write(b'0')
        handle.flush()
    handle.seek(0)
    acquired = False
    try:
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError as exc:
            raise RuntimeError('A pipeline is already running') from exc
        yield
    finally:
        if acquired:
            handle.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


@contextmanager
def stage(data: Path, run_id: str, name: str, logger):
    started = time.monotonic()
    update_run(data, run_id, stage=name)
    with connect(data) as con:
        con.execute('INSERT OR REPLACE INTO stages(run_id,stage,started_at,status) VALUES(?,?,?,?)', (run_id, name, now(), 'running'))
    logger.info(json.dumps({'stage': name, 'status': 'started'}))
    result = {'row_count': None}
    try:
        yield result
    except Exception as exc:
        with connect(data) as con:
            con.execute('UPDATE stages SET status=?,ended_at=?,duration=?,error=? WHERE run_id=? AND stage=?',
                        ('failed', now(), time.monotonic()-started, str(exc)[:4000], run_id, name))
        raise
    else:
        with connect(data) as con:
            con.execute('UPDATE stages SET status=?,ended_at=?,duration=?,row_count=? WHERE run_id=? AND stage=?',
                        ('succeeded', now(), time.monotonic()-started, result['row_count'], run_id, name))
        logger.info(json.dumps({'stage': name, 'status': 'succeeded', **result}))


def published(data: Path) -> dict | None:
    with connect(data) as con:
        record = con.execute('SELECT payload FROM publication WHERE id=1').fetchone()
    return json.loads(record[0]) if record else None


def publish(data: Path, payload: dict) -> None:
    # One SQLite transaction makes run success and the snapshot pointer indivisible.
    with connect(data) as con:
        con.execute('UPDATE runs SET status=?,ended_at=?,duration=?,snapshot_path=?,stats_json=? WHERE run_id=?',
                    ('succeeded', now(), payload['duration'], payload['snapshot_path'], json.dumps(payload['stats']), payload['run_id']))
        con.execute('INSERT OR REPLACE INTO publication(id,payload) VALUES(1,?)', (json.dumps(payload),))
