"""Safe subprocess orchestration; no shell command interpolation."""
from datetime import datetime, timezone
from pathlib import Path
import os
import subprocess
import sys
import uuid
from config import DATA, ROOT
from pipelines.metadata import writer_lock, runs, update_run, now


def active(data: Path = DATA) -> bool:
    try:
        with writer_lock(data):
            for run in runs(data):
                if run['status'] == 'queued':
                    age = (datetime.now(timezone.utc)-datetime.fromisoformat(run['started_at'])).total_seconds()
                    if age < 30:
                        return True
                    update_run(data, run['run_id'], status='failed', ended_at=now(), error='Subprocess did not acquire the pipeline lock within 30 seconds. See console log.')
                elif run['status'] == 'running':
                    update_run(data, run['run_id'], status='interrupted', ended_at=now(), error='Pipeline process exited without completing its run')
    except RuntimeError:
        return True
    return False


def launch(source: str, sample_rows: int | None, download: bool, data: Path = DATA) -> str:
    # A separate launch lock closes the check/start race between browser sessions.
    with writer_lock(data / 'launcher'):
        if active(data):
            raise RuntimeError('A pipeline is already running')
        if not download and not Path(source).is_file():
            raise ValueError('Select an existing Excel file')
        run_id = uuid.uuid4().hex
        args = [sys.executable, '-m', 'pipelines.run', '--run-id', run_id]
        args += ['--download'] if download else ['--source', source]
        if sample_rows is not None:
            if sample_rows < 1:
                raise ValueError('Sample rows must be positive')
            args += ['--sample-rows', str(sample_rows)]
        logdir = data / 'logs'
        logdir.mkdir(parents=True, exist_ok=True)
        update_run(data, run_id, source=source or 'UCI download', sample_rows=sample_rows, status='queued', started_at=now())
        env = os.environ.copy()
        env['COMMERCEFLOW_DATA'] = str(data)
        try:
            with (logdir / f'{run_id}.console.log').open('w', encoding='utf-8') as log:
                proc = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=env,
                                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
            update_run(data, run_id, pid=proc.pid)
        except Exception as exc:
            update_run(data, run_id, status='failed', error=str(exc), ended_at=now())
            raise
        return run_id


def log_text(run_id: str, data: Path = DATA) -> str:
    if not run_id or not all(c.isalnum() or c == '-' for c in run_id):
        raise ValueError('Invalid run ID')
    parts = []
    for suffix in ['.log', '.console.log']:
        path = data / 'logs' / (run_id + suffix)
        if path.exists():
            parts.append(path.read_text(encoding='utf-8', errors='replace'))
    return '\n'.join(parts)
