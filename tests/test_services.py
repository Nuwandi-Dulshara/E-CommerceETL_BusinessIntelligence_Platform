from datetime import date
import pytest


def test_parameterized_filters():
    from backend.services.analytics import filters
    sql, params = filters(date(2011, 1, 1), date(2011, 1, 2), ["France' OR 1=1 --"])
    assert "France'" not in sql
    assert params == [date(2011, 1, 1), date(2011, 1, 2), "France' OR 1=1 --"]
    with pytest.raises(ValueError):
        filters(date(2011, 1, 2), date(2011, 1, 1), [])


def test_predefined_queries_and_metrics(tmp_path):
    import duckdb
    from backend.services.analytics import query, metrics, QUERIES
    path = tmp_path / 'warehouse.duckdb'
    with duckdb.connect(str(path)) as con:
        con.execute('''CREATE TABLE transactions AS SELECT * FROM (VALUES
          ('a',DATE '2011-01-01','UK','1','P','Apple',2,5.00,0.00,5.00,false),
          ('b',DATE '2011-01-02','UK','1','P','Apple',1,3.00,0.00,3.00,false),
          ('c',DATE '2011-01-02','UK','1','P','Apple',-1,0.00,2.50,-2.50,true),
          ('d',DATE '2011-01-03','France',NULL,'Q','Pear',1,4.00,0.00,4.00,false)
          ) t(invoice_no,date,country,customer_id,stock_code,description,quantity,gross_amount,return_amount,net_amount,is_return)''')
    args = (str(path), date(2011, 1, 1), date(2011, 1, 2), ['UK'])
    m = metrics(*args)
    assert m['net'] == 5.5 and m['gross'] == 8 and m['returns'] == 2.5
    assert m['orders'] == 2 and m['customers'] == 1 and m['aov'] == 4
    for name in QUERIES:
        assert not query(name, *args).empty
    customers = query('Customers', *args)
    assert customers.iloc[0]['orders'] == 2
    assert query('Monthly revenue', *args).iloc[0]['net_revenue'] == 5.5
    assert metrics(str(path), date(2000, 1, 1), date(2000, 1, 2), []) is None
    with pytest.raises(ValueError):
        query('DROP TABLE transactions', *args)


def test_publication_failure_is_atomic(tmp_path):
    import sqlite3
    from backend.pipelines.metadata import connect, publish, published, update_run, runs
    update_run(tmp_path, 'old', status='running')
    old = dict(run_id='old', snapshot_path='old.duckdb', stats={}, duration=1)
    publish(tmp_path, old)
    update_run(tmp_path, 'new', status='running')
    with connect(tmp_path) as con:
        con.execute("CREATE TRIGGER reject_publication BEFORE INSERT ON publication BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected failure'):
        publish(tmp_path, dict(run_id='new', snapshot_path='new.duckdb', stats={}, duration=1))
    assert published(tmp_path)['run_id'] == 'old'
    assert next(r for r in runs(tmp_path) if r['run_id'] == 'new')['status'] == 'running'


def test_failed_source_preserves_publication(tmp_path):
    from backend.pipelines.metadata import publish, published, update_run, runs
    from backend.pipelines.run import execute
    update_run(tmp_path, 'old', status='running')
    publish(tmp_path, dict(run_id='old', snapshot_path='old.duckdb', stats={}, duration=1))
    with pytest.raises(FileNotFoundError):
        execute(tmp_path / 'missing.xlsx', data=tmp_path, run_id='failed')
    assert published(tmp_path)['run_id'] == 'old'
    assert next(r for r in runs(tmp_path) if r['run_id'] == 'failed')['status'] == 'failed'


def test_cross_process_lock_and_active_state(tmp_path):
    import os
    import subprocess
    import sys
    from backend.services.pipeline_service import active
    from backend.pipelines.metadata import runs, writer_lock
    code = '''from pathlib import Path
import sys
from backend.pipelines.metadata import writer_lock,update_run,now
data=Path(sys.argv[1])
with writer_lock(data):
    update_run(data,'live',status='running',started_at=now())
    print('READY',flush=True)
    sys.stdin.readline()
'''
    proc = subprocess.Popen([sys.executable, '-u', '-c', code, str(tmp_path)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    try:
        assert proc.stdout.readline().strip() == 'READY'
        assert active(tmp_path)
        assert runs(tmp_path)[0]['status'] == 'running'
        with pytest.raises(RuntimeError, match='already running'):
            with writer_lock(tmp_path):
                pass
    finally:
        proc.communicate('\n', timeout=10)
    assert not active(tmp_path)
    assert runs(tmp_path)[0]['status'] == 'interrupted'
