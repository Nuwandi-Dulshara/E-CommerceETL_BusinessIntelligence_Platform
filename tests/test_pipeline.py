from decimal import Decimal
import pytest


@pytest.fixture(scope='session')
def spark():
    from backend.config import spark_session
    session = spark_session('CommerceFlow-tests')
    yield session
    session.stop()


@pytest.fixture
def source(tmp_path):
    import pandas as pd
    columns = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
    rows = [
        ['100', 'A', 'Apple', 2, '2011-01-01 10:00:00', '2.50', '1', 'UK'],
        ['100', 'A', 'Apple', 2, '2011-01-01 10:00:00', '2.50', '1', 'UK'],
        ['101', 'A', None, 1, '2011-01-02 10:00:00', '3.00', '1', 'UK'],
        ['C102', 'A', 'Apple', 1, '2011-01-02 10:00:00', '2.50', '1', 'UK'],
        ['103', 'B', None, -2, '2011-01-03 10:00:00', '1.25', None, 'France'],
        ['104', 'B', None, 1, '2011-01-03 10:00:00', '0', None, 'France'],
        [None, '', None, 0, 'bad', '-1', None, ''],
        ['105', 'B', 'Banana', '1.5', '2011-01-03 10:00:00', 'bad', '2', 'UK'],
    ]
    path = tmp_path / 'fixture.xlsx'
    pd.DataFrame(rows, columns=columns).to_excel(path, index=False)
    return path


def test_missing_columns(tmp_path):
    import pandas as pd
    from backend.pipelines.ingestion import excel_to_csv
    path = tmp_path / 'bad.xlsx'
    pd.DataFrame({'InvoiceNo': ['1']}).to_excel(path, index=False)
    with pytest.raises(ValueError, match='Missing required columns'):
        excel_to_csv(path, tmp_path / 'bad.csv', None)


def test_validation_and_gold(spark, source, tmp_path):
    from backend.pipelines.ingestion import ingest
    from backend.pipelines.validation import validate
    from backend.pipelines.transformation import gold_tables
    bronze, _ = ingest(spark, source, tmp_path, 'test', None)
    silver, rejected, stats = validate(bronze)
    assert bronze.count() == 8
    assert silver.count() == 6
    assert rejected.count() == 2
    assert stats['repeated_rows'] == 1
    assert stats['missing_customers'] == 3
    assert stats['zero_price_rows'] == 1
    reasons = set(rejected.select('reason_codes').first()[0])
    assert {'MISSING_INVOICE', 'MISSING_STOCK_CODE', 'MISSING_COUNTRY', 'INVALID_DATE', 'ZERO_QUANTITY', 'NEGATIVE_PRICE'} <= reasons
    tables = gold_tables(spark, silver)
    fact = tables['fact_transactions']
    totals = fact.selectExpr('sum(gross_amount)', 'sum(return_amount)').first()
    assert totals[0] == Decimal('13.0000')
    assert totals[1] == Decimal('5.0000')
    assert tables['dim_customer'].count() == 2
    assert tables['dim_product'].filter("stock_code = 'B'").first()['description'] == 'Unknown product'


def test_lock_excludes_second_writer(tmp_path):
    from backend.pipelines.metadata import writer_lock
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError, match='already running'):
            with writer_lock(tmp_path):
                pass
    with writer_lock(tmp_path):
        pass


def test_rejects_fractional_quantity_and_tiny_negative_price(spark, tmp_path):
    import pandas as pd
    from backend.pipelines.ingestion import COLUMNS, ingest
    from backend.pipelines.validation import validate
    source = tmp_path / 'precision.xlsx'
    rows = [
        ['1','A','Item','1.0000000000001','2011-01-01','1','1','UK'],
        ['2','A','Item','1','2011-01-01','-1e-20','1','UK'],
        ['3','A','Item','2147483648','2011-01-01','1','1','UK'],
    ]
    pd.DataFrame(rows, columns=COLUMNS).to_excel(source,index=False)
    bronze, _ = ingest(spark, source, tmp_path, 'precision', None)
    silver, rejected, stats = validate(bronze)
    assert silver.count() == 0
    assert rejected.count() == 3
    assert stats['reasons']['INVALID_QUANTITY'] == 2
    assert stats['reasons']['NEGATIVE_PRICE'] == 1
