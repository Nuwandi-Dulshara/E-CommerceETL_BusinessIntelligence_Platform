"""Cached UCI acquisition and lossless textual Bronze ingestion."""
import hashlib
import urllib.request
from pathlib import Path
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

COLUMNS = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
DOWNLOAD_URL = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'


def download_source(data: Path) -> Path:
    import zipfile
    target = data / 'raw' / 'online_retail.xlsx'
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    archive = target.with_suffix('.zip.part')
    urllib.request.urlretrieve(DOWNLOAD_URL, archive)
    with zipfile.ZipFile(archive) as z:
        member = next(n for n in z.namelist() if n.lower().endswith('.xlsx'))
        partial = target.with_suffix('.xlsx.part')
        with z.open(member) as src, partial.open('wb') as dst:
            import shutil
            shutil.copyfileobj(src, dst)
        partial.replace(target)
    archive.unlink()
    return target


def excel_to_csv(source: Path, target: Path, sample_rows: int | None) -> str:
    if sample_rows is not None and sample_rows < 1:
        raise ValueError('sample_rows must be positive')
    with source.open('rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    frame = pd.read_excel(source, dtype=str, keep_default_na=False, nrows=sample_rows, engine='openpyxl')
    missing = sorted(set(COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f'Missing required columns: {", ".join(missing)}')
    frame = frame[COLUMNS].copy()
    frame['source_row_number'] = range(2, len(frame) + 2)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False)
    return digest


def ingest(spark: SparkSession, source: Path, run_path: Path, run_id: str,
           sample_rows: int | None) -> tuple[DataFrame, str]:
    csv = run_path / 'input.csv'
    digest = excel_to_csv(source, csv, sample_rows)
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in COLUMNS]
                          + [T.StructField('source_row_number', T.LongType(), False)])
    bronze = (spark.read.schema(schema).option('header', True).option('multiLine', True)
              .option('escape', '"').csv(str(csv))
              .withColumn('source_hash', F.lit(digest))
              .withColumn('run_id', F.lit(run_id))
              .withColumn('ingested_at', F.current_timestamp())
              .withColumn('row_id', F.sha2(F.concat_ws(':', F.lit(digest), F.col('source_row_number')), 256)))
    bronze.write.format('delta').mode('overwrite').save(str(run_path / 'bronze'))
    return spark.read.format('delta').load(str(run_path / 'bronze')), digest
