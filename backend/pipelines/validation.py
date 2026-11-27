"""Spark validation: preserve rejected source values and overlapping flags."""
from pyspark.sql import DataFrame, functions as F, Window
from backend.pipelines.ingestion import COLUMNS

NAMES = ['invoice_no', 'stock_code', 'description', 'quantity_raw', 'invoice_date_raw', 'unit_price_raw', 'customer_id', 'country']


def validate(bronze: DataFrame) -> tuple[DataFrame, DataFrame, dict]:
    df = bronze
    for source, target in zip(COLUMNS, NAMES):
        df = df.withColumn(target, F.when(F.length(F.trim(F.col(source))) > 0, F.trim(F.col(source))))
    df = (df.withColumn('quantity_decimal', F.col('quantity_raw').cast('decimal(38,12)'))
          .withColumn('quantity', F.when(F.col('quantity_raw').rlike(r'^[+-]?[0-9]+(?:\.0+)?$')
                                         & F.col('quantity_decimal').between(-2147483648, 2147483647),
                                         F.col('quantity_decimal').cast('int')))
          .withColumn('unit_price', F.col('unit_price_raw').cast('decimal(18,4)'))
          .withColumn('price_numeric', F.col('unit_price_raw').cast('decimal(38,18)'))
          .withColumn('invoice_date', F.coalesce(
              F.to_timestamp('invoice_date_raw', 'yyyy-MM-dd HH:mm:ss'),
              F.to_timestamp('invoice_date_raw', "yyyy-MM-dd'T'HH:mm:ss"),
              F.to_timestamp('invoice_date_raw', 'yyyy-MM-dd'),
              F.to_timestamp('invoice_date_raw', 'M/d/yyyy H:mm'))))
    rules = [
        (F.col('invoice_no').isNull(), 'MISSING_INVOICE'),
        (F.col('stock_code').isNull(), 'MISSING_STOCK_CODE'),
        (F.col('country').isNull(), 'MISSING_COUNTRY'),
        (F.col('invoice_date').isNull(), 'INVALID_DATE'),
        (F.col('quantity').isNull(), 'INVALID_QUANTITY'),
        (F.col('unit_price').isNull(), 'INVALID_PRICE'),
        (F.col('quantity') == 0, 'ZERO_QUANTITY'),
        ((F.col('price_numeric') < 0) | (F.col('unit_price_raw').cast('double') < 0)
         | F.col('unit_price_raw').rlike(r'^-(?=[0-9.]*[1-9])[0-9]*\.?[0-9]+([eE][+-]?[0-9]+)?$'), 'NEGATIVE_PRICE'),
    ]
    df = df.withColumn('reason_codes', F.filter(F.array(*[F.when(c, F.lit(r)) for c, r in rules]), lambda x: x.isNotNull()))
    # Count extra occurrences, rather than marking the first occurrence as a duplicate.
    repeated = Window.partitionBy(*[F.col(c) for c in COLUMNS]).orderBy('source_row_number')
    df = (df.withColumn('is_repeated', F.row_number().over(repeated) > 1)
          .withColumn('missing_customer', F.col('customer_id').isNull())
          .withColumn('zero_price', F.col('unit_price') == 0)
          .withColumn('is_flagged', F.col('is_repeated') | F.col('missing_customer') | F.coalesce(F.col('zero_price'), F.lit(False))))
    df = df.cache()
    rejected = df.filter(F.size('reason_codes') > 0).select(*COLUMNS, 'row_id', 'source_row_number', 'run_id', 'source_hash', 'reason_codes')
    silver = (df.filter(F.size('reason_codes') == 0)
              .withColumn('is_return', F.upper(F.col('invoice_no')).startswith('C') | (F.col('quantity') < 0))
              .withColumn('line_amount', (F.col('quantity') * F.col('unit_price')).cast('decimal(28,4)'))
              .select('row_id', 'source_row_number', 'source_hash', 'run_id', 'invoice_no', 'stock_code',
                      'description', 'quantity', 'invoice_date', 'unit_price', 'customer_id', 'country',
                      'is_return', 'line_amount', 'is_repeated', 'missing_customer', 'zero_price', 'is_flagged'))
    counts = df.agg(F.count('*').alias('input_count'),
                    F.sum(F.when(F.size('reason_codes') == 0, 1).otherwise(0)).alias('accepted_count'),
                    F.sum(F.col('is_flagged').cast('long')).alias('flagged_count'),
                    F.sum(F.col('is_repeated').cast('long')).alias('repeated_rows'),
                    F.sum(F.col('missing_customer').cast('long')).alias('missing_customers'),
                    F.sum(F.col('zero_price').cast('long')).alias('zero_price_rows')).first().asDict()
    counts = {k: int(v or 0) for k, v in counts.items()}
    counts['rejected_count'] = counts['input_count'] - counts['accepted_count']
    counts['reasons'] = {r[0]: r[1] for r in rejected.select(F.explode('reason_codes')).groupBy('col').count().collect()}
    return silver, rejected, counts
