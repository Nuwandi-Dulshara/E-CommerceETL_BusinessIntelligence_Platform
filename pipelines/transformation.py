"""Deterministic star schema and Spark SQL business aggregates."""
from pyspark.sql import DataFrame, SparkSession, functions as F, Window


def gold_tables(spark: SparkSession, silver: DataFrame) -> dict[str, DataFrame]:
    latest = Window.partitionBy('stock_code').orderBy(
        F.col('description').isNotNull().desc(), F.col('invoice_date').desc(), F.col('source_row_number').desc())
    products = (silver.withColumn('rank', F.row_number().over(latest)).filter('rank = 1')
                .select(F.sha2('stock_code', 256).alias('product_key'), 'stock_code',
                        F.coalesce('description', F.lit('Unknown product')).alias('description')))
    customers = (silver.filter('customer_id is not null').select('customer_id').distinct()
                 .select(F.sha2('customer_id', 256).alias('customer_key'), 'customer_id'))
    unknown = spark.range(1).select(F.lit('UNKNOWN').alias('customer_key'), F.lit(None).cast('string').alias('customer_id'))
    customers = customers.unionByName(unknown)
    dates = (silver.select(F.to_date('invoice_date').alias('date')).distinct()
             .select(F.date_format('date', 'yyyyMMdd').cast('int').alias('date_key'), 'date',
                     F.year('date').alias('year'), F.month('date').alias('month'),
                     F.quarter('date').alias('quarter'), F.dayofmonth('date').alias('day')))
    zero = F.lit(0).cast('decimal(28,4)')
    fact = silver.select('row_id', 'invoice_no', F.sha2('stock_code', 256).alias('product_key'),
                         F.coalesce(F.sha2('customer_id', 256), F.lit('UNKNOWN')).alias('customer_key'),
                         F.date_format('invoice_date', 'yyyyMMdd').cast('int').alias('date_key'),
                         'country', 'quantity', 'unit_price',
                         F.when(~F.col('is_return'), F.col('line_amount')).otherwise(zero).alias('gross_amount'),
                         F.when(F.col('is_return'), F.abs('line_amount')).otherwise(zero).alias('return_amount'), 'is_return')
    fact.createOrReplaceTempView('fact')
    dates.createOrReplaceTempView('dates')
    common = 'sum(gross_amount) gross_sales, sum(return_amount) returns, sum(gross_amount-return_amount) net_sales'
    orders = 'count(distinct case when not is_return then invoice_no end) orders'
    return {
        'fact_transactions': fact, 'dim_product': products, 'dim_customer': customers, 'dim_date': dates,
        'agg_daily_sales': spark.sql(f'SELECT date, {common}, {orders} FROM fact JOIN dates USING(date_key) GROUP BY date'),
        'agg_country_sales': spark.sql(f'SELECT country, {common}, {orders} FROM fact GROUP BY country'),
        'agg_product_sales': spark.sql(f'SELECT product_key, {common}, sum(case when not is_return then quantity else 0 end) units, sum(case when is_return then -abs(quantity) else quantity end) net_units FROM fact GROUP BY product_key'),
    }
