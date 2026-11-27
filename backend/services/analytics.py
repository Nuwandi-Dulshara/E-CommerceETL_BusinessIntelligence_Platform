"""All dashboard metrics come from a published warehouse, never raw Excel."""
from datetime import date
import duckdb
import pandas as pd

MEASURES = '''sum(gross_amount) gross_revenue, sum(return_amount) return_value,
 sum(net_amount) net_revenue,
 count(distinct case when not is_return then invoice_no end) orders,
 sum(gross_amount)/nullif(count(distinct case when not is_return then invoice_no end),0) aov'''
CUSTOMERS = '''SELECT customer_id,
 count(distinct case when not is_return then invoice_no end) orders,
 sum(net_amount) net_revenue, sum(gross_amount) gross_revenue
 FROM filtered WHERE customer_id IS NOT NULL GROUP BY customer_id
 HAVING count(distinct case when not is_return then invoice_no end)>0'''
QUERIES = {
    'Daily revenue': f'SELECT date, {MEASURES} FROM filtered GROUP BY date ORDER BY date',
    'Monthly revenue': f"SELECT date_trunc('month',date)::DATE AS date, {MEASURES} FROM filtered GROUP BY 1 ORDER BY 1",
    'Top products': '''SELECT stock_code, description, sum(net_amount) net_revenue,
       sum(case when not is_return then quantity else 0 end) units_sold,
       sum(case when is_return then -abs(quantity) else quantity end) net_units
       FROM filtered GROUP BY stock_code,description ORDER BY net_revenue DESC''',
    'Country sales': f'SELECT country, {MEASURES} FROM filtered GROUP BY country ORDER BY net_revenue DESC',
    'Customers': CUSTOMERS + ' ORDER BY net_revenue DESC',
    'Repeat customers': 'SELECT * FROM (' + CUSTOMERS + ') c WHERE orders>=2 ORDER BY orders DESC',
    'Returns': '''SELECT country, sum(return_amount) return_value, sum(gross_amount) gross_revenue,
       sum(return_amount)/nullif(sum(gross_amount),0) return_to_gross_ratio
       FROM filtered GROUP BY country ORDER BY return_value DESC''',
    'Recent transactions': '''SELECT date,invoice_no,stock_code,description,customer_id,country,quantity,
       gross_amount,return_amount,net_amount,is_return FROM filtered ORDER BY date DESC,invoice_no DESC LIMIT 100''',
}


def filters(start: date, end: date, countries: list[str]) -> tuple[str, list]:
    if start > end:
        raise ValueError('Start date must not be after end date')
    clause = 'date BETWEEN ? AND ?'
    params = [start, end]
    if countries:
        clause += ' AND country IN (' + ','.join('?' for _ in countries) + ')'
        params.extend(countries)
    return clause, params


def bounds(path: str) -> tuple:
    with duckdb.connect(path, read_only=True) as con:
        start, end = con.execute('SELECT min(date),max(date) FROM transactions').fetchone()
        countries = [r[0] for r in con.execute('SELECT DISTINCT country FROM transactions ORDER BY country').fetchall()]
    return start, end, countries


def query_sql(name: str, start: date, end: date, countries: list[str]) -> tuple[str, list]:
    if name not in QUERIES:
        raise ValueError('Only predefined read-only queries are allowed')
    where, params = filters(start, end, countries)
    return f'WITH filtered AS (SELECT * FROM transactions WHERE {where})\n' + QUERIES[name], params


def query(name: str, path: str, start: date, end: date, countries: list[str]) -> pd.DataFrame:
    sql, params = query_sql(name, start, end, countries)
    with duckdb.connect(path, read_only=True) as con:
        return con.execute(sql, params).fetchdf()


def metrics(path: str, start: date, end: date, countries: list[str]) -> dict | None:
    where, params = filters(start, end, countries)
    with duckdb.connect(path, read_only=True) as con:
        values = con.execute(f'''SELECT count(*),sum(gross_amount),sum(return_amount),sum(net_amount),
          count(distinct case when not is_return then invoice_no end),
          count(distinct case when not is_return then customer_id end)
          FROM transactions WHERE {where}''', params).fetchone()
    if not values[0]:
        return None
    _, gross, returns, net, orders, customers = values
    return {'gross': float(gross), 'returns': float(returns), 'net': float(net), 'orders': orders,
            'customers': customers, 'aov': float(gross)/orders if orders else None,
            'return_ratio': float(returns)/float(gross) if gross else None}
