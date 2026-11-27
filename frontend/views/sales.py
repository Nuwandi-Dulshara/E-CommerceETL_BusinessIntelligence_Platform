import streamlit as st
from backend.services.analytics import query, metrics
from frontend.views.common import chart, kpis, money, download, no_matches


def render(args):
    m = metrics(*args)
    if no_matches(m):
        return
    ratio = 'N/A' if m['return_ratio'] is None else f"{m['return_ratio']:.2%}"
    kpis([('Net revenue · GBP', money(m['net'])), ('Return value · GBP', money(m['returns'])), ('Returns / gross sales', ratio)])
    interval = st.radio('Trend interval', ['Daily', 'Monthly'], horizontal=True)
    trend = query(f'{interval} revenue', *args)
    chart(trend, 'line', 'date', ['gross_revenue', 'net_revenue', 'return_value'], 'Sales trends · GBP')
    left, right = st.columns(2)
    with left:
        chart(trend, 'line', 'date', 'orders', 'Orders over time')
    with right:
        chart(trend, 'line', 'date', 'aov', 'Average order value · GBP')
    download(trend, 'Download filtered sales trends', 'sales_trends')
    products = query('Top products', *args)
    sort = st.selectbox('Rank products by', ['net_revenue', 'units_sold', 'net_units'], format_func=lambda s: s.replace('_',' ').title())
    chart(products.nlargest(10, sort).sort_values(sort), 'bar', sort, 'description', 'Top 10 products', orientation='h')
    st.caption('Units sold exclude returns. Net units subtract absolute returned quantities.')
    download(products, 'Download filtered product results', 'sales_products')
    countries = query('Country sales', *args)
    chart(countries, 'bar', 'country', 'net_revenue', 'Country sales · GBP')
    download(countries, 'Download filtered country results', 'sales_countries')
