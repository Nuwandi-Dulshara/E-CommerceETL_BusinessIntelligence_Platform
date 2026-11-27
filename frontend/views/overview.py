import streamlit as st
from backend.services.analytics import query, metrics
from frontend.views.common import chart, kpis, money, no_matches


def render(args):
    m = metrics(*args)
    if no_matches(m):
        return
    kpis([('Net revenue · GBP', money(m['net'])), ('Gross revenue · GBP', money(m['gross'])),
          ('Sale orders', f"{m['orders']:,}"), ('Known customers', f"{m['customers']:,}"),
          ('Average order value', money(m['aov'])), ('Return value · GBP', money(m['returns']))])
    st.write('')
    interval = st.radio('Revenue interval', ['Daily', 'Monthly'], horizontal=True)
    trend = query('Monthly revenue' if interval == 'Monthly' else 'Daily revenue', *args)
    chart(trend, 'line', 'date', 'net_revenue', 'Revenue performance · GBP', markers=True)
    left, right = st.columns(2)
    with left:
        products = query('Top products', *args).head(10).sort_values('net_revenue')
        chart(products, 'bar', 'net_revenue', 'description', 'Top 10 products · net revenue', orientation='h')
    with right:
        chart(query('Country sales', *args).head(10), 'bar', 'country', 'net_revenue', 'Revenue by country · top 10')
    chart(trend, 'bar', 'date', ['gross_revenue', 'return_value'], 'Gross sales and returns · GBP', barmode='group')
    st.subheader('Recent transactions')
    st.caption('Latest 100 matching transaction lines. Unknown customer IDs remain empty.')
    st.dataframe(query('Recent transactions', *args), hide_index=True, use_container_width=True)
