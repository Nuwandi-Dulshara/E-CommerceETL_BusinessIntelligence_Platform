import streamlit as st
import numpy as np
from backend.services.analytics import query
from frontend.views.common import chart, kpis, download


def render(args):
    customers = query('Customers', *args)
    st.caption('Known customers with at least one sale in the selected period and countries. Unknown IDs are excluded; returns reduce net revenue.')
    if customers.empty:
        st.info('No known purchasing customers match these filters.')
        return
    count = len(customers)
    repeat = int((customers.orders >= 2).sum())
    kpis([('Known customers', f'{count:,}'), ('Repeat customers', f'{repeat:,}'), ('Repeat purchase rate', f'{repeat/count:.1%}')])
    customers['segment'] = np.select(
        [(customers.orders >= 3) & (customers.net_revenue >= 1000), customers.orders >= 2],
        ['High value repeat', 'Repeat'], default='Single order')
    chart(customers.head(10).assign(customer_id=lambda x: 'ID ' + x.customer_id.astype(str)).sort_values('net_revenue'),
          'bar', 'net_revenue', 'customer_id', 'Top customers · net revenue GBP', orientation='h')
    segments = customers.groupby('segment', as_index=False).agg(customers=('customer_id','count'), net_revenue=('net_revenue','sum'))
    chart(segments, 'bar', 'segment', 'customers', 'Customer segments')
    st.caption('High value repeat: ≥3 sale orders and ≥£1,000 net revenue. Repeat: ≥2 sale orders. Single order: one sale order. Thresholds apply within the current filters.')
    st.dataframe(customers, hide_index=True, use_container_width=True)
    download(customers, 'Download filtered customer results', 'customer_results')
