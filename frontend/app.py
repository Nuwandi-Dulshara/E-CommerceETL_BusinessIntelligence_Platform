"""CommerceFlow — local lakehouse and business intelligence."""
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
from backend.config import ROOT, DATA
from backend.pipelines.metadata import published
from backend.services.analytics import bounds
from frontend.views import overview, sales, customers, pipelines, lakehouse, quality, sql_analytics

st.set_page_config(page_title='CommerceFlow · Retail intelligence', page_icon='◈', layout='wide', initial_sidebar_state='expanded')
st.markdown('<style>' + (ROOT / 'frontend' / 'assets' / 'styles.css').read_text(encoding='utf-8') + '</style>', unsafe_allow_html=True)
snapshot = published(DATA)
pages = ['Overview', 'Sales Analytics', 'Customer Analytics', 'ETL Pipelines', 'Lakehouse Explorer', 'Data Quality', 'SQL Analytics']

with st.sidebar:
    st.markdown('<div class="brand">◈ Commerce<span>Flow</span></div><div class="brand-sub">RETAIL INTELLIGENCE</div>', unsafe_allow_html=True)
    page = st.radio('Workspace', pages, label_visibility='collapsed', key='navigation')
    st.divider()
    st.caption('ANALYTICS FILTERS')
    args = None
    if snapshot:
        if st.session_state.get('snapshot_id') != snapshot['run_id']:
            st.cache_data.clear()
            for key in ['date_filter', 'country_filter', 'sql_result']:
                st.session_state.pop(key, None)
            st.session_state['snapshot_id'] = snapshot['run_id']
        lower, upper, countries_list = bounds(snapshot['snapshot_path'])
        if lower:
            def reset_filters():
                st.session_state['date_filter'] = (lower, upper)
                st.session_state['country_filter'] = []
            dates = st.date_input('Date range', value=(lower, upper), min_value=lower, max_value=upper, key='date_filter')
            countries_selected = st.multiselect('Countries', countries_list, placeholder='All countries', key='country_filter')
            st.button('Reset filters', on_click=reset_filters, use_container_width=True)
            if len(dates) == 2:
                args = (snapshot['snapshot_path'], dates[0], dates[1], countries_selected)
            else:
                st.warning('Choose both boundary dates.')
    st.divider()
    st.caption('Spark + Delta Lake → DuckDB')
    st.caption('UCI Online Retail · GBP')
    st.caption('Local, open-source analytics')

st.markdown('<div class="eyebrow">COMMERCEFLOW / ANALYTICS WORKSPACE</div>', unsafe_allow_html=True)
st.title(page)
if snapshot:
    scope = f"Sample · first {snapshot['sample_rows']:,} rows" if snapshot['sample_rows'] else 'Full dataset'
    st.caption(f"{scope}  |  Last successful refresh: {snapshot['published_at']}  |  Currency: GBP (£)")
else:
    st.caption('Your local retail lakehouse')

try:
    if page == 'ETL Pipelines':
        pipelines.render()
    elif not snapshot:
        st.info('No published data yet. Open ETL Pipelines to process the original UCI dataset.')
    elif page == 'Lakehouse Explorer':
        lakehouse.render(snapshot)
    elif page == 'Data Quality':
        quality.render(snapshot)
    elif args is None:
        st.info('Select a complete date range, or run a pipeline if the snapshot has no accepted records.')
    else:
        {'Overview': overview.render, 'Sales Analytics': sales.render,
         'Customer Analytics': customers.render, 'SQL Analytics': sql_analytics.render}[page](args)
except (ValueError, OSError) as exc:
    st.error(str(exc))
