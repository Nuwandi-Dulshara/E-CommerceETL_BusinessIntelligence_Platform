import json
import pandas as pd
import streamlit as st
from backend.config import DATA
from backend.pipelines.metadata import runs
from backend.services.lakehouse_service import inspect_table, rejected_csv
from frontend.views.common import chart, kpis


def render(snapshot):
    history = [r for r in runs(DATA) if r['stats_json']]
    if not history:
        st.info('No completed quality report is available.')
        return
    chosen = st.selectbox('Quality report run', [r['run_id'] for r in history])
    run = next(r for r in history if r['run_id'] == chosen)
    st.caption(f"Run status: {run['status']} · Started: {run['started_at']} (UTC)")
    stats = json.loads(run['stats_json'])
    kpis([('Input rows', f"{stats['input_count']:,}"), ('Accepted rows', f"{stats['accepted_count']:,}"),
          ('Rejected rows', f"{stats['rejected_count']:,}"), ('Flagged rows', f"{stats['flagged_count']:,}"),
          ('Acceptance rate', f"{stats['accepted_count']/stats['input_count']:.2%}" if stats['input_count'] else 'N/A'),
          ('Missing customer IDs', f"{stats['missing_customers']:,}")])
    st.caption('Accepted and rejected rows partition input. Quality flags overlap and can apply to accepted or rejected rows; they are not an additional rejection total.')
    st.write(f"**Repeated source rows retained:** {stats['repeated_rows']:,} · **Zero-price lines:** {stats['zero_price_rows']:,}")
    if stats['reasons']:
        reasons = pd.DataFrame(stats['reasons'].items(), columns=['reason','rows'])
        chart(reasons, 'bar', 'reason', 'rows', 'Rejection reasons · a row may have several')
    else:
        st.success('No rejected records in this run.')
    st.subheader('Validation checks')
    if stats.get('checks'):
        st.dataframe(pd.DataFrame(stats['checks']), hide_index=True, use_container_width=True)
    else:
        st.info('Warehouse checks have not completed for this run.')
    _, _, preview = inspect_table(run['run_path'], 'quarantine')
    st.subheader('Quarantine · first 100 records')
    st.dataframe(preview, hide_index=True, use_container_width=True)
    st.download_button('Download all rejected records', rejected_csv(run['run_path']), 'quarantine.csv', 'text/csv')
