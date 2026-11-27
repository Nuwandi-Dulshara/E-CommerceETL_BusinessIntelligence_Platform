from pathlib import Path
import pandas as pd
import streamlit as st
from config import DATA, DEFAULT_SOURCE
from pipelines.metadata import runs, stages
from services.pipeline_service import active, launch, log_text


@st.fragment(run_every='3s')
def render():
    st.caption('Run the local Spark / Delta pipeline. Published analytics remain available while a new snapshot is built.')
    busy = active(DATA)
    with st.container(border=True):
        left, right = st.columns(2)
        with left:
            source_type = st.selectbox('Source', ['Existing local Excel', 'Download from UCI'])
            source = st.text_input('Excel file path', str(DEFAULT_SOURCE), disabled=source_type == 'Download from UCI')
        with right:
            mode = st.selectbox('Dataset size', ['Sample · 50,000 rows', 'Full dataset'])
            st.caption('Samples use the first 50,000 source lines, not a random selection.')
        if st.button('Run Pipeline', type='primary', disabled=busy):
            try:
                run_id = launch(source, 50000 if mode.startswith('Sample') else None, source_type == 'Download from UCI', DATA)
                st.session_state['launched_run'] = run_id
                st.success(f'Pipeline started · {run_id}')
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    if busy:
        st.info('A pipeline is active. Status refreshes every 3 seconds; refreshing does not restart the job.')
    if st.button('Refresh status'):
        st.rerun()
    status_panel()


def status_panel():
    history = runs(DATA)
    if not history:
        st.info('No pipeline executions yet.')
        return
    st.subheader('Execution history')
    frame = pd.DataFrame(history)
    st.dataframe(frame[['run_id','status','stage','started_at','duration','sample_rows','input_count','accepted_count','rejected_count']], hide_index=True, use_container_width=True)
    run_id = st.selectbox('Inspect run', [r['run_id'] for r in history], key='inspect_run')
    selected = next(r for r in history if r['run_id'] == run_id)
    st.dataframe(pd.DataFrame(stages(DATA, run_id)), hide_index=True, use_container_width=True)
    if selected['error']:
        st.error(selected['error'])
    with st.expander('Run details'):
        st.json(selected)
    text = log_text(run_id, DATA)
    st.subheader('Execution log')
    st.code(text[-20000:] if text else 'Waiting for pipeline log output.', language='log')
    st.caption('Preview shows the last 20,000 characters. Download includes the full log.')
    st.download_button('Download full log', text, f'{run_id}.log', 'text/plain', key='pipeline_log')
