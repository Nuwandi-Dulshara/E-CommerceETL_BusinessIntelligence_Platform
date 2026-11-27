from pathlib import Path
import streamlit as st
from backend.services.lakehouse_service import LAYERS, inspect_table


def render(snapshot):
    st.caption('Committed Delta tables from the published run. Previews query their Parquet mirrors; no additional Spark process is started.')
    for tab, (layer, names) in zip(st.tabs(list(LAYERS)), LAYERS.items()):
        with tab:
            name = st.selectbox('Table', names, key=f'table_{layer}')
            count, schema, preview = inspect_table(snapshot['run_path'], name)
            st.metric('Records', f'{count:,}')
            location = Path(snapshot['run_path']) / ('gold' if layer == 'Gold' else '') / name
            st.caption(str(location))
            with st.expander('Schema', expanded=False):
                st.dataframe(schema[['column_name','column_type','null']], hide_index=True, use_container_width=True)
            st.caption('Preview · up to 100 records')
            st.dataframe(preview, hide_index=True, use_container_width=True)
