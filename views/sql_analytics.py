import streamlit as st
from services.analytics import query, query_sql
from views.common import download


def render(args):
    name = st.selectbox('Saved query', ['Monthly revenue','Top products','Country sales','Repeat customers','Returns'])
    sql, params = query_sql(name, *args[1:])
    st.caption('Read-only SQL. Global filters are bound as parameters, never interpolated.')
    st.code(sql, language='sql')
    st.caption('Parameters: ' + ', '.join(str(p) for p in params))
    signature = (name, args[0], str(args[1]), str(args[2]), tuple(args[3]))
    if st.button('Run Query', type='primary'):
        st.session_state['sql_result'] = (signature, query(name, *args))
    result = st.session_state.get('sql_result')
    if result and result[0] == signature:
        frame = result[1]
        st.caption(f'{len(frame):,} result rows')
        st.dataframe(frame, hide_index=True, use_container_width=True)
        download(frame, 'Download query results', 'sql_results')
