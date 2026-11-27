import math
import streamlit as st
import plotly.express as px

COLORS = ['#2563EB', '#0D9488', '#94A3B8', '#B45309']


def money(value):
    return 'N/A' if value is None or math.isnan(float(value)) else f'£{float(value):,.2f}'


def chart(frame, kind, x, y, title, **kwargs):
    st.subheader(title)
    if frame.empty:
        st.info('No matching records for this selection.')
        return
    method = getattr(px, kind)
    fig = method(frame, x=x, y=y, color_discrete_sequence=COLORS, **kwargs)
    fig.update_layout(template='plotly_white', paper_bgcolor='white', plot_bgcolor='white',
                      margin=dict(l=16,r=16,t=20,b=20), height=330,
                      font=dict(family='Inter, sans-serif', color='#172B4D'),
                      legend=dict(orientation='h', y=1.12, title=None),
                      hovermode='x unified' if kind == 'line' else 'closest')
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor='#EDF1F7')
    st.plotly_chart(fig, use_container_width=True)


def download(frame, label, key):
    st.download_button(label, frame.to_csv(index=False).encode('utf-8'), f'{key}.csv', 'text/csv', key=key)


def kpis(items):
    for start in range(0, len(items), 3):
        for col, (label, value) in zip(st.columns(3), items[start:start+3]):
            col.metric(label, value)


def no_matches(metrics):
    if metrics is None:
        st.info('No transactions match these filters. Adjust the dates or countries.')
        return True
    return False
