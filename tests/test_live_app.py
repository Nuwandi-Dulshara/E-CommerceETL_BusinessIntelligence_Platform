"""Read-only UI integration tests against a locally published real snapshot."""
import pytest
from backend.config import DATA
from backend.pipelines.metadata import published


@pytest.mark.skipif(published(DATA) is None, reason='Run the real-data pipeline first')
def test_all_pages_filters_and_saved_queries():
    from streamlit.testing.v1 import AppTest
    app = AppTest.from_file('frontend/app.py', default_timeout=40).run()
    assert not app.exception
    assert len(app.metric) == 6
    original_revenue = app.metric[0].value
    app.multiselect(key='country_filter').set_value(['France']).run()
    assert not app.exception
    assert app.metric[0].value != original_revenue
    next(b for b in app.button if b.label == 'Reset filters').click().run()
    assert app.metric[0].value == original_revenue
    for page in ['Sales Analytics','Customer Analytics','ETL Pipelines','Lakehouse Explorer','Data Quality','SQL Analytics']:
        app.radio(key='navigation').set_value(page).run()
        assert not app.exception, (page, [e.message for e in app.exception])
        if page == 'SQL Analytics':
            for name in ['Monthly revenue','Top products','Country sales','Repeat customers','Returns']:
                next(s for s in app.selectbox if s.label == 'Saved query').set_value(name).run()
                next(b for b in app.button if b.label == 'Run Query').click().run()
                assert not app.exception
                assert len(app.dataframe) == 1
                assert len(app.get('download_button')) == 1
        if page == 'Lakehouse Explorer':
            app.selectbox(key='table_Silver').set_value('quarantine').run()
            for table in ['fact_transactions','dim_product','dim_customer','dim_date','agg_daily_sales','agg_country_sales','agg_product_sales']:
                app.selectbox(key='table_Gold').set_value(table).run()
                assert not app.exception
    app.radio(key='navigation').set_value('Overview').run()
    app.date_input(key='date_filter').set_value((app.date_input(key='date_filter').value[0],)).run()
    assert not app.exception
    assert app.info
