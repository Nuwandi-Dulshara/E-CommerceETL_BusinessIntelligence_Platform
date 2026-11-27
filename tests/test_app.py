def test_empty_app(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import backend.config as config
    monkeypatch.setattr(config, 'DATA', tmp_path)
    app = AppTest.from_file('frontend/app.py', default_timeout=30).run()
    assert not app.exception
    assert any('No published' in info.value for info in app.info)
