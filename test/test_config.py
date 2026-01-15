from Script.config import RocketSourceConfig
from Script.errors import ConfigError

import pytest


def test_config_reads_api_key_from_env(monkeypatch):
    monkeypatch.setenv("ROCKETSOURCE_BASE_URL", "https://example.test")
    monkeypatch.setenv("ROCKETSOURCE_API_KEY", "k")
    cfg = RocketSourceConfig.from_env()
    assert cfg.base_url == "https://example.test"
    assert cfg.api_key == "k"


def test_config_accepts_api_key_fallback(monkeypatch):
    monkeypatch.setenv("ROCKETSOURCE_BASE_URL", "https://example.test")
    monkeypatch.delenv("ROCKETSOURCE_API_KEY", raising=False)
    monkeypatch.setenv("API_KEY", "k2")
    cfg = RocketSourceConfig.from_env()
    assert cfg.api_key == "k2"


def test_config_missing_base_url(monkeypatch):
    monkeypatch.delenv("ROCKETSOURCE_BASE_URL", raising=False)
    monkeypatch.setenv("ROCKETSOURCE_API_KEY", "k")
    with pytest.raises(ConfigError):
        RocketSourceConfig.from_env()
