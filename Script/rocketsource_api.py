"""Minimal RocketSource product/history helper for filling UI averages."""
from __future__ import annotations

from typing import Optional
import logging
import requests

from .config import RocketSourceConfig

_LOG = logging.getLogger(__name__)


class RocketSourceAPI:
    def __init__(self, config: RocketSourceConfig, session: Optional[requests.Session] = None) -> None:
        self._cfg = config
        self._session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        prefix = self._cfg.api_key_prefix or ""
        key = self._cfg.api_key or ""
        val = f"{prefix}{key}" if prefix else key
        return {self._cfg.api_key_header: val, "Accept": "application/json"}

    def get_product(self, asin: str) -> Optional[dict]:
        """Fetch product details for a single ASIN using /products/{asin}.

        Returns parsed JSON or None on failure.
        """
        url = self._cfg.base_url.rstrip("/") + f"/products/{asin}"
        try:
            resp = self._session.get(url, headers=self._headers(), timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            _LOG.debug("Failed to fetch product %s: %s", asin, e)
            return None

    def get_history(self, asin: str, days: int = 90) -> Optional[list[dict]]:
        """Fetch product history (daily). Returns list of day dicts or None."""
        url = self._cfg.base_url.rstrip("/") + f"/products/{asin}/history"
        params = {"period": days, "granularity": "daily"}
        try:
            resp = self._session.get(url, headers=self._headers(), params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            _LOG.debug("Failed to fetch history %s: %s", asin, e)
            return None

    def get_averages(self, asin: str) -> dict:
        """Return a mapping with the five fields requested by the UI.

        Keys: sales_estimate, price_avg_30d, price_avg_90d, bsr_avg_30d, bsr_avg_90d
        Values may be None if not available.
        """
        out = {
            "sales_estimate": None,
            "price_avg_30d": None,
            "price_avg_90d": None,
            "bsr_avg_30d": None,
            "bsr_avg_90d": None,
        }

        prod = self.get_product(asin)
        if prod:
            out["sales_estimate"] = prod.get("estimated_monthly_sales") or prod.get("sales_estimate")

        hist = self.get_history(asin, days=90)
        if hist and isinstance(hist, list):
            prices = [d.get("price") for d in hist if d.get("price") is not None]
            bsr = [d.get("sales_rank") for d in hist if d.get("sales_rank") is not None]

            recent_30 = prices[-30:] if len(prices) >= 1 else []
            recent_90 = prices[-90:] if len(prices) >= 1 else []
            bsr_30 = bsr[-30:] if len(bsr) >= 1 else []
            bsr_90 = bsr[-90:] if len(bsr) >= 1 else []

            try:
                out["price_avg_30d"] = sum(recent_30) / len(recent_30) if recent_30 else None
            except Exception:
                out["price_avg_30d"] = None
            try:
                out["price_avg_90d"] = sum(recent_90) / len(recent_90) if recent_90 else None
            except Exception:
                out["price_avg_90d"] = None
            try:
                out["bsr_avg_30d"] = sum(bsr_30) / len(bsr_30) if bsr_30 else None
            except Exception:
                out["bsr_avg_30d"] = None
            try:
                out["bsr_avg_90d"] = sum(bsr_90) / len(bsr_90) if bsr_90 else None
            except Exception:
                out["bsr_avg_90d"] = None

        return out
