# app/features/fred/services/fred_client.py
import time, httpx
from app.core.config import FRED_API_KEY, FRED_BASE_URL, FRED_REQUEST_TIMEOUT, FRED_CALL_INTERVAL_MS

class FredClient:
    def _get(self, path: str, params: dict):
        if not FRED_API_KEY:
            raise RuntimeError("FRED_API_KEY missing")
        p = {"api_key": FRED_API_KEY, "file_type": "json"} | (params or {})
        url = f"{FRED_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
        with httpx.Client(timeout=FRED_REQUEST_TIMEOUT) as c:
            r = c.get(url, params=p)
            r.raise_for_status()
            time.sleep(FRED_CALL_INTERVAL_MS / 1000)
            return r.json()

    def series(self, series_id: str):
        return self._get("series", {"series_id": series_id})

    def observations(self, series_id: str, **kwargs):
        return self._get("series/observations", {"series_id": series_id} | kwargs)
