import json
from pathlib import Path

import requests

from Script.client import RocketSourceClient
from Script.config import RocketSourceConfig


class FakeSession:
    def __init__(self, post_responses=None, get_responses=None):
        self._post = list(post_responses or [])
        self._get = list(get_responses or [])

    def post(self, *args, **kwargs):
        return self._post.pop(0)

    def get(self, *args, **kwargs):
        return self._get.pop(0)

    def close(self):
        return None


def _resp(status: int, body: object, content_type: str = "application/json") -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r.headers["Content-Type"] = content_type
    if isinstance(body, (dict, list)):
        r._content = json.dumps(body).encode("utf-8")
    elif isinstance(body, str):
        r._content = body.encode("utf-8")
    else:
        r._content = body
    r.encoding = "utf-8"
    return r


def test_run_csv_scan_happy_path(tmp_path: Path):
    cfg = RocketSourceConfig(base_url="https://example.test", api_key="k")

    csv_path = tmp_path / "in.csv"
    csv_path.write_text("ASIN,PRICE\nB000,0.1\n", encoding="utf-8")

    out_path = tmp_path / "out.csv"

    fake = FakeSession(
        post_responses=[
            _resp(200, {"id": "s1"}),
            _resp(200, "a,b\n1,2\n", content_type="text/csv"),
        ],
        get_responses=[
            _resp(200, [{"id": "s0", "name": "Old Scan"}]),
            _resp(200, {"status": "completed"}),
        ],
    )

    client = RocketSourceClient(cfg, session=fake)
    upload_id, scan_id = client.run_csv_scan(csv_path, out_path)

    assert upload_id == "s1"
    assert scan_id == "s1"
    assert out_path.exists()
    assert out_path.read_text(encoding="utf-8").startswith("a,b")
