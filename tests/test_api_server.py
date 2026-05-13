from fastapi.testclient import TestClient

from api_server import app


def test_health():
    client = TestClient(app)
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert "timestamp" in body


def test_info_lists_features():
    client = TestClient(app)
    r = client.get("/api/info")
    assert r.status_code == 200
    assert "features" in r.json()
