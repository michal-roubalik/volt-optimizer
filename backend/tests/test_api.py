from fastapi.testclient import TestClient
from main import app

def test_optimize_endpoint_no_data(client, mocker):
    """
    Ensures the simulation handles a cache miss gracefully.
    """
    # 1. Mock fetch_and_store to avoid needing a real API key during tests
    mocker.patch("main.fetch_and_store", return_value=24)

    # EXECUTE
    response = client.get("/simulate", params={
        "start_date": "2099-01-01", 
        "horizon_days": 1
    })
    
    # ASSERTIONS
    assert response.status_code == 200
    content = response.text
    
    # Match the professional log strings defined in main.py
    assert "Checking cache" in content
    assert "Cache miss" in content