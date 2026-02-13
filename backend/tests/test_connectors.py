import pandas as pd
from unittest.mock import MagicMock
from connectors import OpenMeteoConnector
from datetime import datetime

def test_open_meteo_parsing(mocker):
    # SETUP: Mock requests.get so we don't hit the real internet
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "hourly": {
            "time": ["2023-01-01T00:00", "2023-01-01T01:00"],
            "temperature_2m": [10.5, 11.0],
            "shortwave_radiation": [0.0, 500.0] # Watts
        }
    }
    mock_response.raise_for_status = MagicMock()
    
    # Patch 'requests.get' in the connectors module
    mocker.patch("requests.get", return_value=mock_response)
    
    # EXECUTE
    connector = OpenMeteoConnector(lat=50.0, lon=14.0)
    df = connector.fetch_data(datetime(2023, 1, 1), datetime(2023, 1, 2))
    
    # ASSERTIONS
    assert len(df) == 2
    assert "solar_rad_kw" in df.columns
    # Check unit conversion (Watts -> kW)
    assert df.iloc[1]['solar_rad_kw'] == 0.5  # 500W / 1000 = 0.5kW