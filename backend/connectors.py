from abc import ABC, abstractmethod
import pandas as pd
import requests
from entsoe import EntsoePandasClient
from datetime import datetime
import logging

class DataConnector(ABC):
    @abstractmethod
    def fetch_data(self, start: datetime, end: datetime) -> pd.DataFrame:
        pass

class OpenMeteoConnector(DataConnector):
    """
    Adapter for Open-Meteo Historical Archive API.
    Converts raw solar irradiance into normalized kW values.
    """
    def __init__(self, lat: float, lon: float):
        self.lat = lat
        self.lon = lon
        self.url = "https://archive-api.open-meteo.com/v1/archive"

    def fetch_data(self, start: datetime, end: datetime) -> pd.DataFrame:
        params = {
            "latitude": self.lat,
            "longitude": self.lon,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "hourly": "temperature_2m,shortwave_radiation",
            "timezone": "UTC"
        }
        response = requests.get(self.url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return pd.DataFrame({
            'timestamp': pd.to_datetime(data['hourly']['time']),
            'temperature_c': data['hourly']['temperature_2m'],
            # Raw API returns W/m2. Convert to kW/m2 for optimizer consistency.
            'solar_rad_kw': [x / 1000.0 for x in data['hourly']['shortwave_radiation']]
        }).set_index('timestamp')

class EntsoeConnector(DataConnector):
    def __init__(self, api_key: str, country_code: str = 'DE_LU'):
        self.client = EntsoePandasClient(api_key=api_key)
        self.country_code = country_code

    def fetch_data(self, start: datetime, end: datetime) -> pd.DataFrame:
        try:
            # Fetch Day-ahead prices
            ts = self.client.query_day_ahead_prices(self.country_code, start=pd.Timestamp(start, tz='UTC'), end=pd.Timestamp(end, tz='UTC'))
            df = pd.DataFrame(ts, columns=['price_eur_mwh'])
            df.index = df.index.tz_convert(None) # Remove TZ for DB simplicity
            return df
        except Exception as e:
            logging.error(f"ENTSOE Fetch failed: {e}")
            # Fallback for testing/free users without valid key
            logging.warning("Using Mock Data for Prices due to error.")
            dates = pd.date_range(start, end, freq='H')
            return pd.DataFrame({'price_eur_mwh': 100}, index=dates)