from sqlalchemy import Column, Integer, Float, DateTime, String
from database import Base

class MarketData(Base):
    __tablename__ = "market_data"
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, unique=True, index=True)
    price_eur_mwh = Column(Float)  # Spot Price
    solar_rad_kw = Column(Float)   # Solar Radiation (kW/m2)
    temperature_c = Column(Float)