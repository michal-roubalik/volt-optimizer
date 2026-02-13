from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import pandas as pd
import json
import os
import asyncio

from database import engine, Base, get_db
from models import MarketData
from connectors import OpenMeteoConnector, EntsoeConnector
from optimizer import BatteryOptimizer

# Bootstrap schema on startup
Base.metadata.create_all(bind=engine)

app = FastAPI()

def fetch_and_store(start: datetime, end: datetime, db: Session):
    """
    Orchestrates ETL from weather and market APIs into the local store.
    
    Performs an upsert-style operation by clearing existing range data first
    to maintain time-series integrity.
    """
    meteo = OpenMeteoConnector(lat=51.16, lon=10.45)
    entsoe = EntsoeConnector(api_key=os.getenv("ENTSOE_API_KEY"), country_code="DE_LU")
    
    df_meteo = meteo.fetch_data(start, end)
    df_price = entsoe.fetch_data(start, end)
    
    # Align datasets on common timestamps and drop incomplete rows
    common_idx = df_meteo.index.intersection(df_price.index)
    df_final = pd.concat([df_meteo.loc[common_idx], df_price.loc[common_idx]], axis=1).dropna()

    db.query(MarketData).filter(MarketData.timestamp >= start, MarketData.timestamp <= end).delete()
    
    for idx, row in df_final.iterrows():
        db.add(MarketData(
            timestamp=idx,
            # Cast numpy types to native Python floats to prevent DB serialization errors
            price_eur_mwh=float(row['price_eur_mwh']),
            solar_rad_kw=float(row['solar_rad_kw']),
            temperature_c=float(row['temperature_c'])
        ))
    db.commit()
    return len(df_final)

@app.get("/simulate")
async def run_simulation(start_date: str, horizon_days: int = 7, db: Session = Depends(get_db)):
    """
    Main entry point for the simulation engine. 
    Returns an NDJSON stream providing real-time telemetry of the ingestion and solve phases.
    """
    async def event_generator():
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = start + timedelta(days=horizon_days)

            yield json.dumps({"step": "log", "message": f"Checking cache for {start_date}..."}) + "\n"
            await asyncio.sleep(0.1) 

            existing_count = db.query(MarketData).filter(
                MarketData.timestamp >= start, 
                MarketData.timestamp < end
            ).count()
            
            # Allow for minor data gaps (95% coverage) before triggering re-ingestion
            expected_hours = horizon_days * 24 * 0.95 

            if existing_count < expected_hours:
                yield json.dumps({"step": "log", "message": "Cache miss. Starting external data fetch..."}) + "\n"
                count = fetch_and_store(start, end, db)
                yield json.dumps({"step": "log", "message": f"Ingested {count} records."}) + "\n"
            else:
                yield json.dumps({"step": "log", "message": "Cache hit. Using local data."}) + "\n"

            # Load and sort time-series for the optimizer
            data = db.query(MarketData).filter(MarketData.timestamp >= start, MarketData.timestamp < end).all()
            df = pd.DataFrame([{
                "price_eur_mwh": d.price_eur_mwh,
                "solar_rad_kw": d.solar_rad_kw,
                "timestamp": d.timestamp.isoformat()
            } for d in data]).set_index('timestamp').sort_index()

            yield json.dumps({"step": "log", "message": "Invoking MILP solver..."}) + "\n"
            
            optimizer = BatteryOptimizer()
            result_df = optimizer.solve(df)
            
            yield json.dumps({"step": "log", "message": "Solution found."}) + "\n"
            
            result_json = result_df.reset_index(drop=True).to_dict(orient="records")
            yield json.dumps({"step": "result", "data": result_json}) + "\n"

        except Exception as e:
            yield json.dumps({"step": "error", "message": str(e)}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")