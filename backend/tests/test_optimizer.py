import pandas as pd
from optimizer import BatteryOptimizer

import pandas as pd
from optimizer import BatteryOptimizer

def test_battery_arbitrage_logic():
    # SETUP: Create a fake scenario
    # Hour 0: Price is 0 (Free), Solar is 0.
    # Hour 1: Price is 1000 EUR/MWh (Expensive), Solar is 0.
    data = pd.DataFrame([
        {"price_eur_mwh": 0.0,    "solar_rad_kw": 0.0},
        {"price_eur_mwh": 1000.0, "solar_rad_kw": 0.0}
    ])
    
    # Initialize optimizer with 10kWh battery
    opt = BatteryOptimizer(battery_cap_kwh=10, max_power_kw=5, efficiency=1.0)
    
    # EXECUTE
    results = opt.solve(data)
    
    # ASSERTIONS (Updated to match new API keys)
    
    # 1. At Hour 0 (Cheap), we should Charge
    # OLD: results.iloc[0]['charge']
    assert results.iloc[0]['power_battery_charge_kw'] > 0, "Battery should charge when price is 0"
    
    # 2. At Hour 1 (Expensive), we should Discharge
    # OLD: results.iloc[1]['discharge']
    assert results.iloc[1]['power_battery_discharge_kw'] > 0, "Battery should discharge when price is high"
    
    # 3. Total Profit should be positive
    # OLD: results['profit']
    assert results['profit_eur'].sum() > 0