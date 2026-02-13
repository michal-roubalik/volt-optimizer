import pulp
import pandas as pd

class BatteryOptimizer:
    """
    Handles MILP optimization for a PV-coupled BESS.
    Logic maximizes revenue by performing price arbitrage and maximizing PV self-consumption.
    """
    def __init__(self, battery_cap_kwh=10, max_power_kw=5, efficiency=0.9, pv_peak_kw=5.0):
        self.BATTERY_CAPACITY = battery_cap_kwh
        self.MAX_POWER = max_power_kw
        self.EFFICIENCY = efficiency
        self.PV_PEAK = pv_peak_kw

    def solve(self, data: pd.DataFrame):
        """
        Solves the hourly dispatch problem for the given price and solar profile.
        Returns a DataFrame with power flows and SoC.
        """
        time_steps = range(len(data))
        prob = pulp.LpProblem("BESS_Arbitrage", pulp.LpMaximize)

        # Power flow variables
        charge = pulp.LpVariable.dicts("Charge", time_steps, 0, self.MAX_POWER)
        discharge = pulp.LpVariable.dicts("Discharge", time_steps, 0, self.MAX_POWER)
        soc = pulp.LpVariable.dicts("SoC", time_steps, 0, self.BATTERY_CAPACITY)
        export_grid = pulp.LpVariable.dicts("Export", time_steps, 0)
        import_grid = pulp.LpVariable.dicts("Import", time_steps, 0)
        
        # Binary to prevent simultaneous charge/discharge (Complementarity)
        is_charging = pulp.LpVariable.dicts("IsCharging", time_steps, cat='Binary')

        # PV generation based on peak capacity
        pv_gen_values = [data.iloc[t]['solar_rad_kw'] * self.PV_PEAK for t in time_steps]

        for t in time_steps:
            # Nodal balance: Import + PV + Discharge = Export + Charge
            prob += (import_grid[t] + pv_gen_values[t] + discharge[t] == export_grid[t] + charge[t])

            # State of Charge dynamics
            # Assumes 0 initial SoC; efficiency applied symmetrically
            if t == 0:
                prob += soc[t] == (charge[t] * self.EFFICIENCY) - (discharge[t] / self.EFFICIENCY)
            else:
                prob += soc[t] == soc[t-1] + (charge[t] * self.EFFICIENCY) - (discharge[t] / self.EFFICIENCY)

            # Link binary variables to power limits
            prob += charge[t] <= self.MAX_POWER * is_charging[t]
            prob += discharge[t] <= self.MAX_POWER * (1 - is_charging[t])

        # Objective: Revenue from Export minus cost of Import
        prob += pulp.lpSum([
            (export_grid[t] * data.iloc[t]['price_eur_mwh'] / 1000) - 
            (import_grid[t] * data.iloc[t]['price_eur_mwh'] / 1000) 
            for t in time_steps
        ])

        # Suppress CBC solver output for production
        prob.solve(pulp.PULP_CBC_CMD(msg=False))

        # Result extraction
        results = []
        for t in time_steps:
            results.append({
                "timestamp": data.index[t],
                "price_eur_mwh": data.iloc[t]['price_eur_mwh'],
                "power_production_pv_kw": pv_gen_values[t],
                "power_battery_charge_kw": charge[t].varValue,
                "power_battery_discharge_kw": discharge[t].varValue,
                "stored_energy_kwh": soc[t].varValue,
                "profit_eur": (export_grid[t].varValue - import_grid[t].varValue) * data.iloc[t]['price_eur_mwh'] / 1000
            })
            
        return pd.DataFrame(results)