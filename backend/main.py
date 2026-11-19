from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import requests
import os
import json
import re
from datetime import datetime
import mysql.connector

app = FastAPI(title="AgroRisk Rescue System API - Multi-Year & Real-Time")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = "../powerbi_data"
CLEANED_FILE = f"{DATA_DIR}/cleaned_final.csv"
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOIL_BENCHMARK_CANDIDATES = [
    os.path.join(PROJECT_DIR, "District_Benchmarks_38.csv"),
    os.path.join(PROJECT_DIR, "powerbi_data", "District_Benchmarks_38.csv")
]
SOIL_BENCHMARK_FILE = next(
    (path for path in SOIL_BENCHMARK_CANDIDATES if os.path.exists(path)),
    None
)
SOIL_DEFAULTS = {
    "soil_type": "Unknown",
    "soil_ph": 7.2,
    "nitrogen_n": 150.0,
    "phosphorus_p": 25.0,
    "potassium_k": 180.0
}
FERTILIZER_TARGETS = {
    "nitrogen_n": 200.0,
    "phosphorus_p": 50.0,
    "potassium_k": 200.0
}

# Coordinates for Tamil Nadu districts
DISTRICT_COORDS = {
    "Erode": (11.3410, 77.7172), "Kallakurichi": (11.7384, 78.9639),
    "Namakkal": (11.2189, 78.1674), "Coimbatore": (11.0168, 76.9558),
    "Salem": (11.6643, 78.1460), "Tiruvannamalai": (12.2253, 79.0747),
    "Tiruppur": (11.1085, 77.3411), "Cuddalore": (11.7480, 79.7714),
    "Dharmapuri": (12.1211, 78.1582), "Dindigul": (10.3624, 77.9695),
    "Thanjavur": (10.7870, 79.1378), "Madurai": (9.9252, 78.1198),
    "Tirunelveli": (8.7139, 77.7567), "Thoothukudi": (8.7642, 78.1348),
    "Kanniyakumari": (8.0883, 77.5385), "Tiruchirappalli": (10.7905, 78.7047),
    "Vellore": (12.9165, 79.1325), "Villupuram": (11.9401, 79.4861),
    "Theni": (10.0104, 77.4768), "Krishnagiri": (12.5186, 78.2137),
    "Ariyalur": (11.1401, 79.0786), "Chengalpattu": (12.6819, 79.9888),
    "Chennai": (13.0827, 80.2707), "Kancheepuram": (12.8342, 79.7036),
    "Karur": (10.9601, 78.0766), "Mayiladuthurai": (11.1018, 79.6526),
    "Nagapattinam": (10.7672, 79.8449), "Perambalur": (11.2342, 78.8807),
    "Pudukkottai": (10.3797, 78.8208), "Ramanathapuram": (9.3639, 78.8395),
    "Ranipet": (12.9224, 79.3330), "Sivagangai": (9.8433, 78.4809),
    "Tenkasi": (8.9594, 77.3161), "The Nilgiris": (11.4916, 76.7337),
    "Thiruvarur": (10.7661, 79.6344), "Tirupathur": (12.4950, 78.5678),
    "Tiruvallur": (13.1231, 79.9120), "Virudhunagar": (9.5680, 77.9624)
}

# Load cleaned dataset once at startup for fast year-based filtering
df_main = pd.read_csv(CLEANED_FILE)
YEAR_COL = "year" if "year" in df_main.columns else ("Year" if "Year" in df_main.columns else None)
df_soil = pd.read_csv(SOIL_BENCHMARK_FILE) if SOIL_BENCHMARK_FILE else pd.DataFrame()


def _normalized_column_name(column_name):
    return re.sub(r"[^a-z0-9]", "", str(column_name).casefold())


def _find_soil_column(*aliases):
    alias_names = {_normalized_column_name(alias) for alias in aliases}
    for column in df_soil.columns:
        if _normalized_column_name(column) in alias_names:
            return column
    return None


SOIL_DISTRICT_COLUMN = _find_soil_column("District_Name", "DistrictName", "District")
SOIL_TYPE_COLUMN = _find_soil_column("Dominant_Soil_Type", "Soil_Type", "SoilType")
SOIL_PH_COLUMN = _find_soil_column("Soil_pH", "Soil_pH_Value", "pH")
SOIL_N_COLUMN = _find_soil_column(
    "Nitrogen_N_kg_ha", "Available_Nitrogen_kg_ha", "Nitrogen_N", "N_kg_ha"
)
SOIL_P_COLUMN = _find_soil_column(
    "Phosphorus_P_kg_ha", "Available_Phosphorus_kg_ha", "Phosphorus_P", "P_kg_ha"
)
SOIL_K_COLUMN = _find_soil_column(
    "Potassium_K_kg_ha", "Available_Potassium_kg_ha", "Potassium_K", "K_kg_ha"
)

class FarmerInput(BaseModel):
    farmer_name: str
    district_name: str
    crop_name: str
    season: str
    year: int
    cultivated_area_ha: float
    harvested_area_ha: float
    actual_yield_kg_ha: float
    benchmark_yield_kg_ha: float
    rainfall_mm: float
    pest_severity: str
    soil_type: str = "Unknown"
    soil_ph: float = SOIL_DEFAULTS["soil_ph"]
    nitrogen_n: float = SOIL_DEFAULTS["nitrogen_n"]
    phosphorus_p: float = SOIL_DEFAULTS["phosphorus_p"]
    potassium_k: float = SOIL_DEFAULTS["potassium_k"]


def save_to_mysql(data, risk_label, rescue_tip, fertilizer_recommendation):
    conn = mysql.connector.connect(
        host="172.22.160.1",
        user="root",
        password="root@123",
        database="agrorisk_db"
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO farmer_submissions 
        (farmer_name, district_name, crop_name, season, cultivated_area_ha, 
         harvested_area_ha, actual_yield_kg_ha, rainfall_mm, pest_severity, 
         predicted_loss_risk, rescue_recommendation, soil_ph, nitrogen_n,
         phosphorus_p, potassium_k, fertilizer_recommendation)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        data.farmer_name, data.district_name, data.crop_name, data.season,
        data.cultivated_area_ha, data.harvested_area_ha, data.actual_yield_kg_ha,
        data.rainfall_mm, data.pest_severity, risk_label, rescue_tip,
        data.soil_ph, data.nitrogen_n, data.phosphorus_p, data.potassium_k,
        json.dumps(fertilizer_recommendation, ensure_ascii=False)
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()


def _get_soil_value(row, column, default):
    if row is None or column is None:
        return default
    value = pd.to_numeric(row[column], errors="coerce")
    return float(value) if pd.notna(value) else default


@app.get("/api/district-soil/{district_name}")
def get_district_soil(district_name: str):
    soil_row = None
    if SOIL_DISTRICT_COLUMN:
        matching_rows = df_soil[
            df_soil[SOIL_DISTRICT_COLUMN].astype(str).str.strip().str.casefold()
            == district_name.strip().casefold()
        ]
        if not matching_rows.empty:
            soil_row = matching_rows.iloc[0]

    soil_type = SOIL_DEFAULTS["soil_type"]
    if soil_row is not None and SOIL_TYPE_COLUMN:
        value = soil_row[SOIL_TYPE_COLUMN]
        if pd.notna(value) and str(value).strip():
            soil_type = str(value).strip()

    return {
        "district_name": district_name,
        "soil_type": soil_type,
        "soil_ph": _get_soil_value(soil_row, SOIL_PH_COLUMN, SOIL_DEFAULTS["soil_ph"]),
        "nitrogen_n": _get_soil_value(soil_row, SOIL_N_COLUMN, SOIL_DEFAULTS["nitrogen_n"]),
        "phosphorus_p": _get_soil_value(soil_row, SOIL_P_COLUMN, SOIL_DEFAULTS["phosphorus_p"]),
        "potassium_k": _get_soil_value(soil_row, SOIL_K_COLUMN, SOIL_DEFAULTS["potassium_k"])
    }


@app.get("/api/historical-analytics")
def get_historical_analytics(year: str = Query("All"), district: str = Query("All")):
    try:
        df = df_main.copy()

        # Available filter lists
        available_years = sorted(df[YEAR_COL].dropna().unique().astype(str).tolist()) if YEAR_COL else []
        available_districts = sorted(df["district_name"].dropna().unique().tolist())

        # Yearly trend across all years (filtered by district if selected)
        trend_df = df if district == "All" else df[df["district_name"] == district]
        yearly_trends = []
        if YEAR_COL:
            grp = trend_df.groupby(YEAR_COL).agg(
                total_production_tonnes=("production_tonnes", "sum"),
                avg_yield_kg_ha=("actual_yield_kg_ha", "mean"),
                benchmark_yield_kg_ha=("Historical_Benchmark_Yield_kg_ha", "mean"),
                avg_yield_loss_pct=("yield_loss_percentage", "mean"),
                total_harvested_area_ha=("harvested_area_ha", "sum")
            ).reset_index().sort_values(YEAR_COL)
            for _, r in grp.iterrows():
                yearly_trends.append({
                    "year": str(int(r[YEAR_COL])),
                    "production_tonnes": round(float(r["total_production_tonnes"]), 2),
                    "actual_yield": round(float(r["avg_yield_kg_ha"]), 2),
                    "benchmark_yield": round(float(r["benchmark_yield_kg_ha"]), 2),
                    "yield_loss_pct": round(float(r["avg_yield_loss_pct"]), 2),
                    "harvested_area_ha": round(float(r["total_harvested_area_ha"]), 2)
                })

        # Filter main snapshot by selected year & district
        if year != "All" and YEAR_COL:
            df = df[df[YEAR_COL].astype(str) == str(year)]
        if district != "All":
            df = df[df["district_name"] == district]

        # Crop-wise production, growth vs benchmark, and loss
        crop_grp = df.groupby("crop_name").agg(
            production_tonnes=("production_tonnes", "sum"),
            actual_yield=("actual_yield_kg_ha", "mean"),
            benchmark_yield=("Historical_Benchmark_Yield_kg_ha", "mean"),
            yield_loss_pct=("yield_loss_percentage", "mean")
        ).reset_index().sort_values("production_tonnes", ascending=False).head(10)

        crop_stats = []
        for _, r in crop_grp.iterrows():
            crop_stats.append({
                "crop_name": r["crop_name"],
                "production_tonnes": round(float(r["production_tonnes"]), 2),
                "actual_yield": round(float(r["actual_yield"]), 2),
                "benchmark_yield": round(float(r["benchmark_yield"]), 2),
                "yield_loss_pct": round(float(r["yield_loss_pct"]), 2)
            })

        # Loss Risk Level counts
        risk_counts = df["loss_risk_label"].value_counts().reset_index()
        risk_counts.columns = ["risk_level", "count"]

        return {
            "available_years": available_years,
            "available_districts": available_districts,
            "kpis": {
                "total_production_tonnes": round(float(df["production_tonnes"].sum()), 2),
                "total_harvested_area_ha": round(float(df["harvested_area_ha"].sum()), 2),
                "avg_yield_kg_ha": round(float(df["actual_yield_kg_ha"].mean()), 2) if len(df) else 0,
                "avg_yield_loss_pct": round(float(df["yield_loss_percentage"].mean()), 2) if len(df) else 0
            },
            "yearly_trends": yearly_trends,
            "crop_stats": crop_stats,
            "risk_distribution": risk_counts.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/realtime-weather/{district_name}")
def get_realtime_weather(district_name: str):
    lat, lon = DISTRICT_COORDS.get(district_name, (11.1271, 78.6569))
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,soil_moisture_0_to_1cm"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration"
        f"&timezone=Asia%2FKolkata"
    )
    res = requests.get(url, timeout=10).json()
    current = res.get("current", {})
    daily = res.get("daily", {})

    forecast_days = []
    dates = daily.get("time", [])
    for i in range(len(dates)):
        forecast_days.append({
            "date": dates[i],
            "max_temp": daily["temperature_2m_max"][i],
            "min_temp": daily["temperature_2m_min"][i],
            "rainfall_mm": daily["precipitation_sum"][i],
            "evapotranspiration_mm": daily["et0_fao_evapotranspiration"][i]
        })

    # Live agricultural advisory from real-time weather
    total_7day_rain = sum(daily.get("precipitation_sum", [0]))
    advisory = "Optimal moisture conditions for crop growth."
    if total_7day_rain < 5:
        advisory = "Dry spell ahead (<5mm rain in 7 days): Schedule drip irrigation and mulching."
    elif total_7day_rain > 80:
        advisory = "Heavy rainfall alert (>80mm in 7 days): Clear field drainage channels to prevent root rot."

    return {
        "district": district_name,
        "current": {
            "temperature_c": current.get("temperature_2m"),
            "humidity_pct": current.get("relative_humidity_2m"),
            "precipitation_mm": current.get("precipitation"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "soil_moisture": current.get("soil_moisture_0_to_1cm")
        },
        "forecast_7day": forecast_days,
        "advisory": advisory
    }

@app.post("/api/submit-crop")
def submit_farmer_data(data: FarmerInput):
    # Calculate expected production, yield gap, and loss percentage
    expected_production = round((data.harvested_area_ha * data.actual_yield_kg_ha) / 1000.0, 2)
    benchmark_production = round((data.cultivated_area_ha * data.benchmark_yield_kg_ha) / 1000.0, 2)
    area_loss_pct = max(0.0, round(((data.cultivated_area_ha - data.harvested_area_ha) / max(data.cultivated_area_ha, 0.01)) * 100, 2))
    yield_gap_pct = max(0.0, round(((data.benchmark_yield_kg_ha - data.actual_yield_kg_ha) / max(data.benchmark_yield_kg_ha, 1)) * 100, 2))
    estimated_loss_pct = round(min(100.0, (area_loss_pct * 0.4) + (yield_gap_pct * 0.6)), 2)

    risk_label = "Low"
    rescue_tip = "Crop growth is on track with benchmark. Continue balanced NPK schedule."
    water_saving_est = round(data.cultivated_area_ha * 12000, 2)

    if estimated_loss_pct >= 35 or data.rainfall_mm < 350 or data.pest_severity == "High":
        risk_label = "Critical" if estimated_loss_pct >= 50 else "High"
        rescue_tip = "High loss risk detected: Activate drip irrigation, apply organic bio-pesticides, and harvest mature pods early."
        water_saving_est = round(data.cultivated_area_ha * 38000, 2)
    elif estimated_loss_pct >= 15 or data.rainfall_mm < 600 or data.pest_severity == "Medium":
        risk_label = "Medium"
        rescue_tip = "Moderate stress detected: Apply foliar micronutrient spray and organic mulch to conserve soil moisture."
        water_saving_est = round(data.cultivated_area_ha * 24000, 2)

    fertilizer_recommendation = calculate_fertilizer_recommendation(data)

    record = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        **data.dict(),
        "expected_production_tonnes": expected_production,
        "benchmark_production_tonnes": benchmark_production,
        "estimated_loss_pct": estimated_loss_pct,
        "predicted_loss_risk": risk_label,
        "estimated_water_saving_liters": water_saving_est,
        "rescue_recommendation": rescue_tip,
        "fertilizer_recommendation": fertilizer_recommendation
    }

    log_file = f"{DATA_DIR}/farmer_live_submissions.csv"
    csv_record = {
        **record,
        "fertilizer_recommendation": json.dumps(fertilizer_recommendation, ensure_ascii=False)
    }
    pd.DataFrame([csv_record]).to_csv(log_file, mode="a", header=not os.path.exists(log_file), index=False)
    save_to_mysql(data, risk_label, rescue_tip, fertilizer_recommendation)
    return {
        "status": "success",
        "analysis": record,
        "fertilizer_recommendation": fertilizer_recommendation
    }


def calculate_fertilizer_recommendation(data):
    urea_bags_per_ha = max(0.0, FERTILIZER_TARGETS["nitrogen_n"] - data.nitrogen_n) / 0.46 / 50
    dap_bags_per_ha = max(0.0, FERTILIZER_TARGETS["phosphorus_p"] - data.phosphorus_p) / 0.46 / 50
    potash_bags_per_ha = max(0.0, FERTILIZER_TARGETS["potassium_k"] - data.potassium_k) / 0.60 / 50
    return {
        "soil_type": data.soil_type,
        "soil_ph": data.soil_ph,
        "soil_ph_status": (
            "acidic" if data.soil_ph < 6.2
            else "alkaline" if data.soil_ph > 8.0
            else "suitable"
        ),
        "ph_amendment_code": (
            "agricultural_lime" if data.soil_ph < 6.2
            else "gypsum" if data.soil_ph > 8.0
            else "none"
        ),
        "ph_amendment_tip": (
            "Apply agricultural lime and retest soil pH."
            if data.soil_ph < 6.2
            else "Apply gypsum as advised by a soil test."
            if data.soil_ph > 8.0
            else "No pH amendment is indicated; maintain organic matter."
        ),
        "bag_weight_kg": 50,
        "target_npk_kg_ha": FERTILIZER_TARGETS,
        "per_hectare_bags": {
            "urea": round(urea_bags_per_ha, 2),
            "dap": round(dap_bags_per_ha, 2),
            "potash_mop": round(potash_bags_per_ha, 2)
        },
        "total_area_bags": {
            "urea": round(urea_bags_per_ha * data.cultivated_area_ha, 2),
            "dap": round(dap_bags_per_ha * data.cultivated_area_ha, 2),
            "potash_mop": round(potash_bags_per_ha * data.cultivated_area_ha, 2)
        }
    }

@app.get("/api/farmer-submissions")
def get_farmer_submissions():
    log_file = f"{DATA_DIR}/farmer_live_submissions.csv"
    if not os.path.exists(log_file):
        return []
    df_sub = pd.read_csv(log_file)
    if "fertilizer_recommendation" in df_sub.columns:
        def parse_fertilizer_recommendation(value):
            if pd.isna(value) or not str(value).strip():
                return None
            try:
                return json.loads(value)
            except (TypeError, json.JSONDecodeError):
                return None

        df_sub["fertilizer_recommendation"] = df_sub["fertilizer_recommendation"].apply(
            parse_fertilizer_recommendation
        )
    return df_sub.to_dict(orient="records")
