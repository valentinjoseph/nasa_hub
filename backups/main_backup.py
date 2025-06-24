import requests
import pandas as pd
import uuid
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, text

# 🔐 Postgres credentials
pg_url = "postgresql+psycopg2://postgres:1234admin@localhost:5432/nasa_hub_dev"
engine = create_engine(pg_url)

# 🧠 Helper functions
def fetch_neo_feed(start_date, end_date, api_key):
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {"start_date": start_date, "end_date": end_date, "api_key": api_key}
    data = requests.get(url, params=params).json()
    records = []
    for day, objs in data["near_earth_objects"].items():
        for obj in objs:
            records.append(
                {
                    "id": obj["id"],
                    "date": day,
                    "name": obj["name"],
                    "est_diameter_min": obj["estimated_diameter"]["meters"]["estimated_diameter_min"],
                    "est_diameter_max": obj["estimated_diameter"]["meters"]["estimated_diameter_max"],
                    "is_potentially_hazardous": obj["is_potentially_hazardous_asteroid"],
                    "close_approach_date": obj["close_approach_data"][0]["close_approach_date"],
                    "velocity_kph": float(obj["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]),
                    "miss_distance_km": float(obj["close_approach_data"][0]["miss_distance"]["kilometers"]),
                    "orbiting_body": obj["close_approach_data"][0]["orbiting_body"],
                }
            )
    return pd.DataFrame(records)

def fetch_neo_browse_page(page, api_key, size=100):
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse"
    params = {"page": page, "size": size, "api_key": api_key}
    data = requests.get(url, params=params).json()
    records = []
    for neo in data["near_earth_objects"]:
        records.append(
            {
                "id": neo["id"],
                "name": neo["name"],
                "absolute_magnitude_h": neo["absolute_magnitude_h"],
                "is_potentially_hazardous": neo["is_potentially_hazardous_asteroid"],
                "est_diameter_min": neo["estimated_diameter"]["meters"]["estimated_diameter_min"],
                "est_diameter_max": neo["estimated_diameter"]["meters"]["estimated_diameter_max"],
                "n_close_approaches": len(neo["close_approach_data"]),
            }
        )
    return pd.DataFrame(records), data["page"]["number"], data["page"]["total_pages"]

# 🏃 Run params
run_id = str(uuid.uuid4())
run_name = "NASA_DAILY_LOAD"
run_start_time = datetime.now()

run_id_browse = str(uuid.uuid4())
run_name_browse = "NASA_LEGACY_LOAD"
run_start_browse = datetime.now()

# 🧠 Fetch execution mode
exec_mode_df = pd.read_sql(
    "select param_value from tech.nasa_param where param_name='EXECUTION_MODE'", engine
)
exec_mode = exec_mode_df.iloc[0, 0]

# 🔄 Get last successful run_end_ts
if exec_mode.upper() == "FULL":
    current = date(2015, 9, 7)
else:
    latest_df = pd.read_sql(
        "select max(run_end_ts) as last_run_end from tech.nasa_run_management "
        "where run_name='NASA_DAILY_LOAD' and run_status='SUCCESS'",
        engine
    )
    last_run_end = latest_df.iloc[0, 0]
    if pd.isnull(last_run_end):
        raise ValueError("No successful previous run found for DELTA mode")
    current = last_run_end.date() + timedelta(days=1)

# 🔄 Log new daily run
pd.DataFrame([{
    "run_id": run_id,
    "run_name": run_name,
    "run_status": "IN PROGRESS",
    "run_message": None,
    "records_inserted": 0,
    "run_start_ts": run_start_time,
    "run_end_ts": None
}]).to_sql(
    "nasa_run_management",
    engine,
    schema="tech",
    index=False,
    if_exists="append"
)

# 🧠 Daily data load
api_key = "jRPuYFCnEid60PvQtnMIyODhZKzSvjS3scs9g5xv"
total_records = 0
try:
    end_date = date.today()
    while current <= end_date:
        chunk_start = current
        chunk_end = min(current + timedelta(days=6), end_date) if exec_mode.upper() == "FULL" else current
        print(f"Fetching NEO feed {chunk_start} to {chunk_end}")
        neo_df = fetch_neo_feed(chunk_start.isoformat(), chunk_end.isoformat(), api_key)
        # Fetch existing ids and dates
        existing_ids_df = pd.read_sql(
            "select id, date from stg_nasa_data.stg_neo_daily",
            engine
        )

        # Left-anti join in pandas
        neo_df = pd.merge(
            neo_df, existing_ids_df,
            on=["id", "date"],
            how="left",
            indicator=True
        )
        neo_df = neo_df[neo_df["_merge"] == "left_only"].drop(columns=["_merge"])

        if not neo_df.empty:
            neo_df["run_id"] = run_id
            neo_df["run_name"] = run_name
            neo_df["run_created_d"] = run_start_time
            neo_df.to_sql(
                "stg_neo_daily",
                engine,
                schema="stg_nasa_data",
                if_exists="append",
                index=False
            )
            total_records += len(neo_df)

        current = chunk_end + timedelta(days=1) if exec_mode.upper() == "FULL" else current + timedelta(days=1)

    # ✅ Complete log for daily
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE tech.nasa_run_management SET run_status='SUCCESS', run_message='Daily load execution', "
                "records_inserted=:cnt, run_end_ts=now() WHERE run_id=:run_id"
            ),
            {"cnt": total_records, "run_id": run_id},
        )
        conn.commit()

except Exception as e:
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE tech.nasa_run_management SET run_status='FAILED', run_message=:msg, run_end_ts=now() WHERE run_id=:run_id"
            ),
            {"msg": str(e)[:200], "run_id": run_id},
        )
        conn.commit()
    raise

# 🏃 Legacy NEO load
pd.DataFrame([{
    "run_id": run_id_browse,
    "run_name": run_name_browse,
    "run_status": "IN PROGRESS",
    "run_message": None,
    "records_inserted": 0,
    "run_start_ts": run_start_browse,
    "run_end_ts": None
}]).to_sql(
    "nasa_run_management",
    engine,
    schema="tech",
    index=False,
    if_exists="append"
)

try:
    total_browse_records = 0
    page = 0
    max_pages = 3  # demo
    while page < max_pages:
        print(f"Fetching browse page {page}")
        browse_df, page_num, total_pages = fetch_neo_browse_page(page, api_key)
        # Fetch existing ids
        existing_ids_df = pd.read_sql(
            "select id from stg_nasa_data.stg_neo_legacy",
            engine
        )

        # Left-anti join in pandas
        browse_df = pd.merge(
            browse_df, existing_ids_df,
            on="id",
            how="left",
            indicator=True
        )
        browse_df = browse_df[browse_df["_merge"] == "left_only"].drop(columns=["_merge"])

        if not browse_df.empty:
            browse_df["run_id"] = run_id_browse
            browse_df["run_name"] = run_name_browse
            browse_df["run_created_d"] = run_start_browse
            browse_df.to_sql(
                "stg_neo_legacy",
                engine,
                schema="stg_nasa_data",
                if_exists="append",
                index=False
            )
            total_browse_records += len(browse_df)

        if page_num >= total_pages - 1:
            break
        page += 1

    # ✅ Complete log for legacy
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE tech.nasa_run_management SET run_status='SUCCESS', run_message='Legacy load successful', "
                "records_inserted=:cnt, run_end_ts=now() WHERE run_id=:run_id"
            ),
            {"cnt": total_browse_records, "run_id": run_id_browse},
        )
        # ✅ Finally switch EXECUTION_MODE if FULL
        if exec_mode.upper() == "FULL":
            conn.execute(
                text(
                    "UPDATE tech.nasa_param SET param_value='DELTA' WHERE param_name='EXECUTION_MODE'"
                )
            )
        conn.commit()

except Exception as e:
    with engine.connect() as conn:
        conn.execute(
            text(
                "UPDATE tech.nasa_run_management SET run_status='FAILED', run_message=:msg, run_end_ts=now() WHERE run_id=:run_id"
            ),
            {"msg": str(e)[:200], "run_id": run_id_browse},
        )
        conn.commit()
    raise
