import requests
import pandas as pd
from datetime import date,timedelta, datetime
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import lit

######## FIRST JOB MANAGEMENT
run_id = str(uuid.uuid4())
run_name = "NASA_DAILY_LOAD"
run_start_time = datetime.now()
# Log initial status
initial_log = [(run_id, run_name, "IN PROGRESS",None,0, run_start_time, None)]
schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("run_name", StringType(), True),
    StructField("run_status", StringType(), True),
    StructField("run_message", StringType(), True),
    StructField("records_inserted", IntegerType(), True),
    StructField("run_start_ts", TimestampType(), True),
    StructField("run_end_ts", TimestampType(), True),
])
log_df = spark.createDataFrame(initial_log, schema)
log_df.write.mode("append").saveAsTable("TECH.RUN_MANAGEMENT")
run_status = "IN PROGRESS"
total_records = 0  

######## SECOND JOB MANAGEMENT
run_id_browse = str(uuid.uuid4())
run_name_browse = "NASA_LEGACY_LOAD"
run_start_browse = datetime.now()
# Log initial status
initial_browse_log = [(run_id_browse, run_name_browse, "IN PROGRESS", None, 0, run_start_browse, None)]
log_browse_df = spark.createDataFrame(initial_browse_log, schema)
log_browse_df.write.mode("append").saveAsTable("TECH.RUN_MANAGEMENT")
run_status_browse = "IN PROGRESS"
total_browse_records = 0





try:
    ##NASA API KEY
    api_key = 'jRPuYFCnEid60PvQtnMIyODhZKzSvjS3scs9g5xv'

    def fetch_neo_data(start_date, end_date, api_key):
        url = f"https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": start_date, "end_date": end_date, "api_key": api_key}
        response = requests.get(url, params=params)
        data = response.json()
        neos = []
        
        for date in data["near_earth_objects"]:
            for obj in data["near_earth_objects"][date]:
                neos.append({
                    "id": obj["id"],
                    "name": obj["name"],
                    "date": date,
                    "est_diameter_min": obj["estimated_diameter"]["meters"]["estimated_diameter_min"],
                    "est_diameter_max": obj["estimated_diameter"]["meters"]["estimated_diameter_max"],
                    "is_potentially_hazardous": obj["is_potentially_hazardous_asteroid"],
                    "close_approach_date": obj["close_approach_data"][0]["close_approach_date"],
                    "velocity_kph": float(obj["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]),
                    "miss_distance_km": float(obj["close_approach_data"][0]["miss_distance"]["kilometers"]),
                    "orbiting_body": obj["close_approach_data"][0]["orbiting_body"]
                })
        
        return pd.DataFrame(neos)

    # 1. Read execution mode from TECH.NASA_PARAM
    exec_mode_df = spark.sql("select param_value from tech.nasa_param where param_name='EXECUTION_MODE'")
    if exec_mode_df.count() == 0:
        raise ValueError("EXECUTION_MODE not found in TECH.NASA_PARAM table.")

    exec_mode = exec_mode_df.collect()[0]["param_value"]


    # 2. Define date range
    # 2. Define date range
    end_date = date.today()

    if exec_mode.upper() == "FULL":
        historical_start = date(2015, 9, 7)  # earliest NEO feed data you want
        current = historical_start
    else:
        # Get latest run_end_ts from RUN_MANAGEMENT for daily load
        latest_ts_df = spark.sql("""
            SELECT MAX(run_end_ts) as last_run_end
            FROM TECH.RUN_MANAGEMENT
            WHERE run_name = 'NASA_DAILY_LOAD' AND run_status = 'SUCCESS'
        """)
        if latest_ts_df.count() == 0 or latest_ts_df.collect()[0]["last_run_end"] is None:
            raise ValueError("No successful previous run found for DELTA mode.")
        last_run_end = latest_ts_df.collect()[0]["last_run_end"].date()
        current = last_run_end + timedelta(days=1)  # start from next day

    ## 3. SCHEMA CREATION
    spark.sql("CREATE SCHEMA IF NOT EXISTS STG_NASA")

    # 4. Write mode
    is_full = exec_mode.upper() == "FULL"
    mode = "append"

    # 5. Run loop
    total_records = 0

    if is_full:
        print(f"Starting FULL historical load from {current} to {end_date}")
        while current <= end_date:
            chunk_start = current
            chunk_end = min(current + timedelta(days=6), end_date)
            print(f"Fetching data for chunk {chunk_start} to {chunk_end}")

            neo_df = fetch_neo_data(chunk_start.isoformat(), chunk_end.isoformat(), api_key)
            if not neo_df.empty:
                spark_neo_df = spark.createDataFrame(neo_df)
                record_count = spark_neo_df.count()
                # Check for existing id and date in target
                existing_ids_df = spark.sql("SELECT id, date FROM STG_NASA.STG_NEO_DAILY").distinct()
                spark_neo_df = spark_neo_df.join(existing_ids_df, on=["id", "date"], how="left_anti")
                if record_count > 0:
                    # Add columns
                    spark_neo_df = spark_neo_df.withColumn("run_id", lit(run_id)) \
                                            .withColumn("run_name", lit(run_name)) \
                                            .withColumn("run_created_d", lit(run_start_time))

                    # Reorder columns so new ones are at the end
                    original_cols = [c for c in spark_neo_df.columns if c not in ["run_id","run_name","run_created_d"]]
                    spark_neo_df = spark_neo_df.select(*original_cols, "run_id", "run_name", "run_created_d")

                    # Write the data
                    spark_neo_df.write.format("delta").mode(mode).saveAsTable("STG_NASA.STG_NEO_DAILY")
                    total_records += record_count
            current = chunk_end + timedelta(days=1)

    else:
        # DELTA mode
        while current <= end_date:
            day_str = current.isoformat()
            print(f"Fetching data for {day_str} with mode={mode}")

            neo_df = fetch_neo_data(day_str, day_str, api_key)
            if not neo_df.empty:
                spark_neo_df = spark.createDataFrame(neo_df)
                record_count = spark_neo_df.count()
                # Check for existing id and date in target
                existing_ids_df = spark.sql("SELECT id, date FROM STG_NASA.STG_NEO_DAILY").distinct()
                spark_neo_df = spark_neo_df.join(existing_ids_df, on=["id", "date"], how="left_anti")
                if record_count > 0:
                    # Add columns
                    spark_neo_df = spark_neo_df.withColumn("run_id", lit(run_id)) \
                                            .withColumn("run_name", lit(run_name)) \
                                            .withColumn("run_created_d", lit(run_start_time))

                    # Reorder columns so new ones are at the end
                    original_cols = [c for c in spark_neo_df.columns if c not in ["run_id","run_name","run_created_d"]]
                    spark_neo_df = spark_neo_df.select(*original_cols, "run_id", "run_name", "run_created_d")

                    # Write the data
                    spark_neo_df.write.format("delta").mode(mode).saveAsTable("STG_NASA.STG_NEO_DAILY")
                    total_records += record_count
            current += timedelta(days=1)

    # 6. Update EXECUTION_MODE after full load
    if is_full:
        spark.sql("UPDATE TECH.NASA_PARAM SET PARAM_VALUE = 'DELTA' where param_name='EXECUTION_MODE'")
    
    run_status = "SUCCESS"
    run_message = "Daily load execution"


except Exception as e:
    run_status = "FAILED"
    run_message = str(e)[:200]  # truncate to 200 characters
    print(f"Daily load failed: {run_message}")
    raise

finally:
    run_end_time = datetime.now()
 
    
    # Overwrite the same run_id with final status and metrics
    final_log = [(run_id, run_name, run_status,run_message,total_records, run_start_time, run_end_time)]
    final_log_df = spark.createDataFrame(final_log, ["run_id", "run_name", "run_status","run_message","records_inserted", "run_start_ts", "run_end_ts"])
    
    # Overwrite the existing record
    final_log_df.createOrReplaceTempView("final_log_update")
    spark.sql("""
        MERGE INTO TECH.RUN_MANAGEMENT tgt
        USING final_log_update src
        ON tgt.run_id = src.run_id
        WHEN MATCHED THEN UPDATE SET
            tgt.records_inserted = src.records_inserted,
            tgt.run_end_ts = src.run_end_ts,
            tgt.run_status = src.run_status,
            tgt.run_message = src.run_message
    """)
    if run_status == "SUCCESS" and is_full:
            spark.sql("UPDATE TECH.NASA_PARAM SET PARAM_VALUE = 'DELTA' where param_name='EXECUTION_MODE'")
    

try:
    print("Starting historical NEO browse load...")

    browse_records = []

    def fetch_neo_browse_page(page, api_key, size=100):
        url = f"https://api.nasa.gov/neo/rest/v1/neo/browse"
        params = {
            "api_key": api_key,
            "page": page,
            "size": size
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    # Determine starting page for browse load
    if exec_mode.upper() == "DELTA":
        latest_browse_df = spark.sql("""
            SELECT MAX(run_end_ts) as last_browse_ts
            FROM TECH.RUN_MANAGEMENT
            WHERE run_name = 'NASA_LEGACY_LOAD' AND run_status = 'SUCCESS'
        """)
        
        if latest_browse_df.count() > 0 and latest_browse_df.collect()[0]["last_browse_ts"] is not None:
            existing_browse_count = spark.sql("SELECT COUNT(*) as cnt FROM STG_NASA.STG_NEO_LEGACY").collect()[0]["cnt"]
            start_page = existing_browse_count // 100
        else:
            start_page = 0
    else:
        start_page = 0

    max_pages = start_page + 3  # fetch 3 pages from start point

    page = start_page
    print(f"Starting from legacy page {page} in {exec_mode.upper()} mode")

    while page < max_pages:
        print(f"Fetching Legacy page {page}")
        data = fetch_neo_browse_page(page, api_key)

        for neo in data["near_earth_objects"]:
            browse_records.append({
                "id": neo["id"],
                "name": neo["name"],
                "absolute_magnitude_h": neo["absolute_magnitude_h"],
                "is_potentially_hazardous": neo["is_potentially_hazardous_asteroid"],
                "est_diameter_min": neo["estimated_diameter"]["meters"]["estimated_diameter_min"],
                "est_diameter_max": neo["estimated_diameter"]["meters"]["estimated_diameter_max"],
                "n_close_approaches": len(neo["close_approach_data"])
            })

        if data["page"]["number"] >= data["page"]["total_pages"] - 1:
            break
        page += 1


    if browse_records:
        browse_df = pd.DataFrame(browse_records)
        spark_browse_df = spark.createDataFrame(browse_df)
        #lines to implement if we want to check for ID
        existing_browse_ids_df = spark.sql("SELECT id FROM STG_NASA.STG_NEO_LEGACY").distinct()
        spark_browse_df = spark_browse_df.join(existing_browse_ids_df, on="id", how="left_anti")
        
        total_browse_records = spark_browse_df.count()
        if total_browse_records > 0:
            # Add columns
            spark_browse_df = spark_browse_df.withColumn("run_id", lit(run_id_browse)) \
                                            .withColumn("run_name", lit(run_name_browse)) \
                                            .withColumn("run_created_d", lit(run_start_browse))

            # Reorder columns so new ones are at the end
            original_cols = [c for c in spark_browse_df.columns if c not in ["run_id","run_name","run_created_d"]]
            spark_browse_df = spark_browse_df.select(*original_cols, "run_id", "run_name", "run_created_d")
            # Write columns
            spark_browse_df.write.format("delta").mode(mode).saveAsTable("STG_NASA.STG_NEO_LEGACY")


    run_status_browse = "SUCCESS"
    run_message_browse = "Legacy load successful"

except Exception as e:
    run_status_browse = "FAILED"
    run_message_browse = str(e)[:200]
    print(f"Legacy load failed: {run_message_browse}")
    raise

finally:
    run_end_browse = datetime.now()
    final_browse_log = [(run_id_browse, run_name_browse, run_status_browse, run_message_browse, total_browse_records, run_start_browse, run_end_browse)]
    final_browse_df = spark.createDataFrame(final_browse_log, schema)
    final_browse_df.createOrReplaceTempView("final_browse_log_update")

    spark.sql("""
        MERGE INTO TECH.RUN_MANAGEMENT tgt
        USING final_browse_log_update src
        ON tgt.run_id = src.run_id
        WHEN MATCHED THEN UPDATE SET
            tgt.records_inserted = src.records_inserted,
            tgt.run_end_ts = src.run_end_ts,
            tgt.run_status = src.run_status,
            tgt.run_message = src.run_message
    """)

    if run_status_browse == "SUCCESS" and is_full:
            spark.sql("UPDATE TECH.NASA_PARAM SET PARAM_VALUE = 'DELTA' where param_name='EXECUTION_MODE'")
    