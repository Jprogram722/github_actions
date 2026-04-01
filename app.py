import pandas as pd
import httpx
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine
from dotenv import dotenv_values
import os

def load_env() -> dict:
    """
    Loads in the environment variables depending on if its running locally or on github actions
    """
    # local
    if os.path.isfile(".env"):
        return dotenv_values(".env")
    # gh actions
    return {"PGUSER": os.environ.get("PG_USER"), "PGPASSWORD": os.environ.get("PG_PASSWORD")} 

def ingest():

    env_vars = load_env()
    print("env vars fetched")
    db_host = "localhost"
    db_name = "env_can"
    table_name = "weather_history"
    pg_conn_str = f"postgresql+psycopg2://{env_vars["PGUSER"]}:{env_vars["PGPASSWORD"]}@{db_host}:5432/{db_name}"

    # columns needed for the database
    cols = {
        "climate_identifier": str,
        "local_date": str,
        "temp": float,
        "dew_point_temp":float,
        "humidex": int,
        "precip_amount":int,
        "relative_humidity": int,
        "station_pressure": float,
        "visibility": float,
        "weather_eng_desc": str,
        "windchill":int,
        "wind_direction": int,
        "wind_speed": int
    }

    api_time_format = "%Y-%m-%dT%H:%M:%SZ"
    # get a week of data. just incase they don't post yesterdays data
    start_dt = datetime.now(tz=ZoneInfo("America/Halifax")) - timedelta(days=2)
    start_ts = start_dt.date().strftime(api_time_format)
    end_ts = datetime.now(tz=ZoneInfo("America/Halifax")).strftime(api_time_format)

    # setup api endpoint
    domain = "https://api.weather.gc.ca"
    endpoint = "/collections/climate-hourly/items"
    url = f"{domain}{endpoint}"

    query_params = {
        "CLIMATE_IDENTIFIER": "8202251",
        # only get cols for requirements (example: "CLIMATE_IDENTIFIER, LOCAL_DATE, ..")
        "properties": ",".join(col.upper() for col in cols.keys()),
        "skipGeometry": "true",
        # max limit according to docs
        "limit": 10000,
        # sort by local date in asc order
        "sortby": "+LOCAL_DATE",
        "datetime": f"{start_ts}/{end_ts}"
    }

    # single out numeric cols for later
    num_cols = [k for k, v in cols.items() if v is not str]

    with httpx.Client() as client:
        print("requesting data")
        client.params = query_params
        res = client.get(url)
        res.raise_for_status()
        data = res.json()

    print("flatten data")
    # flattens data
    df = pd.json_normalize(data["features"])
    # drop unneed columns and name them to cols for the database
    df = (
        df[[f"properties.{col.upper()}" for col in cols.keys()]]
        .rename(columns={f"properties.{col.upper()}": col for col in cols.keys()})
    )

    df[num_cols] = df[num_cols].fillna(value=0)

    # many problems inserting into sql tables usally come forom schema mismatches
    df = df.astype(cols)

    # grab the latest availiable date (dates are 10 chars long) and signal for malformed dates
    max_dt = datetime.strptime(df["local_date"].max(), "%Y-%m-%d %H:%M:%S")
    max_date = max_dt.date().strftime("%Y-%m-%d")
    df = df[df["local_date"] > max_date]

    # inserting data
    df["insert_time"] = datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    print("inserting into database")
    df.to_sql(name=table_name, con=create_engine(pg_conn_str), if_exists="delete_rows", index=False, method="multi")
    print("DONE!!!!!")


if __name__ == "__main__":
    ingest()
