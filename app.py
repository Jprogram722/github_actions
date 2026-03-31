import pandas as pd
import httpx
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def ingest():

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

    with httpx.Client() as client:
        res = client.get(url)

    print(res)


if __name__ == "__main__":
    ingest()
