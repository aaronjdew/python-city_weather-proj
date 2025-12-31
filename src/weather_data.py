import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime as dt


def weather_data(loc: str, lat: float, lng: float, st_date: dt, ed_date: dt) -> pd.DataFrame:
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": st_date,
        "end_date": ed_date,
        "hourly": ["temperature_2m", "dew_point_2m", "pressure_msl", "relative_humidity_2m", "cloud_cover", "precipitation", "rain", "wind_speed_100m", "wind_direction_100m"],
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process location
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(2).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(3).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
    hourly_rain = hourly.Variables(6).ValuesAsNumpy()
    hourly_wind_speed_100m = hourly.Variables(7).ValuesAsNumpy()
    hourly_wind_direction_100m = hourly.Variables(8).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}
    hourly_data["location"] = loc
    hourly_data["temperature"] = hourly_temperature_2m
    hourly_data["dew_point"] = hourly_dew_point_2m
    hourly_data["sea_level_pressure"] = hourly_pressure_msl
    hourly_data["relative_humidity"] = hourly_relative_humidity_2m
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["rain"] = hourly_rain
    hourly_data["wind_speed"] = hourly_wind_speed_100m
    hourly_data["wind_direction"] = hourly_wind_direction_100m

    df_hourly = pd.DataFrame(hourly_data)

    return df_hourly
