import requests
import json
import pandas as pd
#
import time
from datetime import datetime, timedelta
import pytz  # Make sure to install pytz if not already installed
import openmeteo_requests
import requests_cache
from retry_requests import retry
#
import os
import sqlalchemy
from sqlalchemy import text, create_engine
import psycopg2
from psycopg2.extras import execute_batch



engine = create_engine(
'postgresql+psycopg2:'
'//postgres_user:'         # username for postgres
'weather_password'              # password for postgres
'@65.109.167.23:5432/'   # postgres server name and the exposed port
'weather_database')

#Find the cities
file_path = "/app/City_coordinates.json"
#
with open(file_path, "r") as json_file:
        city_coordinates_data = json.load(json_file)
#
cities = {
        city: [float(data['lat']), float(data['lon'])]
        for city, data in city_coordinates_data.items()
        if data['lat'] is not None and data['lon'] is not None  # Ensure coordinates are valid
    }
#
# Setup the Open-Meteo API client with cache and retry on error
#from requests_cache import DO_NOT_CACHE
#cache_session = requests_cache.CachedSession('.cache', expire_after=DO_NOT_CACHE)
#retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
#openmeteo = openmeteo_requests.Client(session=retry_session)
openmeteo = openmeteo_requests.Client()

# Base URL for the API
url = "https://archive-api.open-meteo.com/v1/archive"

# Dataframe to store data for each city
Staging_lst = []
start_date = '2020-01-01'
end_date = '2025-01-20'

# Loop through each city
for city, coordinates in cities.items():
    latitude, longitude = coordinates

    # API parameters
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
	"latitude": latitude,
	"longitude": longitude,
	"start_date": start_date,
	"end_date": end_date,
	"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "snowfall", "snow_depth", "weather_code", "cloud_cover", "cloud_cover_high", "wind_speed_10m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"]
}
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Process the response
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity": hourly.Variables(1).ValuesAsNumpy(),
        "dew_point": hourly.Variables(2).ValuesAsNumpy(),
        "apparent_temperature" : hourly.Variables(3).ValuesAsNumpy(),
        "precipitation": hourly.Variables(4).ValuesAsNumpy(),
        "rain": hourly.Variables(5).ValuesAsNumpy(),
        "snowfall": hourly.Variables(6).ValuesAsNumpy(),
        "snow_depth": hourly.Variables(7).ValuesAsNumpy(),
        "weather_code": hourly.Variables(8).ValuesAsNumpy(),
        "cloud_cover_total" : hourly.Variables(9).ValuesAsNumpy(),
        "cloud_cover_high": hourly.Variables(10).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(11).ValuesAsNumpy(),
        "wind_gusts_10m": hourly.Variables(12).ValuesAsNumpy(),
        "soil_temperature_0_to_7cm" : hourly.Variables(13).ValuesAsNumpy(),
        "soil_moisture_0_to_7cm": hourly.Variables(14).ValuesAsNumpy(),
        "soil_moisture_7_to_28cm" : hourly.Variables(15).ValuesAsNumpy() 
        }
    except Exception as e:
        print(f"Error processing city {city}: {e}")
#    finally:
#        cache_session.cache.clear()

   # Process hourly data. The order of variables needs to be the same as requested.

    # Create a DataFrame for the current city
    hourly_dataframe = pd.DataFrame(hourly_data)

    # Add a column for the city name
    hourly_dataframe['city'] = city

    # Append the DataFrame to the list
    Staging_lst.append(hourly_dataframe)
    # Delay before the next request
    time.sleep(5)  # Adjust the delay as needed
# Combine all city-specific DataFrames into a single DataFrame
staging_df = pd.concat(Staging_lst, ignore_index=True)  
#
staging_df['year'] = staging_df['date'].dt.year
staging_df['month'] = staging_df['date'].dt.month
staging_df['day'] = staging_df['date'].dt.day
staging_df['date_only'] = staging_df['date'].dt.date
#
columns_reorg= ['date','date_only','year','month','day','city','temperature_2m', 'relative_humidity', 'dew_point','apparent_temperature', 'precipitation', 'rain', 'snowfall','snow_depth', 'weather_code', 'cloud_cover_total', 'cloud_cover_high','wind_speed_10m', 'wind_gusts_10m', 'soil_temperature_0_to_7cm','soil_moisture_0_to_7cm','soil_moisture_7_to_28cm']
#
staging_df = staging_df[columns_reorg]

#Update the staging table . If the timestamp exists, then update the fields, if it does not exist then 
# get the necessary credentials from enviromental variables
#POSTGRES_USER = os.getenv('POSTGRES_USER')
#POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
#POSTGRES_SERVER_NAME = os.getenv('POSTGRES_SERVER_NAME')
#POSTGRES_DB = os.getenv('POSTGRES_DB')
#
#engine = create_engine(
#f'postgresql+psycopg2:'
#f'//{POSTGRES_USER}:'               # username for postgres
#f'{POSTGRES_PASSWORD}'              # password for postgres
#f'@{POSTGRES_SERVER_NAME}:5432/'    # postgres server name and the exposed port
#f'{POSTGRES_DB}')
#
engine = create_engine(
'postgresql+psycopg2:'
'//postgres_user:'         # username for postgres
'weather_password'              # password for postgres
'@65.109.167.23:5432/'   # postgres server name and the exposed port
'weather_database')


#sql_create_staging = """CREATE TABLE IF NOT EXISTS staging_archive(
#    date TIMESTAMPTZ NOT NULL, -- Includes timezone
#    date_only DATE NOT NULL,
#    year INT NOT NULL,
#    month INT NOT NULL,
#    day INT NOT NULL,
#    city VARCHAR(100) NOT NULL,
#    temperature_2m FLOAT, -- Adjust precision if necessary
#    relative_humidity FLOAT, -- Adjust precision if necessary
#    dew_point FLOAT, -- Adjust precision if necessary
#    apparent_temperature FLOAT,
#    precipitation FLOAT,
#    rain FLOAT,
#    snowfall FLOAT,
#   snow_depth FLOAT,
#    weather_code FLOAT,
#    cloud_cover_total FLOAT,            
#    cloud_cover_high FLOAT,
#    wind_speed_10m FLOAT,
#    wind_gusts_10m FLOAT,
#    soil_temperature_0_to_7cm FLOAT,           
#    soil_moisture_0_to_7cm FLOAT,       
#    soil_moisture_7_to_28cm FLOAT,
#    PRIMARY KEY (city, date)
#);"""
# execute the 'sql' query
#with engine.begin() as conn:  # Automatically commits at the end
#    conn.execute(text(sql_create_staging))

# Database connection parameters
DB_CONFIG = {
    "dbname": "weather_database",
    "user": "postgres_user",
    "password": "weather_password",
    "host": "65.109.167.23",
    "port": "5432",
}

# Insert data into the table with ON CONFLICT handling
def upsert_staging_archive(dataframe, conn):
    query = """
    INSERT INTO staging_archive (
        date,
        date_only,
        year,
        month,
        day,
        city,
        temperature_2m,
        relative_humidity,
        dew_point,
        apparent_temperature,
        precipitation,
        rain,
        snowfall,
        snow_depth,
        weather_code,
        cloud_cover_total,
        cloud_cover_high,
        wind_speed_10m,
        wind_gusts_10m,
        soil_temperature_0_to_7cm,
        soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (city, date)
    DO UPDATE SET
        date_only = EXCLUDED.date_only,
        year = EXCLUDED.year,
        month = EXCLUDED.month,
        day = EXCLUDED.day,
        temperature_2m = EXCLUDED.temperature_2m,
        relative_humidity = EXCLUDED.relative_humidity,
        dew_point = EXCLUDED.dew_point,
        apparent_temperature = EXCLUDED.apparent_temperature,
        precipitation = EXCLUDED.precipitation,
        rain = EXCLUDED.rain,
        snowfall = EXCLUDED.snowfall,
        snow_depth = EXCLUDED.snow_depth,
        weather_code = EXCLUDED.weather_code,
        cloud_cover_total = EXCLUDED.cloud_cover_total,
        cloud_cover_high = EXCLUDED.cloud_cover_high,
        wind_speed_10m = EXCLUDED.wind_speed_10m,
        wind_gusts_10m = EXCLUDED.wind_gusts_10m,
        soil_temperature_0_to_7cm = EXCLUDED.soil_temperature_0_to_7cm,
        soil_moisture_0_to_7cm = EXCLUDED.soil_moisture_0_to_7cm,
        soil_moisture_7_to_28cm = EXCLUDED.soil_moisture_7_to_28cm;
    """
# Convert dataframe to list of tuples for batch insertion
    data_tuples = dataframe.to_records(index=False).tolist()
    
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, data_tuples)  # Efficient batch execution
            conn.commit()
            print("Data successfully upserted into staging_archive.")
    
    except psycopg2.IntegrityError as e:
        conn.rollback()
        print(f"IntegrityError: Skipping duplicate key entry - {e}")

    except Exception as e:
        conn.rollback()
        print(f"Unexpected Error: {e}")



# Establish database connection and upsert data
try:
    conn = psycopg2.connect(**DB_CONFIG)
    upsert_staging_archive(staging_df, conn)
finally:
    conn.close()


