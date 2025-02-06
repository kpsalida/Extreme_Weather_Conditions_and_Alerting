import requests
import json
import pandas as pd
#
import time
from datetime import datetime, timedelta
import pytz  # Make sure to install pytz if not already installed
#
import openmeteo_requests
import requests_cache
from retry_requests import retry
#
#import os
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


# Find the final loaded day in the FACT Table with Historical Data
with engine.begin() as connection:
    result_fact = connection.execute(text("SELECT MAX(date) FROM \"Fact_Archive_Data\";"))
    max_date_fact = result_fact.scalar().date()+timedelta(days=1) # Use .date() to get a datetime.date object
#print(f'start api date: {max_date_fact}')

#
#Find the date of now() - 3 days. These are the number of days where data do not exist in the open-meteo database.
#Three days before today
dt = datetime.now() - timedelta(days = 3)
# Convert to UTC and add timezone information
dt_utc = dt.replace(tzinfo=pytz.UTC)
# Format the datetime object to the desired string format
end_date = dt_utc.strftime('%Y-%m-%d')
#print(f"end_api_date: {end_date}")

#output_data = {"start_date": str(max_date_fact), "end_date": end_date}
#print(json.dumps(output_data))

#Call the API from start_date to end_date
#file_path = "/home/katerina/PROJECT/extreme-weather-conditions-and-real-time-alerting/city_coordinates.json"
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

# Loop through each city
for city, coordinates in cities.items():
    latitude, longitude = coordinates

    # API parameters
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
	"latitude": latitude,
	"longitude": longitude,
	"start_date": max_date_fact,
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
        soil_moisture_7_to_28cm = EXCLUDED.soil_moisture_7_to_28cm
    RETURNING (xmax = 0) AS inserted;  -- Returns TRUE if inserted, FALSE if updated    
    """
    # Convert dataframe to list of tuples for batch insertion
    data_tuples = dataframe.to_records(index=False).tolist()
    
    try:
        with conn.cursor() as cur:
            cur.executemany(query, data_tuples)  # Execute in batch mode
            try:
                results = cur.fetchall()  # Fetch RETURNING values
            # Count only newly inserted rows
                inserted_rows = sum(1 for row in results if row[0]) if results else 0  # True means newly inserted
            except psycopg2.ProgrammingError:
                inserted_rows = 0  # No rows were inserted or updated
            conn.commit()
            return inserted_rows  # Return only inserted rows count

    except psycopg2.IntegrityError as e:
        conn.rollback()
        print(f"IntegrityError: Skipping duplicate key entry - {e}")
        return 0

    except Exception as e:
        conn.rollback()
        print(f"Unexpected Error: {e}")
        return 0
    
# Establish database connection and upsert data
try:
    conn = psycopg2.connect(**DB_CONFIG)
    upsert_staging_archive(staging_df, conn)
    inserted_rows = staging_df.shape[0]
finally:
    conn.close()
    
# Assuming `upserted_rows` contains the count of upserted rows
output_data = {
    "start_date": str(max_date_fact),
    "end_date": end_date,
    "inserted_rows": inserted_rows # Include the upserted rows count
}

# Send data to XCom
print(json.dumps(output_data))


