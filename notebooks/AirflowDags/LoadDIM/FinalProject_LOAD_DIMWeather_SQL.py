import requests
import json
import pandas as pd
#from tqdm import tqdm

#
#from IPython.display import display

import os
from sqlalchemy import text, create_engine

#file_path = "/home/katerina/PROJECT/extreme-weather-conditions-and-real-time-alerting/WeatherCodes.json"
file_path = "/app/Weather_Codes_severity.json"

with open(file_path, "r") as json_file:
        weather_codes = json.load(json_file)

weather_codes

# Transform data into a DataFrame
weather_df = pd.DataFrame(weather_codes)

weather_df

engine = create_engine(
'postgresql+psycopg2:'
'//postgres_user:'         # username for postgres
'weather_password'              # password for postgres
'@65.109.167.23:5432/'   # postgres server name and the exposed port
'weather_database')

#sql_create_dim_weathercode = """CREATE TABLE if not exists "DIM_WeatherCode"
#( 
#    weather_code serial PRIMARY key,
#    weatherdescription VARCHAR(255) NOT NULL, 
#;"""
# Execute the SQL query and commit
#with engine.begin() as conn:  # Automatically commits at the end
#    conn.execute(text(sql_create_dim_weathercode))

# insert the dataframe data to 'staging' SQL table
#with engine.connect().execution_options(autocommit=True) as conn:
#    weather_df.to_sql('DIM_WeatherCode', con=conn, if_exists='append', index= False)

existing_weather = pd.read_sql('SELECT weather_code FROM "DIM_WeatherCode"', engine)

# Keep only new cities that don't exist in the database
weather_df_filtered = weather_df[~weather_df["weather_code"].isin(existing_weather["weather_code"])]

if not weather_df_filtered.empty:
    weather_df_filtered.to_sql('DIM_WeatherCode', con=engine, if_exists='append', index=False)