import requests
import json
import pandas as pd
#from tqdm import tqdm
#
import time
from datetime import datetime, timedelta

import os
from sqlalchemy import text, create_engine
from sqlalchemy.exc import IntegrityError

#file_path = "/home/katerina/AIRFLOW/python_files/City_coordinates.json"
file_path ="/app/City_coordinates.json"

with open(file_path, "r") as json_file:
        city_coordinates_data = json.load(json_file)

#city_coordinates_data       

cities = {
        city: [float(data['lat']), float(data['lon']), data['region']]
        for city, data in city_coordinates_data.items()
        if data['lat'] is not None and data['lon'] is not None and data['region'] is not None # Ensure coordinates are valid
    }

cities

engine = create_engine(
'postgresql+psycopg2:'
'//postgres_user:'         # username for postgres
'weather_password'              # password for postgres
'@65.109.167.23:5432/'   # postgres server name and the exposed port
'weather_database')

#sql_create_dim_cities = """CREATE TABLE if not exists "DIM_cities"
#( 
#    city_id serial PRIMARY key,
#    city_name VARCHAR(100) NOT NULL, 
#    latitude DECIMAL(10, 7) NOT NULL,  
#    longitude DECIMAL(10, 7) NOT NULL,        
#    area VARCHAR(100) NOT NULL
#);"""
# execute the 'sql' query
#with engine.connect().execution_options(autocommit=True) as conn:
#    query = conn.execute(text(sql_create_dim_cities))
# Execute the SQL query and commit
#with engine.begin() as conn:  # Automatically commits at the end
#    conn.execute(text(sql_create_dim_cities))

cities_df = pd.DataFrame.from_dict(cities, columns=['latitude','longitude','area'] , orient='index')
cities_df.reset_index(inplace=True)
cities_df

cities_df.rename(columns={'index': 'city_name'}, inplace=True)
cities_df

# insert the dataframe data to 'staging' SQL table
#with engine.connect().execution_options(autocommit=True) as conn:
#    cities_df.to_sql('DIM_cities', con=conn, if_exists='append', index= False)

#with engine.begin() as conn:  # Automatically commits at the end
#    try:
#        # Attempt to insert data into the table
#        cities_df.to_sql('DIM_cities', con=conn, if_exists='append', index=False)
#    except IntegrityError:
#        print("Primary key violation detected. Skipping conflicting rows and continuing execution.")


existing_cities = pd.read_sql('SELECT city_name FROM "DIM_cities"', engine)

# Keep only new cities that don't exist in the database
cities_df_filtered = cities_df[~cities_df["city_name"].isin(existing_cities["city_name"])]

if not cities_df_filtered.empty:
    cities_df_filtered.to_sql('DIM_cities', con=engine, if_exists='append', index=False)
    
    