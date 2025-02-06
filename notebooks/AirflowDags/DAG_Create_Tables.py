from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator,BranchSQLOperator 
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import logging
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'catchup' : False,
    'retries': 0,
}
#SQLSCRIPTS
sql_script_create_staging = """
    BEGIN;
    DROP TABLE IF EXISTS staging_archive;
    CREATE TABLE IF NOT EXISTS staging_archive(
    date TIMESTAMPTZ NOT NULL, -- Includes timezone
    date_only DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    city VARCHAR(100) NOT NULL,
    temperature_2m FLOAT, -- Adjust precision if necessary
    relative_humidity FLOAT, -- Adjust precision if necessary
    dew_point FLOAT, -- Adjust precision if necessary
    apparent_temperature FLOAT,
    precipitation FLOAT,
    rain FLOAT,
    snowfall FLOAT,
    snow_depth FLOAT,
    weather_code INT,
    cloud_cover_total FLOAT,            
    cloud_cover_high FLOAT,
    wind_speed_10m FLOAT,
    wind_gusts_10m FLOAT,
    soil_temperature_0_to_7cm FLOAT,           
    soil_moisture_0_to_7cm FLOAT,       
    soil_moisture_7_to_28cm FLOAT,
    PRIMARY KEY (city, date)
    );
    COMMIT;
"""  

sql_script_create_DIM_cities ="""
BEGIN;
DROP TABLE IF EXISTS "DIM_cities" CASCADE;
CREATE TABLE "DIM_cities"( 
    city_id serial PRIMARY key,
    city_name VARCHAR(100) NOT NULL UNIQUE, 
    latitude DECIMAL(10, 7) NOT NULL,  
    longitude DECIMAL(10, 7) NOT NULL,        
    area VARCHAR(100) NOT NULL
    );
COMMIT;
"""

sql_script_create_DIM_WeatherCode = """
BEGIN;
DROP TABLE IF EXISTS "DIM_WeatherCode" CASCADE;
CREATE TABLE IF NOT EXISTS "DIM_WeatherCode"( 
    weather_code serial PRIMARY key,
    weatherdescription VARCHAR(255) NOT NULL,
    weathercategory VARCHAR(255) NOT NULL
    );
COMMIT;
"""
sql_script_create_TableDependencies = """
BEGIN;
ALTER TABLE "staging_archive"
    ADD CONSTRAINT fk_city
    FOREIGN KEY (city) REFERENCES "DIM_cities"(city_name);
ALTER TABLE "staging_archive"
    ADD CONSTRAINT fk_weather_code
    FOREIGN KEY (weather_code) REFERENCES "DIM_WeatherCode"(weather_code);
COMMIT;
"""
#
# Create the DAG
with DAG(
    dag_id='Final_Project_CreateTables',  # DAG name
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    tags=['Final_Project_CreateTables'],
) as dag1:
    
#Task 1 : run the query
    with TaskGroup(group_id="check_table_creation") as tg1:
        task_sql_create_staging = SQLExecuteQueryOperator(
        task_id='task_sql_create_staging',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_staging
        )
        task_sql_create_DIM_cities = SQLExecuteQueryOperator(
        task_id='task_sql_create_DIM_cities',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_DIM_cities
        )
        task_sql_create_DIM_weather = SQLExecuteQueryOperator(
        task_id='task_sql_create_DIM_weather',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_DIM_WeatherCode
        )
        task_sql_create_staging >> task_sql_create_DIM_cities >> task_sql_create_DIM_weather
#    
    task_sql_create_TableDependencies = SQLExecuteQueryOperator(
    task_id='task_sql_create_TableDependencies',
    conn_id='PostgresDB',  # Reference to the Airflow connection
    sql=sql_script_create_TableDependencies
    )

    # Task : Evaluate the result of Task 1 being True or False
    
    send_discord_message_positive = DiscordWebhookOperator(
        task_id="send_discord_message_positive",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
  #      message="Staging and DIMENION TABLES CREATED",
        message="""
                ✅ **Success!** Staging and DIMENSION TABLES CREATED 🎉
                
                🔹 DAG: `{{ dag.dag_id }}`
                🔹 Task: `{{ task.task_id }}`
                🔹 Execution Date: `{{ ts }}`

                All tasks completed successfully! 🚀
            """,
        username="Airflow Alert",
        trigger_rule="all_success"  # Trigger if all upstream tasks succeed
    )

    # Task 4: Send a negative message if any SQLCheckOperator fails
    send_discord_message_negative = DiscordWebhookOperator(
        task_id="send_discord_message_negative",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
 #       message="One or more tables failed to be created.",
        message="""
                ❌ **Failure Alert!** One or more tasks failed 🚨
                
                🔹 **DAG:** `{{ dag.dag_id }}`
                🔹 **Task:** `{{ task.task_id }}`
                🔹 **Execution Date:** `{{ ts }}`
                🔹 **Try Number:** `{{ task_instance.try_number }}`

                Please check Airflow logs for details! ⚠️
            """,
        username="Airflow Alert",
        trigger_rule="one_failed"  # Trigger if any upstream task fails
    )

 
    # Set dependencies between corresponding tasks
    #task_sql_create_staging>>task_sql_create_DIM_cities >>task_sql_create_DIM_weather
    #ask_check_staging >> task_check_DIMcities>> task_check_DIMweather>>(send_discord_message_positive,send_discord_message_negative)
    #
                
    tg1 >> task_sql_create_TableDependencies>>(send_discord_message_positive,send_discord_message_negative)
    #tg1 >> [send_discord_message_positive,send_discord_message_negative]
     

