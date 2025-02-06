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
from airflow.providers.docker.operators.docker import DockerOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'catchup' : False,
    'retries': 0,
}
#SQLSCRIPTS
sql_script_create_fact = """
    BEGIN;
    DROP TABLE IF EXISTS "Fact_Archive_Data" CASCADE;
    CREATE TABLE IF NOT EXISTS "Fact_Archive_Data" (
    city_id INT NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    temperature_2m FLOAT,
    relative_humidity FLOAT,
    dew_point FLOAT,
    apparent_temperature FLOAT,
    precipitation FLOAT,
    rain FLOAT,
    snowfall FLOAT,
    snow_depth FLOAT,
    weather_code INT,
    cloud_cover_total FLOAT,
    cloud_cover_high FLOAT,
    wind_speed_10m FLOAT,
    wind_gust_10m FLOAT,
    soil_temperature_0_to_7cm FLOAT,
    soil_moisture_0_to_7cm FLOAT,
    soil_moisture_7_to_28cm FLOAT,
    PRIMARY KEY (city_id, date)
);
    COMMIT;
"""  

sql_script_create_TableDependencies = """
BEGIN;
ALTER TABLE "Fact_Archive_Data"
    ADD CONSTRAINT fk_city_fact
    FOREIGN KEY (city_id) REFERENCES "DIM_cities"(city_id);
ALTER TABLE "Fact_Archive_Data"
    ADD CONSTRAINT fk_weather_code_fact
    FOREIGN KEY (weather_code) REFERENCES "DIM_WeatherCode"(weather_code);
COMMIT;
"""
#
sql_script_create_FullLoadFact = """
INSERT INTO "Fact_Archive_Data" (
    city_id,
    date,
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
    wind_gust_10m,
    soil_temperature_0_to_7cm,
    soil_moisture_0_to_7cm,
    soil_moisture_7_to_28cm
)
SELECT
    c.city_id,
    s.date,
    s.temperature_2m,
    s.relative_humidity,
    s.dew_point,
    s.apparent_temperature,
    s.precipitation,
    s.rain,
    s.snowfall,
    s.snow_depth,
    w.weather_code,
    s.cloud_cover_total,
    s.cloud_cover_high,
    s.wind_speed_10m,
    s.wind_gusts_10m,
    s.soil_temperature_0_to_7cm,
    s.soil_moisture_0_to_7cm,
    s.soil_moisture_7_to_28cm
FROM staging_archive s
JOIN "DIM_cities" c ON s.city = c.city_name
JOIN "DIM_WeatherCode" w ON s.weather_code = w.weather_code
ON CONFLICT (city_id, date) DO NOTHING;
COMMIT;
"""
#
#
# Create the DAG
with DAG(
    dag_id='Final_Project_CreateLoadFact',  # DAG name
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    tags=['Final_Project_CreateLoadFact'],
) as dag:
    
#Task 1 : run the query
    with TaskGroup(group_id="fact_table_creation") as tg1:
        task_sql_create_fact = SQLExecuteQueryOperator(
        task_id='task_sql_create_fact',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_fact
        )
        task_sql_create_FactDependencies = SQLExecuteQueryOperator(
        task_id='task_sql_create_FactDependencies',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_TableDependencies
        )
        task_sql_create_fact >> task_sql_create_FactDependencies
#
    task_sql_create_FullLoadFact = SQLExecuteQueryOperator(
        task_id='task_sql_create_FullLoadFact',
        conn_id='PostgresDB',  # Reference to the Airflow connection
        sql=sql_script_create_FullLoadFact
        )
    # Task : Evaluate the result of Task 1 being True or False
    
    send_discord_message_positive_fact = DiscordWebhookOperator(
        task_id="send_discord_message_positive_fact",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
  #      message="Staging and DIMENION TABLES CREATED",
        message="""
                ✅ **Success!** FACT TABLE CREATED and FIRST Loaded 🎉
                
                🔹 DAG: `{{ dag.dag_id }}`
                🔹 Task: `{{ task.task_id }}`
                🔹 Execution Date: `{{ ts }}`

                All tasks completed successfully! 🚀
            """,
        username="Airflow Alert",
        trigger_rule="all_success"  # Trigger if all upstream tasks succeed
    )

    # Task 4: Send a negative message if any SQLCheckOperator fails
    send_discord_message_negative_fact = DiscordWebhookOperator(
        task_id="send_discord_message_negative_fact",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
 #       message="One or more tables failed to be created.",
        message="""
                ❌ **Failure Alert!** FACT TABLE failed to Load 🚨
                
                🔹 **DAG:** `{{ dag.dag_id }}`
                🔹 **Task:** `{{ task_instance.task_id }}`
                🔹 **Execution Date:** `{{ ts }}`
                🔹 **Try Number:** `{{ task_instance.try_number }}`

                Please check Airflow logs for details! ⚠️
            """,
        username="Airflow Alert",
        trigger_rule="one_failed"  # Trigger if any upstream task fails
    )

 
    # Set dependencies between corresponding tasks
                    
    tg1 >> task_sql_create_FullLoadFact >> (send_discord_message_positive_fact,send_discord_message_negative_fact)

     

