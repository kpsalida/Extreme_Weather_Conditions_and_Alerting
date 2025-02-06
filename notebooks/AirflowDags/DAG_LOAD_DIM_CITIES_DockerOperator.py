from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator,BranchSQLOperator 
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
#
from datetime import datetime
import logging
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from docker.types import Mount
from airflow.utils.task_group import TaskGroup


import subprocess
import os
#
import json


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
    'catchup' : False,
    'retries': 0,
}
# Create the DAG

with DAG(
    dag_id='Final_Project_Load_DIMENSIONS_DO',  # DAG name
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    tags=['Final_Project_Load_DIMENSIONS_DO'],
) as dag2:
    with TaskGroup(group_id="load_dimensions") as tg1:
        python_task_load_Cities_DO = DockerOperator(
            task_id='python_task_load_Cities_DO',
            image='docker_image_dimensions_python:v10',
            container_name='docker_python_Container_DIMCities',
            api_version='auto',
            auto_remove=True,
        #    user="root:999",
            command="python /app/FinalProject_LOAD_DIMCities_SQL.py",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge"
            mounts=[
                Mount(source='/home/katerina/AIRFLOW/python_files',target='/app', type="bind")
            ]
    #       mount_tmp_dir = False
            )
        python_task_load_Weather_DO = DockerOperator(
            task_id='python_task_load_Weather_DO',
            image='docker_image_dimensions_python:v10',
            container_name='docker_python_Container_DIMWeather',
            api_version='auto',
            auto_remove=True,
        #    user="root:999",
            command="python /app/FinalProject_LOAD_DIMWeather_SQL.py",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge"
            mounts=[
                Mount(source='/home/katerina/AIRFLOW/python_files',target='/app', type="bind")
            ]
     #       mount_tmp_dir = False
            )
        python_task_load_Cities_DO >> python_task_load_Weather_DO
   # Task : Evaluate the result of Task 1 being True or False
    
    send_discord_message_positive_dim = DiscordWebhookOperator(
        task_id="send_discord_message_positive_dim",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
  #      message="Staging and DIMENION TABLES CREATED",
        message="""
                ✅ **Success!**DIMENSION TABLES LOADED 🎉
                
                🔹 DAG: `{{ dag.dag_id }}`
                🔹 Task: `{{ task.task_id }}`
                🔹 Execution Date: `{{ ts }}`

                All tasks completed successfully! 🚀
            """,
        username="Airflow Alert",
        trigger_rule="all_success"  # Trigger if all upstream tasks succeed
    )

    # Task 4: Send a negative message if any SQLCheckOperator fails
    send_discord_message_negative_dim = DiscordWebhookOperator(
        task_id="send_discord_message_negative_dim",
        http_conn_id="http_conn_id",
        webhook_endpoint="webhooks/1295334584791203882/5I7CJ0HBnALVcx3YZH9f3K7_L40fb6GYjzn1tz2nm_iV0G1HviV3ZvqCGaCnWlEy0-I_",
 #       message="One or more tables failed to be created.",
        message="""
                ❌ **Failure Alert!** DIMENSION TABLES NOT LOADED 🚨
                
                🔹 **DAG:** `{{ dag.dag_id }}`
                🔹 **Task:** `{{ task.task_id }}`
                🔹 **Execution Date:** `{{ ts }}`
                🔹 **Try Number:** `{{ task_instance.try_number }}`

                Please check Airflow logs for details! ⚠️
            """,
        username="Airflow Alert",
        trigger_rule="one_failed"  # Trigger if any upstream task fails
    )

tg1 >> (send_discord_message_positive_dim,send_discord_message_negative_dim)