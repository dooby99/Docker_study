from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import pendulum
import requests
import pandas as pd
import io
import time

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 7, 25, 7, 0, 0, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def create_table_if_not_exists(redshift_hook):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_data.WRN_NOW_DATA (
        REG_UP VARCHAR(50),
        REG_UP_KO VARCHAR(50),
        REG_ID VARCHAR(50),
        REG_KO VARCHAR(50),
        TM_FC TIMESTAMP,
        TM_EF TIMESTAMP,
        WRN_ID VARCHAR(11),
        WRN_LVL VARCHAR(11),
        WRN_CMD VARCHAR(11),
        ED_TM VARCHAR(110)
    );
    """
    redshift_hook.run(create_table_query)

def load_s3_to_redshift(year, month, day):
    bucket_name = 'team-okky-1-bucket'
    formatted_date = f"{year}_{month}_{day}"
    s3_key = f'special_weather_now/wrn_comparison_target/weather_data.csv'

    redshift_hook = PostgresHook(postgres_conn_id='redshift_default')
    
    create_table_if_not_exists(redshift_hook)

    copy_query = f"""
    COPY raw_data.WRN_NOW_DATA
    FROM 's3://{bucket_name}/{s3_key}'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
    CSV
    IGNOREHEADER 1
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    """
    try:
        redshift_hook.run(copy_query)
        print(f"Successfully copied data for {formatted_date} to Redshift")
    except Exception as e:
        print(f"Redshift copy failed for {formatted_date}: {e}")
        raise


dag = DAG(
    'weather_now_data_to_reshift',
    default_args=default_args,
    description='A simple DAG to process weather data',
    schedule_interval='0 8 * * *',
)

start_date = pendulum.datetime(2024, 7, 25, tz=kst)
end_date = pendulum.now(tz=kst).add(days=1) 

task3 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_s3_to_redshift,
    op_args=[start_date.format('YYYY'), start_date.format('MM'), start_date.format('DD')],
    dag=dag,
)

task3
