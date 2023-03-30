import csv
import json
from datetime import datetime
import requests
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
# from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
import boto3
from airflow.models import Variable
from airflow.operators.python import PythonOperator

"""def print_message():
    print("Tasks running now")"""

georgia_cities = ['Atlanta', 'Savannah', 'Athens', 'Columbus', 'Macon']
california_cities = ['Los Angeles', 'San Francisco', 'San Diego', 'Sacramento', 'Fresno']

"""def save_posts(ti) -> None:
    posts = ti.xcom_pull(task_id=['get_posts'])
    with open('/Users/sunanda/Desktop/assignment/post.json', 'w') as f:
        json.dump(posts[0], f)"""


def get_cities(state, cities):
    data = []
    api_key = '94ca56afa3eee20f7100985fdfe682aa'
    for city in cities:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city},{state},US&appid={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            data.append(response.json())

    filename = f"{state}_cities_data.csv"

    with open(filename, 'w') as csv_file:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        for row in data:
            writer.writerow(row)

        print(f"data saved to {filename}")


with DAG(
        dag_id='api_dag',
        schedule_interval='*/10 * * * *',
        start_date=datetime(2023, 3, 19),
        catchup=False
) as dag:
    api_sensor = HttpSensor(
        task_id='is_api_active',
        http_conn_id='my_api_connection',
        endpoint='weather?q=Atlanta,Georgia,US&units=standard&appid=94ca56afa3eee20f7100985fdfe682aa',
        poke_interval=5,
        timeout=20,
        dag=dag
    )

    task_get_post = SimpleHttpOperator(

        task_id="get_posts",
        http_conn_id="my_api_connection",
        endpoint="weather?q=Atlanta,Georgia,US&units=standard&appid=94ca56afa3eee20f7100985fdfe682aa",
        method="GET",
        headers={'Content-Type': 'application/json'},
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    ga_operator = PythonOperator(
        task_id='ga_operator',

        python_callable=get_cities,
        op_kwargs={'state': 'georgia', 'cities': georgia_cities}

    )

    ca_operator = PythonOperator(
        task_id='ca_operator',
        python_callable=get_cities,
        op_kwargs={'state': 'california', 'cities': california_cities}
    )

    wait_for_file1 = FileSensor(
        task_id='wait_for_file',
        filepath='/Users/sunanda/Downloads/airflow/california_cities_data.csv',
        poke_interval=10,
        dag=dag,
    )

    wait_for_file2 = FileSensor(
        task_id='wait_for_file1',
        filepath='/Users/sunanda/Downloads/airflow/georgia_cities_data.csv',
        poke_interval=10,
        dag=dag,
    )

    move_to_hdfs = BashOperator(
        task_id='move_to_hdfs',
        bash_command='/Users/sunanda/Downloads/hadoop-3.3.4/bin/hdfs dfs -put ~/Downloads/airflow/california_cities_data.csv /user/api',
        dag=dag
    )

    move_to_hdfs1 = BashOperator(
        task_id='move_to_hdfs1',
        bash_command='/Users/sunanda/Downloads/hadoop-3.3.4/bin/hdfs dfs -put ~/Downloads/airflow/georgia_cities_data.csv /user/api/',
        dag=dag

    )

    # python_operator = PythonOperator(task_id="print message", python_callable=print_message, dag=dag)

    spark_submit_local = SparkSubmitOperator(
        application='/Users/sunanda/airflow/dags/spark_api.py',
        task_id='spark_submit_task',
        conn_id="spark_local",
        dag=dag
    )

    # hive operator

    hql_weather = ("""
    CREATE DATABASE IF NOT EXISTS weather_api;
    USE weather_api;

    CREATE TABLE IF NOT EXISTS weather_data (
        state STRING,
        avg_temp_celsius FLOAT,
        stddev_temp_celsius FLOAT
    );
    
    LOAD DATA INPATH '/user/api_output/california' INTO TABLE weather_data;
    LOAD DATA INPATH '/user/api_output/georgia' INTO TABLE weather_data;
""")

hive_task = HiveOperator(
    hql=hql_weather,
    task_id="create_table_task",
    hive_cli_conn_id="airflow_hive",
    dag=dag
)


# hive_create_table >> hive_load_data


# sns operator


def send_email_notification():
    # Define the email content
    from_address = 'sunandapoudel93@gmail.com'
    to_address = 'humbertoaa2011@gmail.com'
    subject = 'Job Status Update'
    body = 'The job has completed successfully.'

    # Get the AWS credentials from Airflow Variables
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')

    # Create the SES client
    ses_client = boto3.client('ses',
                              region_name='us-east-1',
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

    # Send the email message
    response = ses_client.send_email(
        Source=from_address,
        Destination={
            'ToAddresses': [
                to_address,
            ],
        },
        Message={
            'Subject': {
                'Data': subject,
            },
            'Body': {
                'Text': {
                    'Data': body,
                },
            },
        },
    )

    # test
    print(response)


# Create the PythonOperator to call the send_email_notification function
send_email_op = PythonOperator(task_id='send_email_notification',
                               python_callable=send_email_notification,
                               dag=dag)

"""task_save = PythonOperator(
    task_id="save_posts",
    python_callable=save_posts
    )"""

api_sensor >> task_get_post >> ga_operator >> ca_operator >> wait_for_file1 >> wait_for_file2 >> move_to_hdfs >> move_to_hdfs1 >> spark_submit_local >> hive_task >> send_email_op

if __name__ == "__main__":
    dag.cli()
