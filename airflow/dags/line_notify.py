import json
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

msg = "testing"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('line_notify_example', default_args=default_args, tags=['example'], start_date=days_ago(2))

task_post_op = SimpleHttpOperator(
    http_conn_id="line_notify",
    task_id='post_op',    
    method="POST",
    data={"message": msg},
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer UUlnxY9CWEH9XTnx5jRXuXkk51dTgUNjIGLxA7GeTpg"
        },
    response_check=lambda response: response.json()['status'] == 200,
    dag=dag,
)
task_post_op