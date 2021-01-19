from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator

ARGS = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='docker_test',
    default_args=ARGS,
    schedule_interval=None,
    tags=['docker_remotely']
)

dummy = DummyOperator(
    task_id="dummy",
    dag=dag
)

docker = DockerOperator(
    task_id='docker_testing',
    docker_url='tcp://172.16.16.88:2375',
    container_name="test_airflow",
    image='hello-world:latest',
    api_version='auto',
    dag=dag
)

dummy >> docker


