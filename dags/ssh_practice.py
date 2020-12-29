from datetime import datetime
from airflow import DAG
from airflow.models import xcom
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.docker.operators.docker import DockerOperator

args = {
    'owner': 'arthur.sl.lin',
}

dag = DAG(
    dag_id='ssh_example',
    default_args=args,
    schedule_interval="* * * * *",
    start_date = datetime(2020,12,25),
    tags=['ssh', 'example'],
    
)

start = DummyOperator(task_id='start')


t1 = SSHOperator(
        task_id="t1",
        ssh_conn_id="crawler_server",        
        command='echo "airflow"',                #"{{ti.xcom_push(key="t1", value=) }}"                
        get_pty=True,
        dag=dag
                )
start >> t1