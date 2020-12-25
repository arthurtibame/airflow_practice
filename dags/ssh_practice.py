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
    dag_id='foodpanda',
    default_args=args,
    schedule_interval="0 1 * * *",
    start_date = datetime(2020,12,25),
    tags=['foodpanda', 'crawler', 'ETL'],
    
)

start = DummyOperator(task_id='start')


t1 = SSHOperator(
        task_id="t1",
        ssh_conn_id="crawler_server",        
        command='echo "airflow"',                #"{{ti.xcom_push(key="t1", value=) }}"                
        get_pty=True,
        dag=dag
                )
t1.do_xcom_push
# t1_1 = SSHOperator(
#         task_id="t1_1",
#         ssh_conn_id="crawler_server",        
#         command="python3 -c ",
#         dag=dag
#                 )

# task = BashOperator(
#     task_id='also_run_this',
#     bash_command='docker -H tcp://172.16.16.119  images',
#     run_as_user='ub',
#     dag=dag)

# task.do_xcom_push

t2 = DockerOperator(
    task_id='foodpanda_crawler',    
    docker_url='tcp://172.16.16.119:2375',  # Set your docker URL    
    image='arthurtibame/foodpanda_scraper:v2',
    container_name="foodpanda_crawler",
    command="crawl foodpanda",
    network_mode='bridge',    
    auto_remove=True,
    dag=dag
)

start >> t1 >> t2


