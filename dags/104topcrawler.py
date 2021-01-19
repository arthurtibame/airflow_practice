from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import  SSHOperator 
from airflow.models import DAG, Variable
from datetime import timedelta

ARGS = {
    'owner': 'Airflow', # DAG 擁有者的名稱
    'start_date': days_ago(2021,1,18), # Task 從哪個日期後開始可以被 Scheduler 排入排程
    'email': ["austin.dn.wu@modovision.com"],
    'email_on_failure': False, # 如果Task執行失敗，是否要寄信通知
    'email_on_retry': False, # 如果Task 重試失敗，是否要寄信通知
    'retries': 1, # 最多重試幾次
    'retry_delay': timedelta(minutes=5) # 每次重試時間間隔
}
dag = DAG(
    dag_id='104topcrawler', # dag_id 為唯一
    default_args=ARGS,
    tags=['104top'],
    schedule_interval= "00 08 * * *" # 設定該DAG多久執行一次
)

dump_image_from_104top = SSHOperator(
    task_id="104top_01",
    ssh_conn_id="104topcrawler",
    command="docker -H tcp://172.16.16.119 run --rm dingnandocker/top104crawler",
    dag=dag
)

dump_image_from_104top
