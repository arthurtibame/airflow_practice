from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

args = {
    'owner': 'arthur.sl.lin',
}
conn_id='172.16.16.123'

dag = DAG(
    dag_id='foodpanda',
    default_args=args,
    start_date = datetime(2020,12,24),
    tags=['foodpanda', 'crawler', 'ETL'],
    
)
sshHook = SSHHook(
            ssh_conn_id=conn_id, 
            remote_host="172.16.16.119",
            username="ub",
            password="2020aiot",
            port=22,
            timeout=60
        )

start = DummyOperator(task_id='start')


t1 = SSHOperator(
        task_id="t1",
        ssh_hook=sshHook,
        command="python3 test.py 2>&1 | tee -a  test.txt",
        dag=dag
                )
t2 = SSHOperator(
        task_id="t2",
        ssh_hook=sshHook,
        command="docker  ps -a 2>&1 | tee -a  test.txt",
        dag=dag
                )


end = DummyOperator(task_id='end')


start >> t1 >> end


