from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

DAG_ID="foodpanda_crawler"



def alert_slack_channel(context):   
    execution_date = context.get('execution_date')
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"
    error_message = context.get('exception') or context.get('reason')

    title = f'❌: {task_name} has failed!'
    msg_parts = {
        'Execution date': execution_date,
        'Log': log_link,
        'Error': error_message
    }
    msg = "\n".join([title,
        *[f"*{key}*: {value}" for key, value in msg_parts.items()]
    ]).strip()

    SimpleHttpOperator(
    http_conn_id="line_notify",
    task_id='post_op',    
    method="POST",
    data={"message": msg},
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer UUlnxY9CWEH9XTnx5jRXuXkk51dTgUNjIGLxA7GeTpg"
        },
    response_check=lambda response: response.json()['status'] == 200,    
    ).execute(context=context)

    # SlackWebhookOperator(
    # task_id='foodpanda',
    # http_conn_id="slack",
    # webhook_token="T01D1RB1EMQ/B01HP4PJEUB/7VjWpPCdSNsj57iQmIMdF8Gd",
    # channel="#airflow",
    # username="Airflow",
    # message=msg,
    # icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",    
    # ).execute(context=context)

def success_slack_channel(context):   
    execution_date = context.get('execution_date')
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"   

    title = f'✔: {task_name} has finished!'
    msg_parts = {
        'Execution date': execution_date,
        'Log': log_link,        
    }
    msg = "\n".join([title,
        *[f"*{key}*: {value}" for key, value in msg_parts.items()]
    ]).strip()
    # SlackWebhookOperator(
    # task_id='foodpanda',
    # http_conn_id="slack",
    # webhook_token="T01D1RB1EMQ/B01HP4PJEUB/7VjWpPCdSNsj57iQmIMdF8Gd",
    # channel="#airflow",
    # username="Airflow",
    # message=msg,
    # icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",    
    # ).execute(context=context)
    SimpleHttpOperator(
    http_conn_id="line_notify",
    task_id='post_op',    
    method="POST",
    data={"message": msg},
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer UUlnxY9CWEH9XTnx5jRXuXkk51dTgUNjIGLxA7GeTpg"
        },
    response_check=lambda response: response.json()['status'] == 200,    
    ).execute(context=context)


args = {
    'owner': 'arthur.sl.lin',
    'on_failure_callback': alert_slack_channel,    
    'on_success_callback': success_slack_channel,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}          

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 1 * * *",
    start_date = datetime(2020,12,28),
    tags=['foodpanda', 'crawler', 'ETL'],
    
)

start =  SimpleHttpOperator(
    http_conn_id="line_notify",
    task_id='post_op',    
    method="POST",
    data={"message": f"‼ starting {DAG_ID}"},
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer UUlnxY9CWEH9XTnx5jRXuXkk51dTgUNjIGLxA7GeTpg"
        },
    response_check=lambda response: response.json()['status'] == 200,    
    dag=dag
    )

t1 = DockerOperator(
    task_id='foodpanda_crawler',    
    docker_url='tcp://172.16.16.119:2375',  # Set your docker URL    
    image='arthurtibame/foodpanda_scraper:v5',
    container_name=f"foodpanda_crawler{datetime.now().strftime('%Y-%m-%d')}",
    command="crawl foodpanda",
    network_mode='bridge',    
    auto_remove=True,
    dag=dag
)

start >> t1



