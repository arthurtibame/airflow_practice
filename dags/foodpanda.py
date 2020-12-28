from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def alert_slack_channel(context):   
    execution_date = context.get('execution_date')
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"
    error_message = context.get('exception') or context.get('reason')

    title = f':red_circle: {task_name} has failed!'
    msg_parts = {
        'Execution date': execution_date,
        'Log': log_link,
        'Error': error_message
    }
    msg = "\n".join([title,
        *[f"*{key}*: {value}" for key, value in msg_parts.items()]
    ]).strip()
    SlackWebhookOperator(
    task_id='foodpanda',
    http_conn_id="slack",
    webhook_token="T01D1RB1EMQ/B01JAGMQ6TS/etZ0jlEcAO6LyyRIBSlrAn4F",
    channel="#airflow",
    username="Airflow",
    message=msg,
    icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",    
    ).execute(context=context)

def success_slack_channel(context):   
    execution_date = context.get('execution_date')
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    log_link = f"<{last_task.log_url}|{task_name}>"   

    title = f':green_circle: {task_name} has finished!'
    msg_parts = {
        'Execution date': execution_date,
        'Log': log_link,        
    }
    msg = "\n".join([title,
        *[f"*{key}*: {value}" for key, value in msg_parts.items()]
    ]).strip()
    SlackWebhookOperator(
    task_id='foodpanda',
    http_conn_id="slack",
    webhook_token="T01D1RB1EMQ/B01JAGMQ6TS/etZ0jlEcAO6LyyRIBSlrAn4F",
    channel="#airflow",
    username="Airflow",
    message=msg,
    icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",    
    ).execute(context=context)


args = {
    'owner': 'arthur.sl.lin',
    'on_failure_callback': alert_slack_channel,    
    'on_success_callback': success_slack_channel
}          

dag = DAG(
    dag_id='foodpanda_crawler',
    default_args=args,
    schedule_interval="0 1 * * *",
    start_date = datetime(2020,12,25),
    tags=['foodpanda', 'crawler', 'ETL'],
    
)

start = DummyOperator(task_id='start')

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


