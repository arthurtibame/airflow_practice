from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
from airflow import DAG

# def on_failure_callback(context):
#     dag_run = context.get('dag_run')
#     task_instances = dag_run.get_task_instances()
#     SlackWebhookOperator(
#     http_conn_id="slack",
#     task_id="slack_test"    ,
#     webhook_token="T01D1RB1EMQ/B01HP4PJEUB/yQmkY9NupI7cbG877f5zxVn7",
#     message=str(task_instances),
#     channel="#airflow",
#     username="Airflow",
#     icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",
#     dag=dag
#     ).execute(context)

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
    task_id='slack',
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
}          
    
dag = DAG(
    dag_id='test_slack',
    default_args=args,
    schedule_interval="0 1 * * *",
    start_date = datetime(2020,12,28),
    tags=['slack', 'test'],   
)

# t1 = SlackWebhookOperator(
#     http_conn_id="slack",
#     task_id="slack_test"    ,
#     webhook_token="T01D1RB1EMQ/B01HP4PJEUB/yQmkY9NupI7cbG877f5zxVn7",
#     message="failed",
#     channel="#airflow",
#     username="Airflow",
#     icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",
#     dag=dag
# )

msg="successful"

t1 = SlackWebhookOperator(
    task_id='slack',
    http_conn_id="slack",
    webhook_token="aT01D1RB1EMQ/B01JAGMQ6TS/etZ0jlEcAO6LyyRIBSlrAn4F",
    channel="#airflow",
    username="Airflow",
    message=msg,
    icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",
    dag=dag
)

t1
