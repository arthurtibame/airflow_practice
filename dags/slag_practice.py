from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
from airflow import DAG

args = {
    'owner': 'arthur.sl.lin',
}

dag = DAG(
    dag_id='test_slack',
    default_args=args,
    schedule_interval="0 1 * * *",
    start_date = datetime(2020,12,28),
    tags=['slack', 'test'],
    
)

t1 = SlackWebhookOperator(
    http_conn_id="slack",
    task_id="slack_test"    ,
    webhook_token="T01D1RB1EMQ/B01HP4PJEUB/yQmkY9NupI7cbG877f5zxVn7",
    message="test",
    channel="#airflow",
    username="Airflow",
    icon_url="https://www.element61.be/sites/default/files/assets/insights/airflow-2-0/airflow.png",
    dag=dag
)

t1
