from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mercado_libre_extraction',
    default_args=default_args,
    description='Data pipeline to extract from the Mercado Libre API and load into PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime.strptime("2021-09-10", '%Y-%m-%d'),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='api_to_db',
        bash_command='python /opt/airflow/dags/api_to_db.py'
    )

    t2 = BashOperator(
        task_id='filtered_items_to_csv.py',
        bash_command='python /opt/airflow/dags/filtered_items_to_csv.py'
    )

    t3 = EmailOperator(
        task_id='send_email',
        to=Variable.get("to_email"),
        subject='$7M+ Mercado Libre Item Sales',
        html_content='$7M+ Mercado Libre Item Sales',
        files=['/opt/airflow/dags/items.csv']
    )
    t1 >> t2 >> t3