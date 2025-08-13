from pendulum import datetime, duration
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='_TESTE',
    start_date=datetime(2025, 8, 12),
    schedule='0 0 * * *',
    default_args=default_args,
    dagrun_timeout=duration(minutes=5),
    catchup=False,
    description='Dag to execute breweries pipeline',
) as dag:
    sourceToBronze = BashOperator(
        task_id='sourceToBronze',
        bash_command='python /opt/airflow/external_scripts/sourceToBronze_breweries.py'
    )

    testCase = BashOperator(
        task_id='testCase',
        bash_command='python /opt/airflow/external_scripts/test_case_brewery.py'
    )

    bronzeToSilver = BashOperator(
        task_id='bronzeToSilver',
        bash_command='docker exec -it PySpark /home/jovyan/work/scripts/bronzeToSilver_breweries.ipynb'
    )

    sourceToBronze >> testCase >> bronzeToSilver    

