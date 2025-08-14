from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)

def etl_breweries():

    sourceToBronze = PythonOperator(
        task_id='sourceToBronze',
        retries=1,
        retry_delay=timedelta(minutes=5),
        bash_command='python include/scripts/sourceToBronze_breweries.py'
    )

    testCase = PythonOperator(
        task_id='sourceToBronze',
        retries=1,
        retry_delay=timedelta(minutes=5),
        bash_command='python include/scripts/test_case_brewery.py'
    )

    bronzeToSilver = SparkSubmitOperator(
        task_id="bronzeToSilver",
        retries=1,
        retry_delay=timedelta(minutes=5),
        conn_id="my_spark_conn",
        application="include/scripts/bronzeToSilver_breweries.py",
        verbose=True
    )

    silverToGold = SparkSubmitOperator(
        task_id="silverToGold",
        retries=1,
        retry_delay=timedelta(minutes=5),
        conn_id="my_spark_conn",
        application="include/scripts/silverToGold_aggBrewery.py",
        verbose=True
    )
    
    sourceToBronze >> testCase >> bronzeToSilver >> silverToGold

etl_breweries()