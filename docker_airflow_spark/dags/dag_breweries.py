from datetime import datetime
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Função Python a ser executada
def my_python_function():
    print("Executando o script Python na DAG!")
    # Adicione aqui a lógica do seu script
    return "Script executado com sucesso!"

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Definição da DAG
with DAG(
    'breweries',
    default_args=default_args,
    description='Dag to execute breweries pipeline',
    schedule="0 0 * * *",
    start_date=datetime(2025, 8, 11),
    catchup=False,
) as dag:

    list_files_task = BashOperator(
        task_id='list_all_files',
        bash_command='pwd'
        # bash_command='ls -R ./',  # -R for recursive listing
    )

list_files_task

#     executar_script = BashOperator(
#         task_id="rodar_teste_py",
#         bash_command="python ../scripts/sourceToBronze_breweries.py"
#     )

# executar_script