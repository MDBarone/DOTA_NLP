from airflow import DAG
from airflow.operators.python import PythonOperator

# Hacky way to parent folder (https://stackoverflow.com/questions/30669474/beyond-top-level-package-error-in-relative-import)
import sys
sys.path.append('../..')
from scrapers.getPlayerData import getData, respond

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 9, 24, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}


with DAG('my_simple_dag',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         ) as dag:

    opr_greet = PythonOperator(task_id='getPlayerData',
                               python_callable=getData)
    # opr_sleep = BashOperator(task_id='sleep_me', bash_command='sleep 5')
    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)
opr_greet >> opr_respond
