import datetime as dt
import os
import sys
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = '/opt/airflow'
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.main_hits import main_hits
from modules.main_sessions import main_sessions
from modules.load_hits import insert_hits
from modules.load_sessions import insert_sessions

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 12, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='sber_autopodpiska',
        schedule=None,
        default_args=args,
        dagrun_timeout=timedelta(minutes=60 * 4)
) as dag:
    main_hits = PythonOperator(
        task_id='main_hits',
        python_callable=main_hits,
        execution_timeout=timedelta(minutes=20),
        dag=dag,
    )
    main_sessions = PythonOperator(
        task_id='main_sessions',
        python_callable=main_sessions,
        execution_timeout=timedelta(minutes=20),
        dag=dag,
    )
    insert_sessions = PythonOperator(
        task_id='load_sessions',
        python_callable=insert_sessions,
        dag=dag,
    )
    insert_hits = PythonOperator(
        task_id='load_hits',
        python_callable=insert_hits,
        dag=dag,
    )

    main_hits >> main_sessions >> insert_sessions >> insert_hits
    # <YOUR_CODE>

