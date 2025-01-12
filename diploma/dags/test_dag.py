from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def check_tables():
    conn_params = {
        "dbname": "data",
        "user": "data",
        "password": "data",
        "host": "postgres_data",
        "port": "5433"
    }

    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
            """)
            tables = cur.fetchall()
            print("Список таблиц:")
            for table in tables:
                print(f"- {table[0]}")

            for table in tables:
                print(f"\nСодержимое таблицы {table[0]} (до 5 строк):")
                cur.execute(f"SELECT * FROM {table[0]} LIMIT 5;")
                rows = cur.fetchall()
                for row in rows:
                    print(row)
    finally:
        conn.close()


with DAG(
        "check_postgres_tables_without_hook",
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
) as dag:
    check_tables_task = PythonOperator(
        task_id="check_tables",
        python_callable=check_tables,
    )
