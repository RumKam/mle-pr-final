# dags/real_estate.py
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.real_estate import create_table, extract, transform, clean, load # импортируем фукнции с логикой шагов
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='real_estate',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:
    
    # инициализируем задачи DAG, указывая параметр python_callable
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    # CELAN
    clean_step = PythonOperator(task_id='clean', python_callable=clean)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> transform_step >> clean_step >> load_step