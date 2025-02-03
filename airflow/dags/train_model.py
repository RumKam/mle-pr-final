# dags/real_estate.py
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.train_model import (extract,
                               transform,
                               feature_engineering,
                               split_dataframes,
                               train_inference_model) # импортируем фукнции с логикой шагов
from steps.messages import send_telegram_success_message, send_telegram_failure_message

with DAG(
    dag_id='recsys_model',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:
    
    # инициализируем задачи DAG, указывая параметр python_callable
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    feature_engineering_step = PythonOperator(task_id='feature_engineering', python_callable=feature_engineering)
    split_dataframes_step = PythonOperator(task_id='split_dataframes', python_callable=split_dataframes)
    train_inference_model_step = PythonOperator(task_id='train_inference_model', python_callable=train_inference_model)

    extract_step >> transform_step >> feature_engineering_step >> split_dataframes_step >> train_inference_model_step