from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
import os

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными

    hook = TelegramHook(telegram_conn_id='ml_test',
                        token=os.getenv("TOKEN"),
                        chat_id=os.getenv("CHAT_ID"))
    
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag.dag_id} с id={run_id} прошло успешно!' # определение текста сообщения 
    hook.send_message({
        'chat_id': os.getenv("CHAT_ID"),
        'text': message
    }) # отправление сообщения

def send_telegram_failure_message(context):
    
    hook = TelegramHook(telegram_conn_id='ml_test',
                        token=os.getenv("TOKEN"),
                        chat_id=os.getenv("CHAT_ID"))
    
    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag.dag_id} с id={run_id} провалилось! {task_instance_key_str}' # определение текста сообщения
    hook.send_message({
        'chat_id': os.getenv("CHAT_ID"),
        'text': message
    }) # отправление сообщения 