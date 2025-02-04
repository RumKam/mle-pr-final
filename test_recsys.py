import requests
import time
import random
import logging

# Настройка логирования
logging.basicConfig(filename='../test_service.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

url_init = "http://172.20.0.1:8000/recommendations"

users_list = [1001, 1002, 1003,         # Пользователи без истории
              173863, 198270, 994820]   # Пользователи с историей

logging.info(f"Сервис работает")

for i in users_list:
    # Получим 5 рекомендаций товаров
    params = {"user_id": i, "k": 5}

    url = f"{url_init}?user_id={params['user_id']}&k={params['k']}"

    try:
        response = requests.post(url)
        # Сделаем паузы между запросами
        time.sleep(random.uniform(0.01, 2.0))

        # Проверка успешности запроса
        if response.status_code == 200:
            prediction = response.json()
            logging.info(f"User ID: {i}, Prediction: {prediction}")
        else:
            logging.info(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.info(f"An error occurred: {e}")