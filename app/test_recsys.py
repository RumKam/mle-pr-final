import requests
import time
import random

url_init = "http://172.20.0.1:8000/recommendations"

users_list = [1001, 895999, 1002, 1003, 173863,
              198270, 79627, 994820, 198270,
              1109474]

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
            print(f"User ID: {i}, Prediction: {prediction}")
        else:
            print(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")