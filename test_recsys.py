import requests
import logging
from recsys.config import config

# Настройка логирования
logging.basicConfig(filename='test_service.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

def fetch_recommendations(user_id):
    params = {"user_id": user_id, 'k': 10}
    
    # Запрос оффлайн рекомендаций
    try:
        resp_offline = requests.post(config['RECS_URL'] + "/recommendations_offline", headers=headers, params=params)
        resp_offline.raise_for_status()
        recs_offline = resp_offline.json()["recs"]
        logging.info(f'Fetched offline recommendations for user {user_id}. Success.')
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching offline recommendations for user {user_id}: {e}')
        recs_offline = []

    # Запрос онлайн рекомендаций
    try:
        resp_online = requests.post(config['RECS_URL'] + "/recommendations_online", headers=headers, params=params)
        resp_online.raise_for_status()
        recs_online = resp_online.json()["recs"]
        logging.info(f'Fetched online recommendations for user {user_id}. Success.')
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching online recommendations for user {user_id}: {e}')
        recs_online = []

    # Запрос смешанных рекомендаций
    try:
        resp_blended = requests.post(config['RECS_URL'] + "/recommendations", headers=headers, params=params)
        resp_blended.raise_for_status()
        recs_blended = resp_blended.json()["recs"]
        logging.info(f'Fetched blended recommendations for user {user_id}: Success.')
    except requests.exceptions.RequestException as e:
        logging.error(f'Error fetching blended recommendations for user {user_id}: {e}')
        recs_blended = []

    return recs_offline, recs_online, recs_blended

# Сценарии тестирования
test_users = {
    "user_no_recommendations": 1,  # Пользователь без персональных рекомендаций
    "user_no_online_history": 1052774,   # Пользователь с персональными рекомендациями, но без онлайн-истории
    "user_with_online_history": 473202   # Пользователь с персональными рекомендациями и онлайн-историей
}

# Зададим историю последних действий для пользователя
user_id = 473202
event_item_ids = [293274, 198539, 389088]

# Отправка событий в хранилище признаков
for event_item_id in event_item_ids:
    try:
        resp = requests.post(config['EVENTS_STORE_URL'] + "/put", 
                             headers=headers, 
                             params={"user_id": user_id, "item_id": event_item_id})
        # Проверка на HTTP ошибки
        resp.raise_for_status()  
        logging.info(f'Successfully logged event {event_item_id} for user {user_id}.')
    except requests.exceptions.RequestException as e:
        logging.error(f'Error logging event {event_item_id} for user {user_id}: {e}')

# Тестирование сервиса

# Пользователь без персональных рекомендаций 
user_id = test_users["user_no_recommendations"]
logging.info(f'Testing for user_no_recommendations, user_id: {user_id}')
recs_offline, recs_online, recs_blended = fetch_recommendations(user_id)
logging.info(f'Offline Recommendations for user {user_id}: {recs_offline}.')
print(f"Offline Recommendations: {recs_offline}")

# Пользователь с персональными рекомендациями, но без онлайн-истории
user_id = test_users["user_no_online_history"]
logging.info(f'Testing for user_no_online_history, user_id: {user_id}')
recs_offline, recs_online, recs_blended = fetch_recommendations(user_id)
logging.info(f'Blended Recommendations for user {user_id}: {recs_blended}.')
print(f"Blended Recommendations: {recs_blended}")

# Пользователь с персональными рекомендациями и онлайн-историей (онлайн рекомендации)
user_id = test_users["user_with_online_history"]
logging.info(f'Testing for user_with_online_history, user_id: {user_id}')
recs_offline, recs_online, recs_blended = fetch_recommendations(user_id)
logging.info(f'Online Recommendations for user {user_id}: {recs_online}.')
print(f"Online Recommendations: {recs_online}")

# Пользователь с персональными рекомендациями и онлайн-историей (смешанные рекомендации)
user_id = test_users["user_with_online_history"]
logging.info(f'Testing for user_with_online_history, user_id: {user_id}')
recs_offline, recs_online, recs_blended = fetch_recommendations(user_id)
logging.info(f'Blended Recommendations for user {user_id}: {recs_blended}.')
print(f"Blended Recommendations: {recs_blended}")