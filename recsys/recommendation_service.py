import logging as logger
import pandas as pd
import logging
import requests
from fastapi import FastAPI
from contextlib import asynccontextmanager
from config import config


# Настройка логирования
logging.basicConfig(filename='../test_service.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

logger = logging.getLogger("uvicorn.error")

class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """

        logging.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logging.info(f"Loaded")

    def get(self, user_id: int, k: int=10):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            logging.warning(f"No personal recommendations found for user_id: {user_id}. Using default recommendations.")
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception as e:
            logging.error(f"Error while fetching recommendations: {str(e)}")
            recs = []

        return recs

    def stats(self):

        logging.info("Stats for recommendations")
        for name, value in self._stats.items():
            logging.info(f"{name:<30} {value} ")

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

rec_store = Recommendations()

rec_store.load(
    "personal",
    config['PERSONAL_RECS_PATH'],
    columns=["user_id", "item_id", "rank"],
)
rec_store.load(
    "default",
    config['DEFAULT_PATH'],
    columns=["item_id", "rank"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logging.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logging.info("Stopping")
    logging.rec_store.stats()
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)

    return {"recs": recs} 

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 10):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем последнее событие пользователя
    params = {"user_id": user_id, "k": k}
    resp = requests.post(config['EVENTS_STORE_URL'] + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"][:k]

    items = []
    scores = []
    for item_id in events:

        params = {"item_id": item_id, "k": k}
        # для каждого item_id получаем список похожих в item_similar_items
        similar_items_resp = requests.post(config['FEATURE_STORE_URL'] + "/similar_items", headers=headers, params=params)
        item_similar_items = similar_items_resp.json()

        items += item_similar_items["item_id_sim"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    # для старта это приемлемый подход
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)
    
    # Отберем необходимое количество рекомендаций
    return {"recs": recs[:k]} 

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        if i % 2 == 0:
            recs_blended.append(recs_offline[i])
        else:
            recs_blended.append(recs_online[i])

    # добавляем оставшиеся элементы в конец
    recs_blended.extend(recs_offline[min_length:])
    recs_blended.extend(recs_online[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]


    return {"recs": recs_blended}