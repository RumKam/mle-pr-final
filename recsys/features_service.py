import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI
from config import config

logger = logging.getLogger("uvicorn.error")

class SimilarItems:

    def __init__(self):

        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """
        
        logger.info(f"Loading data, type: {type}")
        try:
            self._similar_items = pd.read_parquet(config["SIMILAR_RECS_PATH"])
            self._similar_items.set_index("item_id_init", inplace=True)
            logger.info(f"Loaded")
        except KeyError:
            logger.error("Loading failed")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["item_id_sim", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("No recommendations found")
            i2i = {"item_id_sim": [], "score": {}}

        return i2i

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_items_store.load(
        config['SIMILAR_RECS_PATH'],
        columns=["item_id_init", "item_id_sim", "score"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 3):
    """
    Возвращает список похожих объектов длиной k для item_id
    """

    i2i = sim_items_store.get(item_id, k)

    return i2i