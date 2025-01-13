# Проект: Рекомендация товаров в электронной коммерции

**Цель:** Предсказать, какие товары предложить пользователю интернет-магазина.

**Основные задачи:** Анализ событий и характеристик товаров, выбор метрик, построение модели рекомендаций, внедрение модели в виде веб-сервиса, мониторинг и обновление модели.

**Данные:** [Ссылка на датасет](https://disk.yandex.ru/d/XPthmNk_pqEDaQ)

**Описание данных**

1. category_tree.csv — таблица из двух столбцов: «родительская категория» и «дочерняя категория». 
2. events.csv — таблица с логом событий:
    - timestamp — временная метка события,
    - visitorid — идентификатор пользователя,
    - event — событие (просмотр, добавление в корзину, покупка),
    - itemid — идентификатор товара,
    - transactionid — идентификатор транзакции (покупки).
3. item_properties.csv — таблица со свойствами товаров:
    - timestamp — временная метка добавления свойства,
    - itemid — идентификатор товара,
    - property — свойство товара,
    - value — значение свойства.

## Руководство к проекту

1. Трансляция бизнес-задачи в техническую задачу. Опишите, как вы понимаете поставленную задачу, на какие метрики качества ориентируетесь.
2. Разворачивание инфраструктуры обучения модели. Кратко опишите, как вы развёртывали MLflow и остальную инфраструктуру, чтобы обученить модели. В идеале весь запуск должен быть упакован в shell-скрипт.
3. Проведение EDA. Опишите ключевые шаги вашего EDA. Выделите основные выводы, которые помогут понять данные.
4. Генерация признаков и обучение модели. Укажите, какие признаки сгенерировались и как это повлияло на модель. Опишите, что можно увидеть в запусках в MLflow.
5. Разворачивание инфраструктуры применения модели. Кратко опишите процесс запуска веб-сервиса и дополнительной инфраструктуры: баз данных, систем мониторинга, Airflow-графы обновления моделей и т. д. В идеале весь запуск должен быть упакован в shell-скрипт.

## Подготовка виртуальной машины

1. Склонируйте репозиторий проекта:

```
git clone [<ссылка-репозиторий>](https://github.com/RumKam/mle-pr-final.git)
```

2. Создать новое виртуальное окружение можно командами:

```
# обновление локального индекса пакетов
sudo apt-get update
# установка расширения для виртуального пространства
sudo apt-get install python3.10-venv
# создание виртуального пространства
python3.10 -m venv .venv_pr_final
```

3. После его инициализации следующей командой

```
source .venv_pr_final/bin/activate
```

4. Установите в него необходимые Python-пакеты следующей командой

```
pip install -r requirements.txt
```