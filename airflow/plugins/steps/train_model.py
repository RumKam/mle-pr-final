import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sklearn.preprocessing
from sklearn.preprocessing import MinMaxScaler
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from catboost import CatBoostClassifier, Pool
import pickle
import os
from io import BytesIO
import io
import pyarrow.parquet as pq

PATH = '/home/mle-user/mle_projects/mle-pr-final/services/data/'
PATH_RECS = '/home/mle-user/mle_projects/mle-pr-final/services/recsys/recommendations/'
PATH_MODEL = '/home/mle-user/mle_projects/mle-pr-final/services/models/'
BACKET_NAME = 's3-student-mle-20240921-750d983cc8'

def extract(**kwargs):

    """ Extract data from database """

    # Данные были заранее загружены в базу данных
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    engine = hook.get_sqlalchemy_engine()
    source_conn = engine.connect()

    QUERIES = {"CATEGORY" : '''
            SELECT *
            FROM category_tree AS category_tree
        ''',

           "EVENTS" : '''
        SELECT timestamp,
        visitorid, 
        event, 
        itemid, 
        COALESCE(transactionid, 0) AS transactionid
        FROM events AS events
        ''',

        "ITEMS": '''
        SELECT *
        FROM items AS items
        '''
          }

    category_tree = pd.read_sql(QUERIES["CATEGORY"], source_conn)
    events = pd.read_sql(QUERIES["EVENTS"], source_conn)
    items = pd.read_sql(QUERIES["ITEMS"], source_conn)

    source_conn.close()

    # Промежуточные файлы имеют много строк, поэтому загрузим в хранилище без создания таблиц в бд
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')

    category_tree.to_parquet('category_tree_tmp.parquet', index=False)
    events.to_parquet('events_tmp.parquet', index=False)
    items.to_parquet('items_tmp.parquet', index=False)

    s3_hook.load_file('category_tree_tmp.parquet', 
                      key=f'{PATH}category_tree_tmp.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)
    
    s3_hook.load_file('events_tmp.parquet', 
                      key=f'{PATH}events_tmp.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)
    
    s3_hook.load_file('items_tmp.parquet', 
                      key=f'{PATH}items_tmp.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)


def transform(**kwargs):
    """
    #### Transform task
    """

    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')

    s3_client = s3_hook.get_conn()

    response_category_tree = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}category_tree_tmp.parquet')
    # Читаем содержимое объекта
    parquet_file_category_tree = BytesIO(response_category_tree['Body'].read())
    # Загружаем Parquet файл в DataFrame
    table_1 = pq.read_table(parquet_file_category_tree)
    category_tree = table_1.to_pandas()

    response_events = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}events_tmp.parquet')
    parquet_file_events = BytesIO(response_events['Body'].read())
    table_2 = pq.read_table(parquet_file_events)
    events = table_2.to_pandas()

    response_items = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}items_tmp.parquet')
    parquet_file_items = BytesIO(response_items['Body'].read())
    table_3 = pq.read_table(parquet_file_items)
    items = table_3.to_pandas()

    # Заполним отсутствие транзакции 0 - покупка не совершена
    events['transactionid'] = events['transactionid'].apply(lambda x: 0 if x==None else x).astype('int')
    # Создадим новый признак - факт совершения покупки
    events['istransaction'] = events['transactionid'].apply(lambda x: 0 if x==0 else 1).astype('int')
    # Добавим в качестве таргета признак - добавления товара в корзину
    events['target'] = events['event'].apply(lambda x: 1 if x=='addtocart' else 0)
    # Преобразуем формат времени
    events['timestamp'] = pd.to_datetime(events['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    # Переименуем столбцы в более привычные для рекомендательных систем
    events = events.rename(columns={'visitorid': 'user_id',
                                'itemid': 'item_id'})
    
    # Преобразуем дату
    items['timestamp'] = pd.to_datetime(items['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    # Остортируем датасет по времени и товарам
    items = items.sort_values(['timestamp', 'itemid']).reset_index(drop=True)
    # Удалим дубликаты, оставив только последнюю по времени строку со свойствами
    items = items.drop_duplicates(['itemid','property','value'], keep='last').reset_index(drop=True)
    # Переименуем столбец
    items = items.rename(columns={'itemid': 'item_id'})

    # Проверим, для всех ли товаров из датасета с взаимодействиями есть свойства товаров
    items_list = items['item_id'].unique()

    # Удалим товары без свойств
    events = events[events['item_id'].isin(items_list)].reset_index(drop=True)
    
    # Вынесем отдельно список с популярными своцствами товаров
    pr = ['available', 'categoryid']
    # Создание списка для хранения новых DataFrame
    dfs = []
    # Проходим по каждому элементу из списка pr
    for prop in pr:
        # Фильтруем DataFrame items по свойству
        filtered_items = items[items['property'] == prop].rename(columns={'value': prop}).reset_index(drop=True)
        filtered_items = filtered_items[['item_id', prop]]
        dfs.append(filtered_items)

    # Объединяем все DataFrame в один, оставим последнее по дате изменение
    items_top_properties = pd.concat(dfs, axis=0).groupby('item_id', as_index=False).last()

    events = events.merge(items_top_properties, on='item_id', how='left')
    # Заполним категорию 0
    events['categoryid'] = events['categoryid'].fillna('0')
    # Преобразуем тип данных
    events['categoryid'] = events['categoryid'].astype('int')
    events = events.merge(category_tree, on='categoryid', how='left')

    events.to_parquet('events_transformed.parquet', index=False)
    items.to_parquet('items_transformed.parquet', index=False)

    s3_hook.load_file('events_transformed.parquet', 
                      key=f'{PATH}events_transformed.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)
    
    s3_hook.load_file('items_transformed.parquet', 
                      key=f'{PATH}items_transformed.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)

def feature_engineering(**kwargs):
    """
    #### Transform task
    """
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    s3_client = s3_hook.get_conn()

    response_events = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}events_transformed.parquet')
    # Читаем содержимое объекта
    parquet_file_events = BytesIO(response_events['Body'].read())
    # Загружаем Parquet файл в DataFrame
    table_1 = pq.read_table(parquet_file_events)
    events = table_1.to_pandas()

    events['timestamp'] =  pd.to_datetime(events['timestamp'])

    # Подсчет всех user_id с учетом просмотров и покупок
    events['rating_count'] = events.groupby('item_id')['user_id'].transform('count')
    # Отмасштабируем признак, чтобы оценки были в одной шкале
    scaler = MinMaxScaler()
    events['rating'] = scaler.fit_transform(events[['rating_count']])
    # Извлекаем день недели и час
    events['day_of_week'] = events['timestamp'].dt.dayofweek  # Получаем день недели
    events['day'] = events['timestamp'].dt.day  # Получаем день в месяце
    events['hour'] = events['timestamp'].dt.hour  # Получаем час

    # Отберем необходимые признаки
    selected_columns = ['timestamp',
                    'user_id',
                    'item_id',
                    'available',
                    'categoryid',
                    'parentid',
                    'istransaction',
                    'day_of_week',
                    'day',
                    'hour',
                    'rating',
                    'target']

    # Создадим итоговый датасет
    events = events[selected_columns]

    events.to_parquet('events_final.parquet', index=False)

    s3_hook.load_file('events_final.parquet', 
                      key=f'{PATH}events_final.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)
    

def split_dataframes(**kwargs):
    """
    #### Split data
    """
    ti = kwargs['ti']

    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    s3_client = s3_hook.get_conn()

    response_events = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}events_final.parquet')
    parquet_file_events = BytesIO(response_events['Body'].read())
    table_1 = pq.read_table(parquet_file_events)
    events = table_1.to_pandas()

    response_items = s3_client.get_object(Bucket=BACKET_NAME, 
                                    Key=f'{PATH}items_transformed.parquet')
    parquet_file_items = BytesIO(response_items['Body'].read())
    table_2 = pq.read_table(parquet_file_items)
    items = table_2.to_pandas()

    events['timestamp'] = pd.to_datetime(events['timestamp'], errors='coerce')
    
    # Зададим точку разбиения
    train_test_global_time_split_date = events["timestamp"].max() - pd.Timedelta(days=30)
    
    # Фильтрация данных на основе условия
    train_test_global_time_split_idx = events["timestamp"] < train_test_global_time_split_date
    events_train = events[train_test_global_time_split_idx]
    events_test = events[~train_test_global_time_split_idx]

    # Перекодируем идентификаторы пользователей в последовательность с 0
    user_encoder = sklearn.preprocessing.LabelEncoder()
    user_encoder.fit(events["user_id"])

    events["user_id_enc"] = user_encoder.transform(events["user_id"])
    events_train["user_id_enc"] = user_encoder.transform(events_train["user_id"])
    events_test["user_id_enc"] = user_encoder.transform(events_test["user_id"])

    # Перекодируем идентификаторы объектов в последовательность с 0
    item_encoder = sklearn.preprocessing.LabelEncoder()
    item_encoder.fit(items["item_id"])

    items["item_id_enc"] = item_encoder.transform(items["item_id"])
    events_train["item_id_enc"] = item_encoder.transform(events_train["item_id"])
    events_test["item_id_enc"] = item_encoder.transform(events_test["item_id"])

    # Перекодируем идентификаторы категории в последовательность с 0
    category_encoder = sklearn.preprocessing.LabelEncoder()
    category_encoder.fit(events["categoryid"])

    events["categoryid_enc"] = category_encoder.transform(events["categoryid"])
    events_train["categoryid_enc"] = category_encoder.transform(events_train["categoryid"])
    events_test["categoryid_enc"] = category_encoder.transform(events_test["categoryid"])

    # Перекодируем идентификаторы категории в последовательность с 0
    parent_encoder = sklearn.preprocessing.LabelEncoder()
    parent_encoder.fit(events["parentid"])

    events["parentid_enc"] = parent_encoder.transform(events["parentid"])
    events_train["parentid_enc"] = parent_encoder.transform(events_train["parentid"])
    events_test["parentid_enc"] = parent_encoder.transform(events_test["parentid"])

    # Вычтем из последней даты в датасете несколько дней
    test_days = events_test["timestamp"].max() - pd.Timedelta(days=3)

    # Задаём точку разбиения
    split_date_for_labels = test_days

    # Разделим данные
    split_date_for_labels_idx = events_test["timestamp"] < split_date_for_labels
    events_labels = events_test[split_date_for_labels_idx].copy()
    events_test_2 = events_test[~split_date_for_labels_idx].copy()

    # В кандидатах оставляем только тех пользователей, у которых есть хотя бы один положительный таргет
    candidates_to_sample = events_labels.groupby("user_id").filter(lambda x: x["target"].sum() > 0)

    # для каждого пользователя оставляем 1 негативный пример
    negatives_per_user = 1
    candidates_for_train = pd.concat([
        candidates_to_sample.query("target == 1"),
        candidates_to_sample.query("target == 0") \
            .groupby("user_id") \
            .apply(lambda x: x.sample(negatives_per_user, random_state=0))
        ]).reset_index(drop=True)
    
    user_features_for_train = events_train[events_train['target']==0].groupby("user_id").agg(
            item_id_week=("timestamp", lambda x: (x.max()-x.min()).days/7),
            item_viewed=("item_id", "count"),
            rating_avg=("rating", "mean"),
            rating_std=("rating", "std"))
    
    candidates_for_train = candidates_for_train.merge(user_features_for_train, on="user_id", how="left").fillna(0)

    events_inference = pd.concat([events_train, events_labels])

    # Оставляем только тех пользователей, что есть в тестовой выборке
    candidates_to_rank = events_inference[events_inference.user_id.isin(events_test_2.user_id.drop_duplicates())]

    # Получим новые признаки
    user_features_for_ranking = events_inference[events_inference['target']==0].groupby("user_id").agg(
            item_id_week=("timestamp", lambda x: (x.max()-x.min()).days/7),
            item_viewed=("item_id", "count"),
            rating_avg=("rating", "mean"),
            rating_std=("rating", "std"))

    candidates_to_rank = candidates_to_rank.merge(user_features_for_ranking, on="user_id", how="left").fillna(0) 

    csv_buffer_1 = io.StringIO()
    candidates_to_rank.to_csv(csv_buffer_1, index=False)
    candidates_to_rank_push = csv_buffer_1.getvalue()

    csv_buffer_2 = io.StringIO()
    candidates_for_train.to_csv(csv_buffer_2, index=False)
    candidates_for_train_push = csv_buffer_2.getvalue()

    ti.xcom_push(key='candidates_to_rank', value=candidates_to_rank_push) # вместо return отправляем данные передатчику task_instance
    ti.xcom_push(key='candidates_for_train', value=candidates_for_train_push)

def train_inference_model(**kwargs):

    """ Train model and inference """

    ti = kwargs['ti'] # получение объекта task_instance

    csv_data_1 = ti.xcom_pull(key='candidates_to_rank')
    # Десериализуем CSV обратно в DataFrame
    candidates_to_rank = pd.read_csv(io.StringIO(csv_data_1))

    csv_data_2 = ti.xcom_pull(key='candidates_for_train')
    candidates_for_train = pd.read_csv(io.StringIO(csv_data_2))
    
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')

    features = ['categoryid_enc',
                'parentid_enc',
                'available',
                'istransaction',
                'day_of_week',
                'day',
                'hour',
                'rating',
                'item_id_week',
                'item_viewed',
                'rating_avg',
                'rating_std']
    
    cat_features = ['categoryid_enc',
                'parentid_enc',
                'available',
                'istransaction',
                'day_of_week',
                'day',
                'hour']
    
    target = ["target"]
    
    train_data = Pool(
        data=candidates_for_train[features],
        label=candidates_for_train[target],
        cat_features=cat_features)

    # Инициализируем модель CatBoostClassifier
    cb_model = CatBoostClassifier(iterations=3500,
                           learning_rate=0.1,
                           depth=6,
                           loss_function='Logloss',
                           auto_class_weights='Balanced',
                           verbose=False,
                           early_stopping_rounds=50,
                           random_seed=42)
    # Обучим модель
    cb_model.fit(train_data)

    # Создадим датасет для катбуста
    inference_data = Pool(data=candidates_to_rank[features], cat_features=cat_features)
    # Получим вероятности
    predictions = cb_model.predict_proba(inference_data)

    # Создадим признак с вероятностями базовой модели
    candidates_to_rank["cb_score"] = predictions[:, 1]

    # Для каждого пользователя проставляем rank, начиная с 1 — это максимальный cb_score
    candidates_to_rank = candidates_to_rank.sort_values(["user_id", "cb_score"], ascending=[True, False])
    candidates_to_rank["rank"] = candidates_to_rank.groupby("user_id").cumcount() + 1
    
    # Отранжируем рекомендации
    candidates_to_rank["rank"] = candidates_to_rank.groupby("user_id").cumcount() + 1

    # Сохраним результат с другим названием
    recommendations = candidates_to_rank[['user_id','item_id','rank']].copy()

    # Сохраняем модель в файл .pkl
    with open("cb_model.pkl", 'wb') as model_file:
        pickle.dump(cb_model, model_file)

    # Загружаем файл в S3
    s3_hook.load_file("cb_model.pkl", 
                      key=f'{PATH_MODEL}cb_model.pkl', 
                      bucket_name=BACKET_NAME, 
                      replace=True)


    recommendations.to_parquet('recommendations.parquet', index=False)

    s3_hook.load_file('recommendations.parquet', 
                      key=f'{PATH_RECS}recommendations.parquet',
                       bucket_name=BACKET_NAME, 
                       replace=True)

