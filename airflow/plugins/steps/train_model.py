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
import configparser

# Создаем объект ConfigParser
config = configparser.ConfigParser()

# Читаем конфигурационный файл
config.read('config.ini')

# Получаем значения параметров
path_data = config['PATHS']['PATH_DATA']
path_recs = config['PATHS']['PATH_RECS']
path_model = config['PATHS']['PATH_MODEL']

bucket_name = config['S3']['BUCKET_NAME']

query_category = config['QUERIES']['CATEGORY']
query_events = config['QUERIES']['EVENTS']
query_items = config['QUERIES']['ITEMS']

selected_columns = config['COLUMNS']['selected_columns'].split(',')
features = config['FEATURES']['features'].split(',')
cat_features = config['FEATURES']['cat_features'].split(',')

def upload_parquet_to_s3(df, file_name, path_data, bucket_name, s3_hook):
    """
    Upload parquet dataframe to S3
    """
    # Сохраняем DataFrame в Parquet
    parquet_file = f'{file_name}_tmp.parquet'
    df.to_parquet(parquet_file, index=False)
    
    # Загружаем файл в S3
    s3_key = f'{path_data}{parquet_file}'
    s3_hook.load_file(
        filename=parquet_file,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

def load_parquet_from_s3(s3_client, bucket_name, key):
    """
    Load parquet from S3 and write to pandas DataFrame.
    """
    # Получаем объект из S3
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    
    # Читаем содержимое объекта
    parquet_file = BytesIO(response['Body'].read())
    
    # Загружаем Parquet-файл в DataFrame
    table = pq.read_table(parquet_file)
    return table.to_pandas()

def extract(**kwargs):

    """ Extract data from database """

    # Данные были заранее загружены в базу данных
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    engine = hook.get_sqlalchemy_engine()
    source_conn = engine.connect()

    category_tree = pd.read_sql(query_category, source_conn)
    events = pd.read_sql(query_events, source_conn)
    items = pd.read_sql(query_items, source_conn)

    source_conn.close()

    # Промежуточные файлы имеют много строк, поэтому загрузим в хранилище без создания таблиц в бд
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    
    upload_parquet_to_s3(category_tree, 'category_tree', path_data, bucket_name, s3_hook)
    upload_parquet_to_s3(events, 'events', path_data, bucket_name, s3_hook)
    upload_parquet_to_s3(items, 'items', path_data, bucket_name, s3_hook)


def transform(**kwargs):
    """
    #### Transform task
    """

    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    s3_client = s3_hook.get_conn()

    # Загружаем данные с использованием функции
    category_tree = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}category_tree_tmp.parquet')
    
    events = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}events_tmp.parquet')
    
    items = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}items_tmp.parquet')

    # Заполним отсутствие транзакции 0 - покупка не совершена
    events['transactionid'] = events['transactionid'].fillna(0).astype('int')
    # Создадим новый признак - факт совершения покупки
    events['istransaction'] = events['transactionid'].apply(lambda x: 0 if x==0 else 1).astype('int')
    # Добавим в качестве таргета признак - добавления товара в корзину
    events['target'] = events['event'].apply(lambda x: 1 if x=='addtocart' else 0)
    # Преобразуем формат времени
    events['timestamp'] = pd.to_datetime(events['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    
    # Преобразуем дату
    items['timestamp'] = pd.to_datetime(items['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M')
    # Остортируем датасет по времени и товарам
    items = items.sort_values(['timestamp', 'item_id']).reset_index(drop=True)
    # Удалим дубликаты, оставив только последнюю по времени строку со свойствами
    items = items.drop_duplicates(['item_id','property','value'], keep='last').reset_index(drop=True)

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
    events['categoryid'] = events['categoryid'].fillna(0).astype('int')

    events = events.merge(category_tree, on='categoryid', how='left')

    upload_parquet_to_s3(
        df=events,
        file_name='events_transformed',
        path_data=path_data,
        bucket_name=bucket_name,
        s3_hook=s3_hook
        )
    
    upload_parquet_to_s3(
        df=items,
        file_name='items_transformed',
        path_data=path_data,
        bucket_name=bucket_name,
        s3_hook=s3_hook
        )


def feature_engineering(**kwargs):
    """
    #### Transform task
    """
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    s3_client = s3_hook.get_conn()

    events = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}events_transformed.parquet'
        )

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

    upload_parquet_to_s3(
        df=events,
        file_name='events_final',
        path_data=path_data,
        bucket_name=bucket_name,
        s3_hook=s3_hook
        )
    
# Функция split_dataframes была разделена на логические блоки
def split_train_test(events, test_days=30):
    """
    Train test split
    """
    events['timestamp'] = pd.to_datetime(events['timestamp'], errors='coerce')
    split_date = events["timestamp"].max() - pd.Timedelta(days=test_days)
    train_idx = events["timestamp"] < split_date

    return events[train_idx], events[~train_idx]

def encode_categorical_features(events, 
                                events_train,
                                events_test, 
                                column):
    """
    Category features encoding
    """
    user_encoder = sklearn.preprocessing.LabelEncoder() 
    user_encoder.fit(events[column]) 

    events[f"{column}_enc"] = user_encoder.transform(events[column]) 
    events_train[f"{column}_enc"] = user_encoder.transform(events_train[column]) 
    events_test[f"{column}_enc"] = user_encoder.transform(events_test[column]) 

    return events, events_train, events_test

def create_candidates_for_train(events_labels, negatives_per_user=1):
    """
    Catboost train dataset creation
    """
    candidates_to_sample = events_labels.groupby("user_id").filter(lambda x: x["target"].sum() > 0)
    candidates_for_train = pd.concat([
        candidates_to_sample.query("target == 1"),
        candidates_to_sample.query("target == 0") \
            .groupby("user_id") \
            .apply(lambda x: x.sample(negatives_per_user, random_state=0))
    ]).reset_index(drop=True)

    return candidates_for_train

def create_candidates_to_rank(events_train, events_labels, events_test_2):
    """
    Ranking dataset creation
    """
    events_inference = pd.concat([events_train, events_labels])
    candidates_to_rank = events_inference[events_inference.user_id.isin(events_test_2.user_id.drop_duplicates())]

    return candidates_to_rank

def add_user_features(df):
    """
    Add user features
    """
    user_features = df[df['target'] == 0].groupby("user_id").agg(
        item_id_week=("timestamp", lambda x: (x.max() - x.min()).days / 7),
        item_viewed=("item_id", "count"),
        rating_avg=("rating", "mean"),
        rating_std=("rating", "std")
    )
    return df.merge(user_features, on="user_id", how="left").fillna(0)


def split_dataframes(**kwargs):
    """
    #### Split data
    """
    ti = kwargs['ti']

    # Загрузка данных
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    s3_client = s3_hook.get_conn()

    events = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}events_final.parquet'
    )

    items = load_parquet_from_s3(
        s3_client=s3_client,
        bucket_name=bucket_name,
        key=f'{path_data}items_transformed.parquet'
    )

    # Разделение данных на train и test
    events_train, events_test = split_train_test(events)

    # Перекодируем идентификаторы объектов в последовательность с 0 
    item_encoder = sklearn.preprocessing.LabelEncoder() 
    item_encoder.fit(items["item_id"]) 
    items["item_id_enc"] = item_encoder.transform(items["item_id"]) 
    events_train["item_id_enc"] = item_encoder.transform(events_train["item_id"]) 
    events_test["item_id_enc"] = item_encoder.transform(events_test["item_id"]) 

    # Далее воспользуемся функцией
    enc_col = ["user_id", "categoryid", "parentid"]
    for column in enc_col:
        events, events_train, events_test = encode_categorical_features(events, 
                                                                        events_train,
                                                                        events_test, 
                                                                        column)

    # Разделение тестовой выборки на labels и test_2
    test_days = events_test["timestamp"].max() - pd.Timedelta(days=3)
    split_date_for_labels_idx = events_test["timestamp"] < test_days
    events_labels = events_test[split_date_for_labels_idx].copy()
    events_test_2 = events_test[~split_date_for_labels_idx].copy()

    # Создание кандидатов для обучения
    candidates_for_train = create_candidates_for_train(events_labels)
    candidates_for_train = add_user_features(candidates_for_train)

    # Создание кандидатов для ранжирования
    candidates_to_rank = create_candidates_to_rank(events_train, events_labels, events_test_2)
    candidates_to_rank = add_user_features(candidates_to_rank)

    # вместо return отправляем данные передатчику task_instance
    csv_buffer_1 = io.StringIO()
    candidates_to_rank.to_csv(csv_buffer_1, index=False)
    candidates_to_rank_push = csv_buffer_1.getvalue()

    csv_buffer_2 = io.StringIO()
    candidates_for_train.to_csv(csv_buffer_2, index=False)
    candidates_for_train_push = csv_buffer_2.getvalue()

    ti.xcom_push(key='candidates_to_rank', value=candidates_to_rank_push)
    ti.xcom_push(key='candidates_for_train', value=candidates_for_train_push)

def train_inference_model(**kwargs):

    """ Train model and inference """

    ti = kwargs['ti'] # получение объекта task_instance
    
   # Десериализуем CSV обратно в DataFrame
    csv_data_1 = ti.xcom_pull(key='candidates_to_rank')
    candidates_to_rank = pd.read_csv(io.StringIO(csv_data_1))

    csv_data_2 = ti.xcom_pull(key='candidates_for_train')
    candidates_for_train = pd.read_csv(io.StringIO(csv_data_2))
    
    s3_hook = S3Hook(aws_conn_id='s3_aws_connection')
    
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
    
    # Отранжируем рекомендации
    candidates_to_rank["rank"] = candidates_to_rank.groupby("user_id").cumcount() + 1

    # Сохраним результат с другим названием
    recommendations = candidates_to_rank[['user_id','item_id','rank']].copy()

    # Сохраняем модель в файл .pkl
    with open("cb_model.pkl", 'wb') as model_file:
        pickle.dump(cb_model, model_file)

    # Загружаем файл в S3
    s3_hook.load_file("cb_model.pkl", 
                      key=f'{path_model}cb_model.pkl', 
                      bucket_name=bucket_name, 
                      replace=True)
    
    upload_parquet_to_s3(
        df=recommendations,
        file_name='recommendations',
        path_data=path_data,
        bucket_name=bucket_name,
        s3_hook=s3_hook
        )

