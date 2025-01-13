import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import numpy as np
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime, Float, inspect, UniqueConstraint

def create_table(**kwargs):
    
    ti = kwargs['ti'] # получение объекта task_instance
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    source_conn = engine.connect()

    metadata = MetaData()
    property_stage1_table = Table(
        'real_estate',
        metadata,
        Column('id', Float, primary_key=True, autoincrement=True),
        Column('building_id', Float),
        Column('build_year', Float),
        Column('building_type_int', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Float),
        Column('floors_total', Float),
        Column('has_elevator', String),
        Column('floor', Float),
        Column('is_apartment', String),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Float),
        Column('studio', String),
        Column('total_area', Float),
        Column('target', Float),
        UniqueConstraint('id', name='unique_flat_id_new')
    ) 

    if not inspect(source_conn).has_table(property_stage1_table.name): 
        metadata.create_all(source_conn)

    source_conn.close()

def extract(**kwargs):
	# ваш код здесь #
    ti = kwargs['ti'] # получение объекта task_instance
    
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    sql = f"""
        SELECT  df1.id,
                df2.build_year,
                df2.building_type_int,
                df2.latitude,
                df2.longitude,
                df2.ceiling_height,
                df2.flats_count,
                df2.floors_total,
                df2.has_elevator,
                df1.building_id,
                df1.floor,
                df1.is_apartment,
                df1.kitchen_area,
                df1.living_area,
                df1.rooms,
                df1.studio,
                df1.total_area,
                df1.price AS target
        FROM flats AS df1
        LEFT JOIN buildings AS df2
        ON df2.id = df1.building_id
        """
    data = pd.read_sql(sql, conn)
    conn.close()
    
    ti.xcom_push('extracted_data', data) # вместо return отправляем данные передатчику

def transform(**kwargs):
    """
    #### Transform task
    """
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='extracted_data') # выгрузка данных из task_instance
    ti.xcom_push('transformed_data', data) # вместо return отправляем данные передатчику task_instance

# CLEAN
def clean(**kwargs):
    """
    #### Transform task
    """
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='transformed_data') # выгрузка данных из task_instance
    ti.xcom_push('cleaned_data', data) # вместо return отправляем данные передатчику task_instance

def load(**kwargs):
    ti = kwargs['ti'] # получение объекта task_instance
    
    data = ti.xcom_pull(task_ids="transform", key='cleaned_data') # выгрузка данных из task_instance
    hook = PostgresHook('destination_db')
    hook.insert_rows(
            table="real_estate",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
    ) 