#Prueba_ETL2_Python

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Importar Pandas
import pandas as pd

# Definir las variables de conexión a la base de datos
PG_CONN_ID = "postgres_default"
TARGET_TABLE = "ventas_consolidadas"

# Función de Python que ejecuta el flujo ETL
def etl_pandas_job():
    """
    Realiza la extracción, transformación y carga de datos usando Pandas.
    1. Se conecta a la base de datos.
    2. Extrae las tablas de origen en DataFrames de Pandas.
    3. Realiza los merges (joins) con Pandas.
    4. Carga el DataFrame resultante en la tabla de destino.
    """
    # Inicializar el hook para interactuar con la base de datos
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    # 1. Extracción (Extract) de las tablas de PostgreSQL a DataFrames de Pandas
    df_departments = pg_hook.get_pandas_df("SELECT * FROM departments")
    df_categories = pg_hook.get_pandas_df("SELECT * FROM categories")
    df_products = pg_hook.get_pandas_df("SELECT * FROM products")
    df_customers = pg_hook.get_pandas_df("SELECT * FROM customers")
    df_orders = pg_hook.get_pandas_df("SELECT * FROM orders")
    df_order_items = pg_hook.get_pandas_df("SELECT * FROM order_items")

    # 2. Transformación (Transform) - Unir los DataFrames
    # Comenzar con df_order_items y unir secuencialmente
    df_resultados = df_order_items.merge(
        df_orders, 
        how="inner",
        left_on="order_item_order_id",
        right_on="order_id"
    ).merge(
        df_customers,
        how="inner",
        left_on="order_customer_id",
        right_on="customer_id"
    ).merge(
        df_products,
        how="inner",
        left_on="order_item_product_id",
        right_on="product_id"
    ).merge(
        df_categories,
        how="inner",
        left_on="product_category_id",
        right_on="category_id"
    ).merge(
        df_departments,
        how="inner",
        left_on="category_department_id",
        right_on="department_id"
    )
    
    # 3. Selección de columnas de interés
    df_final = df_resultados[[
        "order_id",
        "order_status",
        "order_date",
        "customer_fname",
        "customer_lname",
        "product_name",
        "product_price",
        "order_item_quantity",
        "department_name"
    ]]
    df_final = df_final.sort_values(by="order_id", ascending=True)
    
    # 4. Carga (Load) del DataFrame en la tabla de destino
    df_final.to_sql(
        TARGET_TABLE, 
        con=pg_hook.get_sqlalchemy_engine(), 
        if_exists="replace", # O 'append'
        index=False
    )

# Definición del DAG
with DAG(
    dag_id="etl_ventas_pandas_v3",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "pandas", "postgres"],
) as dag:
    # Una única tarea que maneja todo el proceso ETL
    etl_task = PythonOperator(
        task_id="run_etl_process_pandas",
        python_callable=etl_pandas_job
    )