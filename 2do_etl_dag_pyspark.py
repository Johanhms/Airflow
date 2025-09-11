from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# Importar PySpark y otras librerías necesarias
from pyspark.sql import SparkSession
import pandas as pd

# Definir las variables de conexión y la tabla de destino
PG_CONN_ID = "postgres_default"
TARGET_TABLE = "ventas_consolidadas"

# Función de Python que ejecuta todo el flujo ETL
def etl_pyspark_job():
    """
    Realiza la extracción de datos de PostgreSQL,
    la transformación con PySpark y la carga en la tabla de destino.
    """
    # 1. Conexión a la base de datos
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    
    # 2. Creación de la sesión de Spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ETL_Airflow") \
        .getOrCreate()
        
    try:
        # Extraer los datos directamente de PostgreSQL como DataFrames de Pandas
        # y convertirlos a DataFrames de Spark
        df_departments_pd = pg_hook.get_pandas_df("SELECT * FROM departments")
        df_departments = spark.createDataFrame(df_departments_pd)

        df_categories_pd = pg_hook.get_pandas_df("SELECT * FROM categories")
        df_categories = spark.createDataFrame(df_categories_pd)

        df_products_pd = pg_hook.get_pandas_df("SELECT * FROM products")
        df_products = spark.createDataFrame(df_products_pd)
        
        df_customers_pd = pg_hook.get_pandas_df("SELECT * FROM customers")
        df_customers = spark.createDataFrame(df_customers_pd)
        
        df_orders_pd = pg_hook.get_pandas_df("SELECT * FROM orders")
        df_orders = spark.createDataFrame(df_orders_pd)

        df_order_items_pd = pg_hook.get_pandas_df("SELECT * FROM order_items")
        df_order_items = spark.createDataFrame(df_order_items_pd)
        
        # 3. Transformación (joins en PySpark)
        df_resultados = df_order_items.join(
            df_orders, 
            df_order_items.order_item_order_id == df_orders.order_id
        ).join(
            df_customers,
            df_orders.order_customer_id == df_customers.customer_id
        ).join(
            df_products,
            df_order_items.order_item_product_id == df_products.product_id
        ).join(
            df_categories,
            df_products.product_category_id == df_categories.category_id
        ).join(
            df_departments,
            df_categories.category_department_id == df_departments.department_id
        )

        df_final = df_resultados.select(
            "order_id",
            "order_status",
            "customer_fname",
            "customer_lname",
            "product_name",
            "product_price",
            "order_item_quantity",
            "department_name"
        )
        
        # 4. Carga (Load) de los datos a PostgreSQL
        # Convertir a Pandas para la inserción
        df_final_pd = df_final.toPandas()
        
        # Cargar los datos a la tabla de destino
        df_final_pd.to_sql(
            TARGET_TABLE, 
            con=pg_hook.get_sqlalchemy_engine(), 
            if_exists="replace", # 'replace' borra la tabla y la recrea, 'append' agrega filas
            index=False
        )

    finally:
        # Detener la sesión de Spark
        spark.stop()

# Definición del DAG
with DAG(
    dag_id="etl_ventas_consolidadas_v2",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "pyspark", "postgres"],
) as dag:
    # Una única tarea que maneja todo el proceso ETL de forma secuencial
    etl_task = PythonOperator(
        task_id="run_etl_process",
        python_callable=etl_pyspark_job
    )