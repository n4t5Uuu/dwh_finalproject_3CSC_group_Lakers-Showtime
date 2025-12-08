from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values


# =======================================
# PYTHON LOADER FOR STAGING PRODUCT DATA
# =======================================

def load_staging_product_data():
    df = pd.read_csv("/clean_data/business/product_list.csv")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO staging.product_data (
            product_id, product_name, product_type, price, product_key
        ) VALUES %s
        """,
        df.values.tolist()
    )

    conn.commit()
    cur.close()



# =======================================
# DAG DEFINITION
# =======================================
with DAG(
    dag_id="dwh_product_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dwh", "product"],
) as dag:


    # --------------------------------------
    # 1. CREATE STAGING TABLE
    # --------------------------------------
    create_staging_product = PostgresOperator(
        task_id="create_staging_product",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.product_data (
            product_id VARCHAR(40),
            product_name VARCHAR(255),
            product_type VARCHAR(100),
            price NUMERIC(12,2),
            product_key VARCHAR(40)
        );
        """
    )


    # --------------------------------------
    # 2. LOAD STAGING FROM CSV
    # --------------------------------------
    load_staging = PythonOperator(
        task_id="load_staging_product",
        python_callable=load_staging_product_data,
    )

    create_dim_product = PostgresOperator(
        task_id="create_dim_product",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS shopzada;

            CREATE TABLE IF NOT EXISTS shopzada.dimProduct (
                product_key VARCHAR(40) PRIMARY KEY,
                product_id VARCHAR(40),
                product_name VARCHAR(255),
                product_type VARCHAR(100),
                price NUMERIC(12,2)
            );
        """
    )
    # --------------------------------------
    # 3. INSERT INTO DIM PRODUCT (DEDUPED)
    # --------------------------------------
    load_dim_product = PostgresOperator(
        task_id="load_dim_product",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO shopzada.dimProduct (
            product_key,
            product_id,
            product_name,
            product_type,
            price
        )
        SELECT DISTINCT ON (product_id)
            product_key,
            product_id,
            product_name,
            product_type,
            price
        FROM staging.product_data
        ORDER BY product_id, product_key;
        """
    )


    # --------------------------------------
    # PIPELINE ORDER
    # --------------------------------------
    create_staging_product >> load_staging >> create_dim_product >> load_dim_product
