from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup

# Chemins
RAW_CSV       = "/opt/airflow/data/online_retail.csv"
ARCHIVE_DIR   = "/opt/airflow/data/archive"
EXTRACTED_CSV = "/opt/airflow/data/extracted_online_retail.csv"
CLEANED_CSV   = "/opt/airflow/data/cleaned_online_retail.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    "etl_online_retail_etl_format",
    default_args=default_args,
    description="ETL Online Retail : extract → transform → load",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["etl", "online_retail"],
) as dag:

    # 1) Extract
    def extract():
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        shutil.copy(RAW_CSV, f"{ARCHIVE_DIR}/online_retail_{ts}.csv")
        pd.read_csv(RAW_CSV, encoding="ISO-8859-1") \
          .to_csv(EXTRACTED_CSV, index=False)

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # 2) Transform
    def transform():
        df = pd.read_csv(EXTRACTED_CSV, encoding="ISO-8859-1")
        df["Quantity"]   = pd.to_numeric(df["Quantity"],   errors="coerce")
        df["UnitPrice"]  = pd.to_numeric(df["UnitPrice"],  errors="coerce")
        df = df.dropna(subset=["CustomerID", "InvoiceDate"])
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], dayfirst=True)
        df = df[(df["Quantity"] > 0) & (df["UnitPrice"] >= 0)]
        df = df.drop_duplicates(subset=["InvoiceNo", "StockCode"])
        for col in ("Quantity", "UnitPrice"):
            Q1, Q3 = df[col].quantile([0.25, 0.75])
            IQR = Q3 - Q1
            df = df[(df[col] >= Q1 - 1.5 * IQR) & (df[col] <= Q3 + 1.5 * IQR)]
        df["total_price"] = df["Quantity"] * df["UnitPrice"]
        df["year"]    = df["InvoiceDate"].dt.year
        df["month"]   = df["InvoiceDate"].dt.month
        df["day"]     = df["InvoiceDate"].dt.day
        df["weekday"] = df["InvoiceDate"].dt.weekday
        df["hour"]    = df["InvoiceDate"].dt.hour
        df.columns = [c.lower() for c in df.columns]
        df.to_csv(CLEANED_CSV, index=False)

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # 3) Load into staging table

    def load_staging():
        df = pd.read_csv(CLEANED_CSV)
        engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
        with engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS online_retail (
                    invoiceno     VARCHAR,
                    stockcode     VARCHAR,
                    description   TEXT,
                    quantity      INT,
                    invoicedate   TIMESTAMP,
                    unitprice     NUMERIC,
                    customerid    INT,
                    country       VARCHAR,
                    total_price   NUMERIC,
                    year          INT,
                    month         INT,
                    day           INT,
                    weekday       INT,
                    hour          INT
                );
                TRUNCATE online_retail;
            """)
            df.to_sql("online_retail", con=conn, if_exists="append", index=False)


    load_staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
    )


    # 4) Build dims, fact, QA & notify
    with TaskGroup("load") as load_group:

        create_dimensions = PostgresOperator(
            task_id="create_dimensions",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE TABLE IF NOT EXISTS dim_customer (
              customerid INT PRIMARY KEY,
              country    VARCHAR
            );
            CREATE TABLE IF NOT EXISTS dim_product (
              stockcode    VARCHAR PRIMARY KEY,
              description  TEXT
            );
            CREATE TABLE IF NOT EXISTS dim_date (
              date_id DATE PRIMARY KEY,
              year    INT,
              month   INT,
              day     INT,
              weekday INT,
              hour    INT
            );
            """
        )

        create_fact = PostgresOperator(
            task_id="create_fact_sales",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE TABLE IF NOT EXISTS fact_sales (
              invoiceno    VARCHAR,
              stockcode    VARCHAR,
              customerid   INT,
              date_id      DATE,
              quantity     INT,
              total_price  NUMERIC
            );
            """
        )


        load_dim_customer = PostgresOperator(
            task_id="load_dim_customer",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_customer(customerid, country)
            SELECT DISTINCT customerid, country
              FROM online_retail
            ON CONFLICT (customerid) DO NOTHING;
            """
        )

        load_dim_product = PostgresOperator(
            task_id="load_dim_product",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_product(stockcode, description)
            SELECT DISTINCT stockcode, description
              FROM online_retail
            ON CONFLICT (stockcode) DO NOTHING;
            """
        )

        load_dim_date = PostgresOperator(
            task_id="load_dim_date",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_date(date_id, year, month, day, weekday, hour)
            SELECT DISTINCT invoicedate::date, year, month, day, weekday, hour
              FROM online_retail
            ON CONFLICT (date_id) DO NOTHING;
            """
        )

        load_fact = PostgresOperator(
            task_id="load_fact_sales",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO fact_sales(invoiceno, stockcode, customerid, date_id, quantity, total_price)
            SELECT invoiceno, stockcode, customerid, invoicedate::date, quantity, total_price
              FROM online_retail;
            """
        )

        quality_checks = PostgresOperator(
            task_id="quality_checks",
            postgres_conn_id="postgres_default",
            sql="""
            SELECT
              (SELECT COUNT(*) FROM online_retail) AS src,
              (SELECT COUNT(*) FROM fact_sales)    AS tgt;
            """
        )



        create_dimensions >> create_fact \
          >> [load_dim_customer, load_dim_product, load_dim_date] \
          >> load_fact >> quality_checks 
    # Orchestration finale
    extract_task >> transform_task >> load_staging_task >> load_group


# Nécessaire pour que le DAG soit reconnu par Airflow
dag = dag
