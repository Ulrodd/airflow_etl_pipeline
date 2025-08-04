from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
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
    load_staging = PostgresOperator(
        task_id="load_staging",
        postgres_conn_id="postgres_default",
        sql=f"""
          CREATE TABLE IF NOT EXISTS online_retail (
            invoice_no   VARCHAR,
            stock_code   VARCHAR,
            description  TEXT,
            quantity     INT,
            invoice_date TIMESTAMP,
            unit_price   NUMERIC,
            customer_id  INT,
            country      VARCHAR,
            total_price  NUMERIC,
            year         INT,
            month        INT,
            day          INT,
            weekday      INT,
            hour         INT
          );
          TRUNCATE online_retail;
          COPY online_retail
            FROM '{CLEANED_CSV}'
            WITH (FORMAT csv, HEADER true);
        """
    )

    # 4) Build dims, fact, QA & notify
    with TaskGroup("load") as load_group:

        create_dimensions = PostgresOperator(
            task_id="create_dimensions",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE TABLE IF NOT EXISTS dim_customer (
              customer_id INT PRIMARY KEY,
              country     VARCHAR
            );
            CREATE TABLE IF NOT EXISTS dim_product (
              stock_code VARCHAR PRIMARY KEY,
              description TEXT
            );
            CREATE TABLE IF NOT EXISTS dim_date (
              date_id DATE PRIMARY KEY,
              year INT, month INT, day INT, weekday INT, hour INT
            );
            """
        )

        create_fact = PostgresOperator(
            task_id="create_fact_sales",
            postgres_conn_id="postgres_default",
            sql="""
            CREATE TABLE IF NOT EXISTS fact_sales (
              invoice_no  VARCHAR,
              stock_code  VARCHAR,
              customer_id INT,
              date_id     DATE,
              quantity    INT,
              total_price NUMERIC
            );
            """
        )

        load_dim_customer = PostgresOperator(
            task_id="load_dim_customer",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_customer(customer_id, country)
            SELECT DISTINCT customer_id, country
              FROM online_retail
            ON CONFLICT (customer_id) DO NOTHING;
            """
        )

        load_dim_product = PostgresOperator(
            task_id="load_dim_product",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_product(stock_code, description)
            SELECT DISTINCT stock_code, description
              FROM online_retail
            ON CONFLICT (stock_code) DO NOTHING;
            """
        )

        load_dim_date = PostgresOperator(
            task_id="load_dim_date",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO dim_date(date_id, year, month, day, weekday, hour)
            SELECT DISTINCT invoice_date::date, year, month, day, weekday, hour
              FROM online_retail
            ON CONFLICT (date_id) DO NOTHING;
            """
        )

        load_fact = PostgresOperator(
            task_id="load_fact_sales",
            postgres_conn_id="postgres_default",
            sql="""
            INSERT INTO fact_sales(invoice_no, stock_code, customer_id, date_id, quantity, total_price)
            SELECT invoice_no, stock_code, customer_id, invoice_date::date, quantity, total_price
              FROM online_retail;
            """
        )

        quality_checks = PostgresOperator(
            task_id="quality_checks",
            postgres_conn_id="postgres_default",
            sql="""
            SELECT
              (SELECT COUNT(*) FROM online_retail) AS src,
              (SELECT COUNT(*) FROM fact_sales)  AS tgt;
            """
        )

        notify = SlackWebhookOperator(
            task_id="slack_notify",
            http_conn_id="slack_webhook",
            message="DAG exécuté avec succès",
            channel="#data-alerts",
        )

        create_dimensions >> create_fact \
          >> [load_dim_customer, load_dim_product, load_dim_date] \
          >> load_fact >> quality_checks >> notify

    # Orchestration finale
    extract_task >> transform_task >> load_staging >> load_group
