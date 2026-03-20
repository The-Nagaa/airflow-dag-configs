# DAG: customer_care_emails_ingest.py
# Author: naga_poluri
# Project: DHAP-34
#
# This DAG loads the customer_care_emails CSV into PostgreSQL.
# It validates the schema first, cleans the data, then loads it.
# Rows where status is already "done" are skipped.

import os
import logging
import yaml
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# paths inside the docker container
DATASET_NAME = "customer_care_emails"
BASE_DIR = f"/opt/airflow/extraction/{DATASET_NAME}"
CSV_PATH = os.path.join(BASE_DIR, "sample_data", f"{DATASET_NAME}.csv")
SCHEMA_PATH = os.path.join(BASE_DIR, "config", "schema_expected.yaml")
DDL_PATH = os.path.join(BASE_DIR, "config", "create_table.sql")
CLEAN_CSV = os.path.join(BASE_DIR, "sample_data", "transformed.csv")


def get_db_connection():
    # reads credentials from environment variables, never hardcoded
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"]
    )


def task_file_check(**context):
    # make sure the CSV exists before we do anything else
    logging.info("checking if CSV file exists...")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"CSV not found at: {CSV_PATH}\n"
            "Please download the file from SharePoint and place it in sample_data/"
        )

    df = pd.read_csv(CSV_PATH)

    if df.empty:
        raise ValueError("The CSV file is empty, nothing to load.")

    logging.info(f"found file: {CSV_PATH}")
    logging.info(f"rows: {len(df)}, columns: {list(df.columns)}")

    context["ti"].xcom_push(key="total_rows", value=len(df))
    context["ti"].xcom_push(key="column_names", value=list(df.columns))


def task_validate_schema(**context):
    # compare the CSV columns against our schema contract
    logging.info("validating schema against schema_expected.yaml...")

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_cols = [col["name"] for col in schema["columns"]]
    df = pd.read_csv(CSV_PATH)
    actual_cols = list(df.columns)

    logging.info(f"expected: {expected_cols}")
    logging.info(f"actual:   {actual_cols}")

    # fail if any expected columns are missing
    missing = set(expected_cols) - set(actual_cols)
    if missing:
        raise ValueError(
            f"Schema check failed! Missing columns: {missing}\n"
            "Check your CSV headers or update schema_expected.yaml"
        )

    # just warn if there are extra columns we dont expect
    extra = set(actual_cols) - set(expected_cols)
    if extra:
        logging.warning(f"extra columns in CSV (will be ignored): {extra}")

    # check not-null columns dont have any nulls
    errors = []
    for col_def in schema["columns"]:
        col_name = col_def["name"]
        nullable = col_def.get("nullable", True)
        if not nullable and col_name in df.columns:
            null_count = df[col_name].isnull().sum()
            if null_count > 0:
                errors.append(f"  '{col_name}' has {null_count} null(s) but is NOT NULL")

    if errors:
        raise ValueError("Nullability check failed:\n" + "\n".join(errors))

    logging.info("schema validation passed!")


def task_transform(**context):
    # clean the data before loading
    logging.info("starting transform...")

    df = pd.read_csv(CSV_PATH)
    total = len(df)
    logging.info(f"loaded {total} rows from CSV")

    # skip rows that are already marked done
    done_mask = df["status"].str.strip().str.lower() == "done"
    skipped = int(done_mask.sum())
    df = df[~done_mask].copy()
    logging.info(f"skipped {skipped} rows with status=done, {len(df)} rows remaining")

    if df.empty:
        raise ValueError(
            "Nothing left to load after skipping done rows. "
            "All records may have already been processed."
        )

    # strip whitespace from all string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # replace empty strings with None so they become NULL in postgres
    df.replace("", None, inplace=True)

    # parse timestamps
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            bad = df[col].isnull().sum()
            if bad > 0:
                logging.warning(f"{bad} bad timestamps in '{col}', set to NULL")

    # fix numeric types
    if "sentiment_score" in df.columns:
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    df.to_csv(CLEAN_CSV, index=False)
    logging.info(f"saved cleaned data to {CLEAN_CSV}")
    logging.info(f"summary -> total: {total}, skipped: {skipped}, to load: {len(df)}")

    context["ti"].xcom_push(key="rows_to_load", value=len(df))
    context["ti"].xcom_push(key="rows_skipped", value=skipped)


def task_create_table(**context):
    # run the DDL to create the table if it doesnt exist yet
    logging.info("running create_table.sql...")

    with open(DDL_PATH, "r") as f:
        ddl = f.read()

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        logging.info("table is ready in postgres")
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"failed to create table: {e}")
    finally:
        conn.close()


def task_load_to_postgres(**context):
    # load the cleaned CSV into the postgres table
    logging.info("loading data into postgres...")

    df = pd.read_csv(CLEAN_CSV)

    if df.empty:
        logging.warning("nothing to load, transformed file is empty")
        return

    # make sure we only load columns defined in the schema
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    schema_cols = [col["name"] for col in schema["columns"]]
    load_cols = [c for c in schema_cols if c in df.columns]
    df = df[load_cols]

    # convert rows to list of tuples for psycopg2
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    col_str = ", ".join(load_cols)
    insert_sql = f"""
        INSERT INTO public.{DATASET_NAME} ({col_str})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        execute_values(cur, insert_sql, rows, page_size=500)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM public.{DATASET_NAME};")
        total_in_table = cur.fetchone()[0]

        logging.info(f"inserted {len(rows)} rows this run")
        logging.info(f"total rows in table: {total_in_table}")
        logging.info("load done!")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"load failed: {e}")
    finally:
        conn.close()


# DAG definition
default_args = {
    "owner": "naga_poluri",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="customer_care_emails_ingest",
    default_args=default_args,
    description="DHAP-34: load customer_care_emails CSV into PostgreSQL",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["DHAP-34", "customer_care_emails", "naga_poluri"],
) as dag:

    t1 = PythonOperator(
        task_id="file_check",
        python_callable=task_file_check,
    )

    t2 = PythonOperator(
        task_id="validate_schema",
        python_callable=task_validate_schema,
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    t4 = PythonOperator(
        task_id="create_table",
        python_callable=task_create_table,
    )

    t5 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=task_load_to_postgres,
    )

    t1 >> t2 >> t3 >> t4 >> t5
