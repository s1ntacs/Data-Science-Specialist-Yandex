import os
import sys
import boto3
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# Добавляем текущую папку в пути Python, чтобы импорты работали
sys.path.insert(0, os.path.dirname(__file__))
from calculate_batch_features import build_batch_features

POSTGRES_CONN_ID = "postgres_raw"
S3_CONN_ID = "s3_features"


def get_s3_client_and_bucket():
    connection = BaseHook.get_connection(S3_CONN_ID)
    extras = connection.extra_dejson
    bucket = extras.get("bucket")
    client = boto3.client(
        "s3",
        aws_access_key_id=extras.get("aws_access_key_id"),
        aws_secret_access_key=extras.get("aws_secret_access_key"),
        endpoint_url=extras.get("endpoint_url")
    )
    return client, bucket


def build_and_upload_features(**kwargs):
    run_date = Variable.get("batch_features_run_date")

    # Передаем ID соединения (строку), а не URI
    features = build_batch_features(POSTGRES_CONN_ID, run_date)

    local_path = f"/tmp/batch_features/run_date={run_date}/batch_features.csv"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    features.to_csv(local_path, index=False)

    s3_client, s3_bucket = get_s3_client_and_bucket()
    s3_key = f"run_date={run_date}/batch_features.csv"
    s3_client.upload_file(local_path, s3_bucket, s3_key)

    return {
        "run_date": run_date,
        "rows": len(features),
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
    }


def validate_saved_result(**kwargs):
    ti = kwargs['ti']
    result_info = ti.xcom_pull(task_ids='build_and_upload_features')

    if int(result_info["rows"]) <= 0:
        raise ValueError("Saved feature file is empty")

    s3_client, s3_bucket = get_s3_client_and_bucket()
    s3_key = result_info["s3_key"]

    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
    except Exception as error:
        raise FileNotFoundError(f"S3 object was not found: s3://{s3_bucket}/{s3_key}") from error


with DAG(
        dag_id="batch_features",
        schedule=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["batch-features"],
) as dag:
    task_calculate = PythonOperator(
        task_id="build_and_upload_features",
        python_callable=build_and_upload_features,
    )

    task_validate = PythonOperator(
        task_id="validate_saved_result",
        python_callable=validate_saved_result,
    )

    task_calculate >> task_validate