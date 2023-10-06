import logging
from datetime import datetime
from typing import List

import gpxpy
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

s3_hook = S3Hook(aws_conn_id="minio_s3_connection")

bucket_name = 'landing'
current_date = datetime.now()
output_key = f'trips/year={current_date.year}/month={current_date.month}/day={current_date.day}/trips.parquet'


def read_gpx(file: str) -> pd.DataFrame:
    log.info(f'Opening file {file}')
    points = []

    response = s3_hook.get_key(file, 'landing')
    gpx_content = response.get()['Body'].read().decode('utf-8')

    gpx = gpxpy.parse(gpx_content)

    for segment in gpx.tracks[0].segments:
        for i, p in enumerate(segment.points):
            points.append({
                'trip': file,
                'step': i,
                'time': p.time,
                'latitude': p.latitude,
                'longitude': p.longitude,
                'elevation': p.elevation,
            })

    return pd.DataFrame.from_records(points)


@task()
def list_keys() -> List[str]:
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="trips")
    return keys


@task()
def read_files(files_path: List[str]) -> pd.DataFrame:
    df = pd.concat([read_gpx(file) for file in files_path], ignore_index=True)
    df["time"] = pd.to_datetime(df.time).dt.tz_localize(None)
    return df


@task()
def store_to_raw(df: pd.DataFrame):
    df.to_parquet('trips.parquet')

    s3_hook.load_file(
        'trips.parquet',
        output_key,
        'raw',
        True,
        gzip=False
    )


trigger_dag_b = TriggerDagRunOperator(
    task_id='trigger_dag_b',
    trigger_dag_id='feature_engineering',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    conf={
        "bucket": 'raw',
        "key": output_key,
        "url": f's3://raw/{output_key}',
    }
)


@dag(
    dag_id="data_ingestion",
    start_date=datetime.now(),
    schedule=None
)
def data_ingestion():
    data = list_keys()
    merged_dataframe = read_files(data)
    store_to_raw(merged_dataframe) >> trigger_dag_b


data_ingestion()
