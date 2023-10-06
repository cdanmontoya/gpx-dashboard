import io
import math
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from geopy import distance

s3_hook = S3Hook(aws_conn_id="minio_s3_connection")


@task()
def read_dataframe(**kwargs):
    bucket = kwargs["dag_run"].conf["bucket"]
    key = kwargs["dag_run"].conf["key"]

    s3_object = s3_hook.get_key(key, bucket)
    df = pd.read_parquet(io.BytesIO(s3_object.get()['Body'].read()))

    return df


@task()
def get_elapsed_time(df: pd.DataFrame) -> pd.DataFrame:
    df['time'] = pd.to_datetime(df.time)

    df = ((df.groupby('trip').last().time - df.groupby('trip').first().time)
          .to_frame()
          .reset_index()
          .rename(columns={'time': 'elapsed_time'}))

    df['elapsed_time'] = df.elapsed_time.apply(lambda x: round(x.total_seconds() / 60))

    return df


@task()
def get_weekday_and_hour(df: pd.DataFrame) -> pd.DataFrame:
    df['time'] = pd.to_datetime(df.time)
    df['weekday'] = df.time.apply(lambda x: x.strftime("%A"))
    df['hour'] = df.time.apply(lambda x: x.hour)

    return df[['trip', 'step', 'weekday', 'hour']]


@task()
def get_distance(df: pd.DataFrame) -> pd.DataFrame:
    df[['previous_latitude', 'previous_longitude', 'previous_elevation']] = df.groupby('trip')[
        ['latitude', 'longitude', 'elevation']].shift()

    df['distance'] = df.apply(_lambda_partial_distance(), axis=1)

    return (df.groupby('trip')['distance']
            .sum()
            .to_frame()
            .reset_index())


def _lambda_partial_distance():
    return lambda x: _get_partial_distance(
        (x['latitude'], x['longitude'], x['elevation']),
        (x['previous_latitude'], x['previous_longitude'],
         x['previous_elevation']))


def _get_partial_distance(actual_position: Tuple[float, float, float],
                          previous_position: Tuple[float, float, float]) -> float:
    if math.isnan(previous_position[0]):
        return 0

    flat_distance = distance.distance(previous_position[:2], actual_position[:2]).km

    return flat_distance


@task()
def merge_features(enriched_df: pd.DataFrame,
                   elapsed_time_df: pd.DataFrame,
                   distance_df: pd.DataFrame,
                   weekday_df: pd.DataFrame
                   ) -> pd.DataFrame:
    df = (enriched_df.merge(elapsed_time_df, on=['trip'], how='inner')
          .merge(distance_df, on=['trip'], how='inner')
          .merge(weekday_df, on=['trip', 'step'], how='left')
          )

    df['average_speed'] = df.distance / (df.elapsed_time / 60)
    df.replace([np.inf], 0, inplace=True)

    return df


@task()
def store_to_stage(df: pd.DataFrame):
    df.to_parquet('features.parquet')

    current_date = datetime.now()

    s3_hook.load_file(
        'features.parquet',
        f'year={current_date.year}/month={current_date.month}/day={current_date.day}/features.parquet',
        'stage',
        True,
        gzip=False
    )


@dag(
    dag_id="feature_engineering",
    start_date=datetime.now(),
    schedule=None
)
def feature_engineering():
    data = read_dataframe()
    elapsed_time_df = get_elapsed_time(data)
    distance_df = get_distance(data)
    weekday_df = get_weekday_and_hour(data)
    feature_df = merge_features(data, elapsed_time_df, distance_df, weekday_df)
    store_to_stage(feature_df)


feature_engineering()
