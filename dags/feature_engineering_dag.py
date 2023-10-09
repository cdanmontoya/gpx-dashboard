import io
import math
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from geopy import distance
from sqlalchemy import create_engine

s3_hook = S3Hook(aws_conn_id="minio_s3_connection")
engine = create_engine('postgresql://postgres:mysecretpassword@db:5432/gpx-db')


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
def store_to_datalake(stage: str, name: str, df: pd.DataFrame):
    df.to_parquet(f'{name}.parquet')

    current_date = datetime.now()

    s3_hook.load_file(
        f'{name}.parquet',
        f'year={current_date.year}/month={current_date.month}/day={current_date.day}/{name}.parquet',
        stage,
        True,
        gzip=False
    )


@task()
def store_to_db(name: str, df: pd.DataFrame):
    df.to_sql(name, engine, if_exists='replace')


@task()
def compact_data(df: pd.DataFrame) -> pd.DataFrame:
    origin_rename = {
        'time': 'origin_time',
        'country': 'origin_country',
        'state': 'origin_state',
        'city': 'origin_city',
        'suburb': 'origin_suburb',
        'neighbourhood': 'origin_neighbourhood',
        'postcode': 'origin_postcode',
        'latitude': 'origin_latitude',
        'longitude': 'origin_longitude',
        'elevation': 'origin_elevation',
        'weekday': 'origin_weekday',
        'hour': 'origin_hour',
    }

    destination_rename = {
        'time': 'destination_time',
        'country': 'destination_country',
        'state': 'destination_state',
        'city': 'destination_city',
        'suburb': 'destination_suburb',
        'neighbourhood': 'destination_neighbourhood',
        'postcode': 'destination_postcode',
        'latitude': 'destination_latitude',
        'longitude': 'destination_longitude',
        'elevation': 'destination_elevation',
        'weekday': 'destination_weekday',
        'hour': 'destination_hour',
    }

    origin = (df.groupby('trip', as_index=False)
              .first()
              .rename(columns=origin_rename)
              .drop(columns=['step', 'elapsed_time', 'distance', 'average_speed']))

    destination = (df.groupby('trip', as_index=False)
                   .tail(1)
                   .rename(columns=destination_rename)
                   .drop(columns=['step', 'elapsed_time', 'distance', 'average_speed', ]))

    return (df.merge(origin, on=['trip'], how='inner')
            .merge(destination, on=['trip'], how='inner')
            .drop(columns=list(origin_rename.keys()))
            .drop_duplicates(subset=['trip']))


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
    store_to_datalake('stage', 'features', feature_df)
    store_to_db('feature_data', feature_df)


feature_engineering()
