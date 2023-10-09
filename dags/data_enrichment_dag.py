import io
from datetime import datetime
from typing import Callable, Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="geoapiExercises")
geolocator = RateLimiter(geolocator.reverse, min_delay_seconds=1.2)
s3_hook = S3Hook(aws_conn_id="minio_s3_connection")

current_date = datetime.now()
output_key = f'trips/year={current_date.year}/month={current_date.month}/day={current_date.day}/trips.parquet'


@task()
def read_dataframe(**kwargs):
    bucket = kwargs["dag_run"].conf["bucket"]
    key = kwargs["dag_run"].conf["key"]

    s3_object = s3_hook.get_key(key, bucket)
    df = pd.read_parquet(io.BytesIO(s3_object.get()['Body'].read()))

    return df


@task()
def get_first_and_last_trip_points(df: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df.groupby('trip', as_index=False).first(), df.groupby('trip', as_index=False).tail(1)])


@task()
def add_geographic_attributes(df: pd.DataFrame, geocoder: Callable) -> pd.DataFrame:
    df['lat_lon'] = df['latitude'].astype('string') + ", " + df['longitude'].astype('string')
    df['location'] = df['lat_lon'].apply(geocoder)

    df['country'] = df['location'].transform(_get_location_detail('country'))
    df['state'] = df['location'].transform(_get_location_detail('state'))
    df['city'] = df['location'].transform(_get_city())
    df['suburb'] = df['location'].transform(_get_location_detail('suburb'))
    df['neighbourhood'] = df['location'].transform(_get_location_detail('neighbourhood'))
    df['postcode'] = df['location'].transform(_get_location_detail('postcode'))

    df.drop(['lat_lon', 'location'], axis='columns', inplace=True)

    return df


@task()
def _get_location_detail(field: str) -> Callable[[Any], Any]:
    return lambda location: location.raw.get('address').get(field)


@task()
def _get_city() -> Callable[[Any], Any]:
    return lambda location: location.raw.get('address').get('city') if location.raw.get('address').get(
        'city') is not None else location.raw.get('address').get('town')


@task()
def merge_trips_with_geographic_attributes(trips_df: pd.DataFrame, geographic_data_df: pd.DataFrame) -> pd.DataFrame:
    columns_to_rename = {
        "time_x": "time",
        "latitude_x": "latitude",
        "longitude_x": "longitude",
        "elevation_x": "elevation",
    }

    columns_to_drop = [
        'time_y',
        'latitude_y',
        'longitude_y',
        'elevation_y'
    ]

    return (trips_df.merge(geographic_data_df, on=['trip', 'step'], how='left')
            .rename(columns=columns_to_rename)
            .drop(columns_to_drop, axis='columns'))


@task()
def store_enriched_df_to_stage(df: pd.DataFrame):
    df.to_parquet('enriched.parquet')

    s3_hook.load_file(
        'enriched.parquet',
        f'year={current_date.year}/month={current_date.month}/day={current_date.day}/enriched.parquet',
        'stage',
        True,
        gzip=False
    )


trigger_feature_engineering = TriggerDagRunOperator(
    task_id='trigger_feature_engineering',
    trigger_dag_id='feature_engineering',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    conf={
        "bucket": 'stage',
        "key": output_key,
        "url": f's3://stage/{output_key}',
    }
)


@dag(
    dag_id="data_enrichment",
    start_date=datetime.now(),
    schedule=None
)
def data_enrichment():
    data = read_dataframe()

    first_last_df = get_first_and_last_trip_points(data)
    geographic_data_df = add_geographic_attributes(first_last_df, geocoder=geolocator)
    enriched_df = merge_trips_with_geographic_attributes(data, geographic_data_df)
    store_enriched_df_to_stage(enriched_df) >> trigger_feature_engineering


data_enrichment()
