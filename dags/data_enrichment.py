from typing import Callable, Any

import pandas as pd
from airflow.decorators import task
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="geoapiExercises")
geolocator = RateLimiter(geolocator.reverse, min_delay_seconds=1.2)


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