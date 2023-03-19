"""
This is a boilerplate pipeline 'data_enrichment'
generated using Kedro 0.18.6
"""
import pandas as pd
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from typing import Callable


def get_geocoder() -> RateLimiter:
    geolocator = Nominatim(user_agent="geoapiExercises")
    return RateLimiter(geolocator.reverse, min_delay_seconds=1)


def get_first_and_last_trip_points(df: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df.groupby('trip', as_index=False).first(), df.groupby('trip', as_index=False).last()])


def add_geographic_attributes(df: pd.DataFrame, geocoder: Callable) -> pd.DataFrame:
    df['lat_lon'] = df['latitude'].astype('string') + ", " + df['longitude'].astype('string')
    df['location'] = df['lat_lon'].apply(geocoder)

    df['neighbourhood'] = df['location'].transform(_get_location_datail('neighbourhood'))
    df['suburb'] = df['location'].transform(_get_location_datail('suburb'))
    df['city'] = df['location'].transform(_get_location_datail('city'))
    df['postcode'] = df['location'].transform(_get_location_datail('postcode'))

    df.drop(['lat_lon', 'location'], axis='columns', inplace=True)

    return df


def _get_location_datail(field: str) -> str:
    return lambda location: location.raw.get('address').get(field)


def merge_trips_with_geographic_attributes(trips_df: pd.DataFrame, geographic_data_df: pd.DataFrame) -> pd.DataFrame:
    return trips_df.merge(geographic_data_df, on=['trip', 'step'], how='left').ffill()
