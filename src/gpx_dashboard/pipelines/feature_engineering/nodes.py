import math
from typing import Tuple

import pandas as pd
from geopy import distance


def get_elapsed_time(df: pd.DataFrame) -> pd.DataFrame:
    df['time'] = pd.to_datetime(df.time)
    return ((df.groupby('trip', ).last().time - df.groupby('trip', ).first().time)
            .to_frame()
            .reset_index()
            .rename(columns={'time': 'elapsed_time'}))


def get_distance(df: pd.DataFrame) -> pd.DataFrame:
    df[['previous_latitude', 'previous_longitude', 'previous_elevation']] = df.groupby('trip')[
        ['latitude', 'longitude', 'elevation']].shift()

    # TODO: Vectorize this call to improve performance
    df['distance'] = df.apply(_lambda_partial_distance(), axis=1)

    return (df.groupby('trip')['distance']
            .sum()
            .to_frame()
            .reset_index())


def _lambda_partial_distance():
    return lambda x: _get_partial_distance((x['latitude'], x['longitude'], x['elevation']),
                                           (x['previous_latitude'], x['previous_longitude'], x['previous_elevation']))


def _get_partial_distance(actual_position: Tuple[float, float, float],
                          previous_position: Tuple[float, float, float]) -> float:
    if math.isnan(previous_position[0]):
        return 0

    flat_distance = distance.distance(previous_position[:2], actual_position[:2]).km
    euclidian_distance = math.sqrt(flat_distance ** 2 + (actual_position[2] - previous_position[2]) ** 2)

    return euclidian_distance


def merge_features(enriched_df: pd.DataFrame, elapsed_time_df: pd.DataFrame, distance_df: pd.DataFrame) -> pd.DataFrame:
    return (enriched_df.merge(elapsed_time_df, on=['trip'], how='inner')
            .merge(distance_df, on=['trip'], how='inner'))
