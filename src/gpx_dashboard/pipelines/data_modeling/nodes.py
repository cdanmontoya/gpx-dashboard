"""
This is a boilerplate pipeline 'data_modeling'
generated using Kedro 0.18.6
"""
import pandas as pd


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
