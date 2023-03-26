from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    get_elapsed_time,
    get_distance,
    get_weekday_and_hour,
    merge_features
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=get_elapsed_time,
            inputs=['enriched_df'],
            outputs='elapsed_time_df',
            name='get_elapsed_time'
        ),
        node(
            func=get_distance,
            inputs=['enriched_df'],
            outputs='distance_df',
            name='get_distance'
        ),
        node(
            func=get_weekday_and_hour,
            inputs=['enriched_df'],
            outputs='weekday_df',
            name='get_weekday_and_hour'
        ),
        node(
            func=merge_features,
            inputs=['enriched_df', 'elapsed_time_df', 'distance_df', 'weekday_df'],
            outputs='feature_df',
            name='merge_features'
        ),
    ])
