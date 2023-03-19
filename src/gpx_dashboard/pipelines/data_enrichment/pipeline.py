"""
This is a boilerplate pipeline 'data_enrichment'
generated using Kedro 0.18.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    get_geocoder,
    get_first_and_last_trip_points,
    add_geographic_attributes,
    merge_trips_with_geographic_attributes
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=get_first_and_last_trip_points,
            inputs=['merged_dataframe'],
            outputs='first_last_df',
            name='get_first_and_last_trip_points'
        ),
        node(
            func=get_geocoder,
            inputs=[],
            outputs="geocoder",
            name='get_geocoder'
        ),
        node(
            func=add_geographic_attributes,
            inputs=['first_last_df', 'geocoder'],
            outputs="geographic_data_df",
            name='add_geolocation_attr'
        ),
        node(
            func=merge_trips_with_geographic_attributes,
            inputs=['merged_dataframe', 'geographic_data_df'],
            outputs="enriched_df",
            name='enrich_df'
        )
    ])
