"""
This is a boilerplate pipeline 'data_enrichment'
generated using Kedro 0.18.6
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import add_geographic_attributes


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=add_geographic_attributes,
            inputs=['merged_dataframe'],
            outputs="geo_df",
            name='add_geolocation_attr'
        )
    ])
