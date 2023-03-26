"""
This is a boilerplate pipeline 'data_modeling'
generated using Kedro 0.18.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import compact_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=compact_data,
            inputs=['feature_df'],
            outputs='compact_df',
            name='compact-data'
        ),
    ])
