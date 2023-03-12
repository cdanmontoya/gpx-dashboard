"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 0.18.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import get_files_path, read_files


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=get_files_path,
            inputs=[],
            outputs="files_path",
            name='get-files-path'
        ),
        node(
            func=read_files,
            inputs=['files_path'],
            outputs="df",
            name='read_files'
        )
    ])
