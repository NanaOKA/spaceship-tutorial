"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from tutorial_template.pipelines import (
    data_processing,
    data_science
)

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # pipelines = find_pipelines()
    # pipelines["__default__"] = sum(pipelines.values())
    # return pipelines

    data_science_pipeline = data_science.create_pipeline()
    data_processing_pipeline = data_processing.create_pipeline()

    return {
        "__default__": data_science_pipeline + data_processing_pipeline,
        "data_science_pipeline": data_science_pipeline,
        "data_processing_pipeline": data_processing_pipeline,
    }
