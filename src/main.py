from pathlib import Path

from src import logger
from .data_pipeline import LayerBuilder, LayerSchemaManager


def test(no: int):
    """ TODO """
    logger.info("Running test...")
    root = Path(__file__).parent.parent
    repo_path = root / "data" / "repository" / "test"
    if no == 1:
        workflow_yml_path = root / "config" / "pipeline_config" / "test" / "workflows" / "build_landing.yml"
        layer_builder = LayerBuilder.from_yaml(workflow_yml_path, repo_path)
        layer_builder.build(manual=True)

    elif no == 2:
        workflow_yml_path = root / "config" / "pipeline_config" / "test" / "workflows" / "build_intermediate.yml"
        layer_builder = LayerBuilder.from_yaml(workflow_yml_path, repo_path)
        layer_builder.build(manual=True)

    elif no == 3:
        schema_yml_path = root / "config" / "pipeline_config" / "test" / "schemas" / "intermediate.yml"
        layer_schema_manager = LayerSchemaManager.from_yaml(schema_yml_path, repo_path)
        result = layer_schema_manager.validate(raise_on_error=False)
        logger.debug(f"Schema validation result: {result}")
        