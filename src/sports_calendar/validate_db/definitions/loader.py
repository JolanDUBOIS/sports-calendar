from pathlib import Path

from . import logger
from .specs import SchemaSpec, LayerSchemaSpec
from sports_calendar.sc_core import load_yml


def load_schema(strict: bool = True) -> SchemaSpec:
    """ Load all layer specifications from the given directory path and return a SchemaSpec. """
    schema_dir = Path(__file__).parent / "schemas"
    logger.info(f"Loading schemas from directory: {schema_dir}")

    layers: list[LayerSchemaSpec] = []
    for file_path in schema_dir.glob("*.yml"):
        if not file_path.is_file():
            logger.warning(f"Skipping non-file path: {file_path}")
            continue
        try:
            config = load_yml(file_path)
            layer_spec = LayerSchemaSpec.from_dict(config)
            layers.append(layer_spec)
            logger.debug(f"Loaded schema from {file_path}")
        except Exception:
            logger.exception(f"Failed to load schema from {file_path}")
            if strict:
                raise 
    return SchemaSpec(layers=layers)
