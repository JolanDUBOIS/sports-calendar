from pathlib import Path

from . import logger
from .specs import WorkflowSpec, LayerSpec
from sports_calendar.core import load_yml


def load_workflow(strict: bool = True) -> WorkflowSpec:
    """ Load all layer specifications from the given directory path and return a WorkflowSpec. """
    workflow_dir = Path(__file__).parent / "workflows"
    logger.info(f"Loading workflows from directory: {workflow_dir}")

    layers: list[LayerSpec] = []
    for file_path in workflow_dir.glob("*.yml"):
        if not file_path.is_file():
            logger.warning(f"Skipping non-file path: {file_path}")
            continue
        try:
            config = load_yml(file_path)
            layer_spec = LayerSpec.from_dict(config)
            layers.append(layer_spec)
            logger.debug(f"Loaded workflow from {file_path}")
        except Exception:
            logger.exception(f"Failed to load workflow from {file_path}")
            if strict:
                raise 
    return WorkflowSpec(layers=layers)
