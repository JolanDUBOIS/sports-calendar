from pathlib import Path

from . import logger
from .utils import order_models
from .process_model import process_model


def build_layer(db_repo: str, instructions: dict, manual: bool = False):
    """ TODO """
    layer = instructions.get("stage")
    if not layer:
        logger.error("Stage is not specified in instructions.")
        raise ValueError("Stage is not specified in instructions.")
    logger.info(f"Building layer {layer}...")
    db_repo = Path(db_repo)

    models = instructions.get("models", [])
    models = order_models(models, layer)

    for model in models:
        process_model(
            db_repo=db_repo,
            model=model,
            manual=manual
        )
