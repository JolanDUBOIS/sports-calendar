from pathlib import Path

from .enforce_schema import enforce_schema
from src.data_pipeline.utils import read_yml_file


def validate_stage_schema(db_repo: str, schema_file_path: str) -> None:
    """ Validate one schema file corresponding to one stage. """
    db_repo = Path(db_repo)
    schema_file_path = Path(schema_file_path)
    schemas = read_yml_file(schema_file_path)
    stage = schemas.get("stage")
    
    for model_schema in schemas.get("models", []):
        enforce_schema(db_repo, model_schema, stage)
