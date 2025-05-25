from pathlib import Path

from src import logger
from src.data_pipeline.utils import read_yml_file
from src.data_pipeline.data_processing import build_layer
from src.data_pipeline.data_validation import validate_stage_schema


def test(no: int):
    """ TODO """
    logger.info("Running test...")
    if no == 1:
        logger.info("Running build_landing test...")
        db_repo = Path("data/repository/test")
        workflow_instructions = read_yml_file(Path("config/pipeline_config/test/workflows/build_landing.yml"))
        build_layer(db_repo, workflow_instructions)

    elif no == 2:
        logger.info("Running build_intermediate test...")
        db_repo = Path("data/repository/test")
        intermediate_dir = db_repo / "intermediate"
        for file in intermediate_dir.glob("*"):
            if file.is_file():
                file.unlink()
        workflow_instructions = read_yml_file(Path("config/pipeline_config/test/workflows/build_intermediate.yml"))
        build_layer(db_repo, workflow_instructions)

    elif no == 3:
        logger.info("Running the true build_landing...")
        db_repo = Path("data/repository")
        workflow_instructions = read_yml_file(Path("config/pipeline_config/daily/workflows/build_landing.yml"))
        build_layer(db_repo, workflow_instructions)
    
        # Temporary
        landing_dir = db_repo / "landing"
        test_landing_dir = Path("data/repository/test/landing")
        for file in test_landing_dir.glob("*"):
            if file.is_file():
                file.unlink()
        for file in landing_dir.glob("*"):
            if file.is_file():
                file.copy(test_landing_dir / file.name)

    elif no == 4:
        # logger.info("Running data validation...")
        db_repo = Path("data/repository/test")
        schema_file_path = Path("config/pipeline_config/test/schemas/intermediate.yml")
        validate_stage_schema(db_repo, schema_file_path, log_summary=True)
