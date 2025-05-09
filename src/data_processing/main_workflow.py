from pathlib import Path

from .ingestion import ingestion_workflow
from .extraction import extraction_workflow

from .utils import read_yml_file
from src.data_processing import logger


ORDER_OF_OPERATIONS = {
    "data_ingestion": ingestion_workflow,
    "data_extraction": extraction_workflow
}

def data_processing_workflow(db_repo: str, instructions_path: str|Path, workflows: list[str] = None, manual: bool = False):
    """ TODO """
    logger.info("Starting data processing workflow...")
    instructions_path = Path(instructions_path) if isinstance(instructions_path, str) else instructions_path
    if instructions_path.is_dir():
        instructions_path = list(instructions_path.glob("*.yml"))
    else:
        logger.error(f"The instructions path must be a directory: {instructions_path}")
        raise ValueError(f"The instructions path must be a directory: {instructions_path}")

    data_processing_instructions = {}
    for path in instructions_path:
        instructions = read_yml_file(path)
        data_processing_instructions[instructions.get('name')] = instructions

    if workflows:
        operations = {key: op_workflow for key, op_workflow in ORDER_OF_OPERATIONS.items() if key in workflows}
    else:
        operations = ORDER_OF_OPERATIONS
    for operation_name, operation_workflow in operations.items():
        if operation_name not in data_processing_instructions:
            logger.error(f"Operation {operation_name} is missing in the instructions.")
            raise ValueError(f"Operation {operation_name} is missing in the instructions.")
        logger.info(f"Processing: {operation_name}")
        instructions = data_processing_instructions[operation_name]
        operation_workflow(db_repo, instructions, manual=manual)

    logger.info("Data processing workflow completed.")


if __name__ == "__main__":
    logger.info("Running data processing workflow...")
    from src.config import get_config
    db_repo = get_config("pipeline.repository_path")
    instructions_path = get_config("pipeline.config_files")
    workflows = get_config("pipeline.workflows")
    manual = get_config("pipeline.manual")
    data_processing_workflow(db_repo, instructions_path, workflows=workflows, manual=manual)
    logger.info("Data processing workflow completed.")
