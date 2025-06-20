from src.config import DataStage
from src.config.manager import base_config


def clean_repository(stage: DataStage | None = None) -> None:
    """ Clean the repository by removing all files and metadata for the specified stage. """
    repo_path = base_config.active_repo.path
    stages = [stage] if stage is not None else DataStage.instances()
    for _stage in stages:
        stage_path = repo_path / _stage.name.lower()
        