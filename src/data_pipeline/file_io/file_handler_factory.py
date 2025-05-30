from pathlib import Path

from . import (
    FileHandler,
    CSVHandler,
    JSONHandler,
    TrackedFileHandler,
    logger
)


class FileHandlerFactory:
    """ Factory class to create tracked file handlers. """

    @staticmethod
    def create_file_handler(file_path: Path, tracked: bool = True) -> FileHandler | TrackedFileHandler:
        """ Create a tracked file handler based on the file extension. """
        if file_path.suffix == ".csv":
            handler = CSVHandler(file_path)
            if tracked:
                return TrackedFileHandler(handler)
            return handler
        elif file_path.suffix == ".json":
            handler = JSONHandler(file_path)
            if tracked:
                return TrackedFileHandler(handler)
            return handler
        else:
            logger.error(f"Unsupported file type: {file_path.suffix}")
            raise ValueError(f"Unsupported file type: {file_path.suffix}")
