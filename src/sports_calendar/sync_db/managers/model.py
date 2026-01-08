from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .utils import inject_static_fields
from .sources import SourcesManager
from .output import OutputManager
from .processing import ProcessingManager

if TYPE_CHECKING:
    from sports_calendar.sync_db.definitions.specs import ModelSpec


class ModelManager:
    """ TODO """

    def __init__(self, model_spec: ModelSpec):
        """ Initialize ModelManager with a model specification. """
        self.model_spec = model_spec

    def run(self, dry_run: bool = False, reset: bool = False, **kwargs) -> None:
        """ TODO """
        logger.info(f"Running model: {self.model_spec.name}")
        
        # Managers
        source_manager = SourcesManager(self.model_spec.sources)
        output_manager = OutputManager(self.model_spec.output)
        processing_manager = ProcessingManager(self.model_spec.processing)

        # Reset if specified
        if reset:
            output_manager.reset()

        # Load sources
        source_versions = output_manager.read_source_versions()
        loaded_sources = source_manager.get_loaded_sources(source_versions=source_versions)

        # Process data
        processed_data = processing_manager.process(sources=loaded_sources)
        output_io_content = processed_data.get(self.model_spec.output.name)

        # Inject static fields
        data = inject_static_fields(output_io_content, self.model_spec.static_fields)

        # Write output
        new_source_versions = source_manager.get_new_source_versions()
        if not dry_run:
            output_manager.write(data, new_source_versions)
        else:
            logger.info("Dry run mode enabled, output will not be written.")
        
        logger.info(f"Model {self.model_spec.name} processed successfully.")
