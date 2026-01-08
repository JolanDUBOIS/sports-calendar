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
    """
    Handles the execution lifecycle of a single model based on its specification.

    This class manages the full workflow for a model:
    loading sources, processing data, injecting static fields, and writing the output.
    It respects the model's trigger setting to control whether the model runs automatically
    or only when triggered manually.

    The main method `run()` executes the processing pipeline according to the model spec.
    """

    def __init__(self, model_spec: ModelSpec):
        """ Initialize ModelManager with a model specification. """
        self.model_spec = model_spec

    def run(self, manual: bool = False, dry_run: bool = False, reset: bool = False, **kwargs) -> None:
        """
        Execute the model processing pipeline.

        The process includes:
          - Validating the model's trigger setting (manual or automatic).
          - Loading source data based on the model's source definitions.
          - Processing the data using the specified processing steps.
          - Injecting any static fields defined in the model spec.
          - Writing the processed output and updating source version metadata.

        Args:
            manual (bool): If True, forces processing of models with manual trigger;
                           if False, only processes models set to automatic trigger.
            dry_run (bool): If True, simulates the run without making any changes.
                            Can be used for testing purposes.
            reset (bool): TODO
        """
        if not self._validate_trigger(self.model_spec.trigger, manual):
            return

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

    def _validate_trigger(self, trigger: str, manual: bool) -> bool:
        """ Validate the trigger for the model. """
        if trigger == "manual" and not manual:
            logger.info(f"Model trigger is set to manual, skipping processing for model: {self.model_spec.name}.")
            return False
        return True
