from __future__ import annotations
from typing import Any
from pathlib import Path
from dataclasses import dataclass

from . import logger
from .utils import inject_static_fields
from .sources import SourcesManager, SourceSpec
from .output import OutputManager, OutputSpec
from .processing import ProcessingManager, ProcessingSpec
from src.config.manager import base_config


@dataclass
class ModelSpec:
    """ Describes the full model (name, sources, output, processing). """
    name: str
    trigger: str
    sources: list[SourceSpec]  # Can be empty
    dependencies: list[str]  # Can be empty
    output: OutputSpec
    processing: ProcessingSpec
    description: str | None = None
    static_fields: list[dict[str, Any]] | None = None

    def __post_init__(self):
        """ Validate the model specification. """
        if not self.name:
            logger.error("Model name cannot be empty.")
            raise ValueError("Model name cannot be empty.")
        if not self.trigger:
            logger.error("Model trigger cannot be empty.")
            raise ValueError("Model trigger cannot be empty.")
        if not self.output:
            logger.error("Model must have an output specification.")
            raise ValueError("Model must have an output specification.")
        if not self.processing:
            logger.error("Model must have a processing specification.")
            raise ValueError("Model must have a processing specification.")

        valid_triggers = ["automatic", "manual"]
        if not self.trigger in valid_triggers:
            logger.error(f"Invalid trigger '{self.trigger}'. Valid triggers are: {valid_triggers}.")
            raise ValueError(f"Invalid trigger '{self.trigger}'. Valid triggers are: {valid_triggers}.")

        self._resolve_paths(base_config.active_repo.path)

    def _resolve_paths(self, base_path: Path) -> None:
        """ Resolve model paths relative to the base path. """
        for source in self.sources:
            source.path = base_path / source.path
        self.output.path = base_path / self.output.path

    @classmethod
    def from_dict(cls, d: dict) -> ModelSpec:
        """ Create a ModelSpec from a dictionary. """
        return cls(
            name=d["name"],
            trigger=d["trigger"],
            sources=[SourceSpec.from_dict(s) for s in d.get("sources", [])],
            dependencies=d.get("dependencies", []),
            output=OutputSpec.from_dict(d["output"]),
            processing=ProcessingSpec.from_dict(d["processing"]),
            description=d.get("description"),
            static_fields=d.get("static_fields", [])
        )

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

        # Inject static fields
        data = inject_static_fields(processed_data, self.model_spec.static_fields)

        # Write output
        new_source_versions = source_manager.get_new_source_versions()
        if not dry_run:
            output_manager.write(data, new_source_versions)
        else:
            logger.info("Dry run mode enabled, output will not be written.")
        
        logger.info(f"Model {self.model_spec.name} processed successfully.")

    @staticmethod
    def _validate_trigger(trigger: str, manual: bool) -> bool:
        """ Validate the trigger for the model. """
        if trigger == "manual" and not manual:
            logger.info("Model trigger is set to manual, skipping processing.")
            return False
        return True
