from __future__ import annotations
from typing import Any
from pathlib import Path
from dataclasses import dataclass

from . import logger
from .utils import inject_static_fields
from .sources import SourcesManager, SourceSpec
from .output import OutputManager, OutputSpec
from .processing import ProcessingManager, ProcessingSpec


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
    """ TODO """

    def __init__(self, model_spec: ModelSpec, repo_path: str | Path):
        """ TODO """
        self.repo_path = Path(repo_path)
        self.model_spec = self._resolve_paths(model_spec)

    def _resolve_paths(self, spec: ModelSpec) -> ModelSpec:
        """ Resolve paths in the model specification. """
        for source in spec.sources:
            source.path = self.repo_path / source.path
        spec.output.path = self.repo_path / spec.output.path
        return spec

    def run(self, manual: bool = False) -> None:
        """ Run the model processing. """
        if not self._validate_trigger(self.model_spec.trigger, manual):
            return

        logger.info(f"Running model: {self.model_spec.name}")
        
        # Managers
        source_manager = SourcesManager(self.model_spec.sources)
        output_manager = OutputManager(self.model_spec.output)
        processing_manager = ProcessingManager(self.model_spec.processing)
        
        # Load sources
        source_versions = output_manager.read_source_versions()
        loaded_sources = source_manager.get_loaded_sources(source_versions=source_versions)

        # Process data
        processed_data = processing_manager.process(sources=loaded_sources)

        # Inject static fields
        data = inject_static_fields(processed_data, self.model_spec.static_fields)

        # Write output
        new_source_versions = source_manager.get_new_source_versions()
        output_manager.write(data, new_source_versions)
        
        logger.info(f"Model {self.model_spec.name} processed successfully.")

    @staticmethod
    def _validate_trigger(trigger: str, manual: bool) -> bool:
        """ Validate the trigger for the model. """
        if trigger == "manual" and not manual:
            logger.info("Model trigger is set to manual, skipping processing.")
            return False
        elif trigger == "automatic" and manual:
            logger.info("Model trigger is set to automatic, processing will continue.")
        return True
