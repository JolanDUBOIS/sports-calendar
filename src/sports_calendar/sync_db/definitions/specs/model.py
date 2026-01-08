from typing import Any
from pathlib import Path
from dataclasses import dataclass, field

from .source import SourceSpec
from .output import OutputSpec
from .processing import ProcessingStepSpec
from sports_calendar.sc_core import SpecModel


@dataclass
class ModelSpec(SpecModel):
    name: str
    output: OutputSpec
    processing: list[ProcessingStepSpec]
    sources: list[SourceSpec] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    description: str | None = None
    static_fields: list[dict[str, Any]] | None = None

    def resolve_paths(self, base_path: Path | str) -> None:
        """ Resolve the paths of all sources and outputs relative to a base path. """
        for source in self.sources:
            source.resolve_path(base_path)
        self.output.resolve_path(base_path)
