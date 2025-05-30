from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from ..pipeline_stages import DataStage

if TYPE_CHECKING:
    from .managers import ModelSpec


class ModelOrder:
    """
    Manage and iterate over a list of models respecting their dependencies and execution stage.

    This class ensures models are processed in an order where all dependencies of a model
    within the same stage are completed first. It handles tracking of completed and failed models,
    and detects circular dependencies to prevent deadlocks.
    """

    def __init__(self, models: list[ModelSpec], stage: str = "landing"):
        self.models = models
        self.dependencies = self._get_dependencies(models, stage)
        self._check_circular_dependencies(self.dependencies)
        self.model_names = set(model.name for model in models)

        self.completed = set()
        self.failed = set()

    def __iter__(self) -> ModelOrder:
        """ Return the iterator object itself. """
        return self

    def __next__(self) -> ModelSpec:
        """ Return the next model ready for processing, respecting dependency completion. """
        ready_models = [
            model for model in self.models
            if model.name not in self.completed # Model isn't already completed
            and model.name not in self.failed # Model isn't already failed
            and self.dependencies[model.name].issubset(self.completed) # All dependencies are completed
        ]
        if not ready_models:
            if self.completed | self.failed == self.model_names:
                raise StopIteration
            else:
                logger.error("Deadlock or unresolved dependencies due to previous failures.")
                raise ValueError("Deadlock or unresolved dependencies due to previous failures.")
        self.completed.add(ready_models[0].name)
        return ready_models[0]

    def mark_failed(self, model: ModelSpec) -> None:
        """ Mark a model as failed. """
        if model.name not in self.completed:
            logger.error(f"Model {model.name} is not completed, cannot mark as failed.")
            raise ValueError(f"Model {model.name} is not completed, cannot mark as failed.")
        self.completed.remove(model.name)
        self.failed.add(model.name)

    @staticmethod
    def _get_dependencies(models: list[ModelSpec], stage: str) -> dict[str, set[str]]:
        """ Get the dependencies of the models. """
        stage = DataStage.from_str(stage)
        dependencies = {}
        for model in models:
            dependencies[model.name] = set()
            model_dependencies = model.dependencies

            for dep in model_dependencies:
                dep_stage, dep_name = dep.split(".")
                dep_stage = DataStage.from_str(dep_stage)

                if dep_stage > stage:
                    logger.error(f"Model {model.name} has a dependency on a model in a later stage: {dep}")
                    raise ValueError(f"Model {model.name} has a dependency on a model in a later stage: {dep}")
                elif dep_stage == stage:
                    dependencies[model.name].add(dep_name)

        return dependencies

    @staticmethod
    def _check_circular_dependencies(dependencies: dict[str, set[str]]) -> None:
        """ Check for circular dependencies in the models. """
        visited = set()
        stack = set()
    
        def visit(node: str) -> None:
            if node in stack:
                raise ValueError(f"Circular dependency detected involving model: {node}.")
            if node in visited:
                return
            visited.add(node)
            stack.add(node)
            for dep in dependencies.get(node, []):
                if dep not in dependencies:
                    logger.error(f"Model {node} has a dependency on a non-existent model: {dep}")
                    raise ValueError(f"Model {node} has a dependency on a non-existent model: {dep}")
                visit(dep)
            stack.remove(node)
    
        for model in dependencies:
            visit(model)
