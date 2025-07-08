from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from src.datastage import DataStage

if TYPE_CHECKING:
    from src.specs import ModelSpec


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
        self.ignored = set()
        self._build_reverse_deps()

    def __iter__(self) -> ModelOrder:
        """ Return the iterator object itself. """
        return self

    def __next__(self) -> ModelSpec:
        """ Return the next model ready for processing, respecting dependency completion. """
        ready_models = [
            model for model in self.models
            if model.name not in self.completed # Model isn't already completed
            and model.name not in self.failed # Model isn't already failed
            and model.name not in self.ignored # Model isn't ignored
            and self.dependencies[model.name].issubset(self.completed) # All dependencies are completed
        ]
        if not ready_models:
            if self.completed | self.failed | self.ignored == self.model_names:
                if self.ignored:
                    logger.warning(f"Ignored models due to failed dependencies: {self.ignored}")
                raise StopIteration
            else:
                logger.error("Deadlock or unresolved dependencies due to previous failures.")
                logger.debug(f"Missing models: {self.model_names - (self.completed | self.failed)}")
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
        self._ignore_deps(model.name)

    def _build_reverse_deps(self) -> None:
        """ Build a reverse dependency mapping for the models. """
        self.reverse_deps = {model.name: set() for model in self.models}
        for model, deps in self.dependencies.items():
            for dep in deps:
                self.reverse_deps[dep].add(model)

    def _ignore_deps(self, model_name: str) -> None:
        """ Recursively ignore dependencies of a failed model. """
        stack = [model_name]
        while stack:
            current = stack.pop()
            for dependent in self.reverse_deps.get(current, []):
                if dependent not in self.ignored:
                    self.ignored.add(dependent)
                    logger.debug(f"Ignoring model {dependent} due to failure of {model_name}.")
                    stack.append(dependent)

    @staticmethod
    def _get_dependencies(models: list[ModelSpec], stage: str) -> dict[str, set[str]]:
        """ Get the dependencies of the models. """
        stage = DataStage(stage)
        dependencies = {}
        for model in models:
            dependencies[model.name] = set()
            model_dependencies = model.dependencies

            for dep in model_dependencies:
                dep_stage, dep_name = dep.split(".")
                dep_stage = DataStage(dep_stage)

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
