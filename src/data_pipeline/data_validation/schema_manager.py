from __future__ import annotations
from pathlib import Path

import pandas as pd

from . import logger
from .schema_spec import SchemaSpec, ModelSchemaSpec, ColumnSpec
from .validation_result import SchemaValidationResult, ModelValidationResult, ValidationError, ValidationIssue
from ..file_io import FileHandlerFactory
from ..utils import read_yml_file


class ColumnManager:
    """ TODO """

    def __init__(self, column_spec: ColumnSpec):
        """ Initialize the column manager with a column specification. """
        self.column_spec = column_spec

    def validate(self, data: pd.DataFrame, model: str) -> None:
        """ Validate the column against the DataFrame. """
        if self.column_spec.name not in data.columns:
            self._handle_issue(
                model=model,
                error_msg=f"Model {model} raised an issue: column {self.column_spec.name} is missing in the DataFrame.",
                constraint="exists"
            )

        col = data[self.column_spec.name]
        if self.column_spec.type:
            try:
                col = col.astype(self.column_spec.type)
            except Exception as e:
                self._handle_issue(
                    model=model,
                    error_msg=f"Model {model} raised an issue: column {self.column_spec.name} type is invalid, expected {self.column_spec.type}.",
                    constraint="type"
                )

        if self.column_spec.unique:
            if not col.is_unique:
                duplicates = col[col.duplicated()].unique()
                self._handle_issue(
                    model=model,
                    error_msg=(
                        f"Model {model} raised an issue: column {self.column_spec.name} is not unique. "
                        f"First duplicate value: {duplicates[0]}."
                    ),
                    constraint="unique"
                )

        if not self.column_spec.nullable:
            if col.isnull().any():
                self._handle_issue(
                    model=model,
                    error_msg=f"Model {model} raised an issue: column {self.column_spec.name} has null values.",
                    constraint="nullable"
                )
    
    def _handle_issue(self, model: str, error_msg: str, constraint: str) -> None:
        """ Handle validation issues. """
        issue = ValidationIssue(
            model=model,
            column=self.column_spec.name,
            error=error_msg,
            constraint=constraint
        )
        logger.error(issue.error)
        raise ValidationError(issue)

class ModelSchemaManager:
    """ TODO """

    def __init__(self, schema_spec: ModelSchemaSpec):
        """ TODO """
        self.schema_spec = schema_spec
        self.file_handler = FileHandlerFactory.create_file_handler(schema_spec.path)

    def validate(self, raise_on_error: bool = False) -> ModelValidationResult:
        """ TODO """
        model_result = ModelValidationResult(model=self.schema_spec.name)

        try:
            data = self.file_handler.read()
        except Exception as e:
            self._handle_fatal_issue(
                model_result,
                error_msg=f"Failed to read data for model {self.schema_spec.name}: {e}",
                raise_on_error=raise_on_error
            )
            return model_result
        if not isinstance(data, pd.DataFrame):
            self._handle_fatal_issue(
                model_result,
                error_msg=f"Data for model {self.schema_spec.name} must be a pandas DataFrame, got {type(data).__name__}",
                raise_on_error=raise_on_error
            )
            return model_result
        if data.empty:
            self._handle_fatal_issue(
                model_result,
                error_msg=f"Data for model {self.schema_spec.name} is empty.",
                raise_on_error=raise_on_error
            )
            return model_result
        for col_spec in self.schema_spec.columns:
            try:
                ColumnManager(col_spec).validate(data, self.schema_spec.name)
            except ValidationError as e:
                if raise_on_error:
                    raise e
                else:
                    model_result.append(e.issue)
        
        if len(model_result) == 0:
            logger.info(f"Model {self.schema_spec.name} passed validation.")
        return model_result

    def _handle_fatal_issue(
        self,
        result: ModelValidationResult,
        error_msg: str,
        raise_on_error: bool
    ) -> None:
        """ Handle validation errors. """
        issue = ValidationIssue(
            model=self.schema_spec.name,
            error=error_msg
        )
        logger.error(issue.error)
        if raise_on_error:
            raise ValidationError(issue)
        result.append(issue)

class LayerSchemaManager:
    """ TODO """

    def __init__(self, schema_spec: SchemaSpec, repo_path: str | Path):
        """ TODO """
        self.repo_path = Path(repo_path)
        self.schema_spec = self._resolve_paths(schema_spec)

    def _resolve_paths(self, schema_spec: SchemaSpec) -> SchemaSpec:
        """ TODO """
        for model in schema_spec.models:
            model.path = self.repo_path / schema_spec.stage / model.path
        return schema_spec

    def validate(self, raise_on_error: bool = False) -> SchemaValidationResult:
        """ TODO """
        logger.info(f"Validating schema for stage: {self.schema_spec.stage}")
        schema_result = SchemaValidationResult(schema=self.schema_spec.name)
        for model_spec in self.schema_spec.models:
            model_manager = ModelSchemaManager(model_spec)
            schema_result.append(model_manager.validate(raise_on_error=raise_on_error))
        return schema_result

    @classmethod
    def from_dict(cls, d: dict, repo_path: str | Path) -> LayerSchemaManager:
        """ Create a LayerSchemaManager from a dictionary. """
        schema_spec = SchemaSpec.from_dict(d)
        return cls(schema_spec=schema_spec, repo_path=repo_path)

    @classmethod
    def from_yaml(cls, yaml_path: str | Path, repo_path: str | Path) -> LayerSchemaManager:
        """ Create a LayerSchemaManager from a YAML file. """
        layer_schema = read_yml_file(Path(yaml_path))
        return cls.from_dict(layer_schema, repo_path)
