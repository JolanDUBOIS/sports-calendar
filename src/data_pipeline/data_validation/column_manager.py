import pandas as pd

from . import logger
from .validation_result import ValidationError, ValidationIssue
from src.specs import ColumnSpec


class ColumnManager:
    """ TODO """

    def __init__(self, column_spec: ColumnSpec):
        """ Initialize the column manager with a column specification. """
        self.column_spec = column_spec

    def validate(self, data: pd.DataFrame, model: str) -> None:
        """ Validate the column against the DataFrame. """
        self._validate_exists(data, model)
        self._validate_type(data, model)
        self._validate_unique(data, model)
        self._validate_nullable(data, model)

    def _validate_exists(self, data: pd.DataFrame, model: str) -> None:
        """ Validate that the column exists in the DataFrame. """
        if self.column_spec.name not in data.columns:
            self._handle_issue(
                model=model,
                error_msg=f"Model {model} raised an issue: column {self.column_spec.name} is missing in the DataFrame.",
                constraint="exists"
            )

    def _validate_type(self, data: pd.DataFrame, model: str) -> None:
        """ Validate that the column has the correct type. """
        if self.column_spec.type:
            col = data[self.column_spec.name]
            try:
                if self.column_spec.type == "datetime":
                    invalid_values = col[pd.to_datetime(col, errors="coerce").isnull() & col.notnull()]
                    if not invalid_values.empty:
                        raise ValueError(f"Invalid datetime values detected: {invalid_values.tolist()}")
                else:
                    col.astype(self.column_spec.type)
            except Exception as e:
                self._handle_issue(
                    model=model,
                    error_msg=f"Model {model} raised an issue: column {self.column_spec.name} type is invalid, expected {self.column_spec.type}.",
                    constraint="type"
                )

    def _validate_unique(self, data: pd.DataFrame, model: str) -> None:
        """ Validate that the column values are unique. """
        if self.column_spec.unique:
            col = data[self.column_spec.name]
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

    def _validate_nullable(self, data: pd.DataFrame, model: str) -> None:
        """ Validate that the column does not contain null values if not nullable. """
        if not self.column_spec.nullable:
            col = data[self.column_spec.name]
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