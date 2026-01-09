from dataclasses import dataclass, field

from . import logger


@dataclass
class ValidationIssue:
    model: str
    error: str
    column: str | None = None
    constraint: str | None = None  # e.g. "type", "unique", "nullable", "exists"

    def __post_init__(self):
        """ Validate the validation issue. """
        if not self.model:
            logger.error("Model name cannot be empty.")
            raise ValueError("Model name cannot be empty.")
        if not self.error:
            logger.error("Error message cannot be empty.")
            raise ValueError("Error message cannot be empty.")
        if self.column and not isinstance(self.column, str):
            logger.error("Column must be a string or None.")
            raise TypeError("Column must be a string or None.")
        valid_constraints = ["type", "unique", "nullable", "exists"]  # TODO - Defined higher up in the codebase
        if self.constraint and self.constraint not in valid_constraints:
            logger.error(f"Invalid constraint '{self.constraint}'. Valid constraints are: {valid_constraints}.")
            raise ValueError(f"Invalid constraint '{self.constraint}'. Valid constraints are: {valid_constraints}.")

class ValidationError(Exception):
    """ TODO """

    def __init__(self, issue: ValidationIssue):
        """ Initialize the validation error with validation issue. """
        super().__init__(issue.error)
        self.issue = issue

@dataclass
class ModelValidationResult:
    model: str
    passed: bool = True
    issues: list[ValidationIssue] = field(default_factory=list)

    def __len__(self) -> int:
        """ Return the number of issues in the validation result. """
        return len(self.issues)

    def append(self, issue: ValidationIssue) -> None:
        """ TODO """
        self.passed = False
        self.issues.append(issue)

    def __repr__(self):
        """ String representation of the model validation result. """
        return f"ModelValidationResult(model={self.model}, passed={self.passed}, issues={self.issues})"

@dataclass
class SchemaValidationResult:
    schema: str
    results: list[ModelValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """ Check if all models passed validation. """
        return all(r.passed for r in self.results)

    def append(self, model_result: ModelValidationResult) -> None:
        """ TODO """
        self.results.append(model_result)

    def __repr__(self):
        """ String representation of the schema validation result. """
        return f"SchemaValidationResult(schema={self.schema}, results={self.results})"

    def to_markdown(self) -> str:
        """ TODO """
        raise NotImplementedError("Markdown conversion is not implemented yet.")
