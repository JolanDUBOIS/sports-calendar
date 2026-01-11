import typer

from .main import run_validation
from sports_calendar.core import DataStage
from sports_calendar.core.cli_helpers import (
    parse_stage,
    require_stage_if_model,
)


validate_db = typer.Typer(help="Commands to run and manage the data validation pipeline.")

@validate_db.callback(invoke_without_command=True)
@require_stage_if_model
def main(
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the validation on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = typer.Option(None, "--model", help=f"Specify the model to run the validation on (stage must be specified)."),
    raise_on_error: bool = typer.Option(False, "--raise-on-error")
):
    """ Run the data validation. """
    # results = run_validation(
    #     stage=stage,
    #     model=model,
    #     raise_on_error=raise_on_error,
    #     verbose=verbose
    # )
    # # TODO - Display results
    # raise NotImplementedError("Displaying validation results is not implemented yet.")
    raise NotImplementedError("Data validation CLI command is not implemented yet.")
