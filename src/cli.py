import typer # type: ignore
from typer import BadParameter # type: ignore

from .main import (
    run_pipeline_logic,
    run_validation_logic,
    run_selection_logic,
    clear_calendar_logic,
    run_test_logic
)
from .data_pipeline import DataStage
from .config.manager import base_config


app = typer.Typer()

# ------------------------
# Shared Utility Functions
# ------------------------

def parse_stage(stage: str | None) -> DataStage | None:
    if stage is None:
        return None
    try:
        return DataStage.from_str(stage)
    except ValueError:
        raise BadParameter(f"Invalid stage '{stage}'. Valid stages are: {DataStage.as_str()}.")

def confirm_reset() -> bool:
    return typer.confirm("Are you sure you want to proceed with reset mode? This will delete files and metadata.")

# ------------------------
# Option Factories
# ------------------------

def repo_option(workflow: str, default: str = "prod"):
    return typer.Option(
        default,
        "--repo",
        help=f"Specify the repository to run the {workflow} on (e.g. 'prod' or 'test')."
    )

def stage_option(workflow: str):
    return typer.Option(
        None,
        "--stage",
        help=f"Specify the stage to run the {workflow} on (default is all stages). Valid values are {DataStage.as_str()}."
    )

def model_option(workflow: str):
    return typer.Option(
        None,
        "--model",
        help=f"Specify the model to run the {workflow} on (stage must be specified)."
    )

# ------------------------
# CLI Commands
# ------------------------

@app.command()
def run_pipeline(
    manual: bool = typer.Option(False, "--manual"),
    repo: str = repo_option("pipeline"),
    stage: str | None = stage_option("pipeline"),
    model: str | None = model_option("pipeline"),
    reset: bool = typer.Option(False, "--reset"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data pipeline. """
    if stage is None and model is not None:
        typer.echo("Error: --model requires --stage to be specified.", err=True)
        raise typer.Exit(code=1)
    if reset and not confirm_reset():
        typer.echo("Reset mode aborted by the user.", err=True)
        raise typer.Exit(code=0)

    base_config.set_active_repo(repo)
    run_pipeline_logic(
        stage=parse_stage(stage),
        model=model,
        manual=manual,
        reset=reset,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def run_validation(
    repo: str = repo_option("validation"),
    stage: str | None = stage_option("validation"),
    model: str | None = model_option("validation"),
    raise_on_error: bool = typer.Option(False, "--raise-on-error"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data validation. """
    if stage is None and model is not None:
        typer.echo("Error: --model requires --stage to be specified.", err=True)
        raise typer.Exit(code=1)

    base_config.set_active_repo(repo)
    results = run_validation_logic(
        stage=parse_stage(stage),
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )
    # TODO - Display results

@app.command()
def run_selection(
    repo: str = repo_option("selection"),
    key: str = typer.Argument("dev"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data selection. """
    base_config.set_active_repo(repo)
    run_selection_logic(
        key=key,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def test(
    name: str = typer.Argument(..., help="Name of the test to run.")
):
    """ Run a specific test. """
    run_test_logic(name)

@app.command()
def clean_repository(
    repo: str = repo_option("cleaning"),
    stage: str | None = stage_option("cleaning")
):
    """ Clean the data repository. """
    base_config.set_active_repo(repo)
    raise NotImplementedError("The clean command is not implemented yet.")

@app.command()
def revert(
    run_id: str = typer.Argument(None, help="ID of the run to revert to.")
):
    """ Revert the data repository to a previous state. """
    raise NotImplementedError("The revert command is not implemented yet.")

@app.command()
def clear_calendar(
    key: str = typer.Argument("dev"),
    scope: str | None = typer.Option(None, "--scope", help="Specify which events to clear: 'all', 'future', or 'past'."),
    date_from: str | None = typer.Option(None, "--date-from", help="Clear events from this date onwards (YYYY-MM-DD). Not needed if --scope is specified."),
    date_to: str | None = typer.Option(None, "--date-to", help="Clear events up to this date (YYYY-MM-DD). Not needed if --scope is specified."),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Clear events from the Google Calendar. """
    if scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)
    clear_calendar_logic(
        key=key,
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        verbose=verbose
    )
