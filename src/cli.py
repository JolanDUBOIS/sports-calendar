import typer

from .utils import DataStage
from .config.main import config


app = typer.Typer()

# ------------------------
# Shared Utility Functions
# ------------------------

def parse_stage(value: str | None) -> DataStage | None:
    if value is None:
        return value
    try:
        return DataStage(value)
    except ValueError as e:
        raise typer.BadParameter(str(e))

def confirm_reset() -> bool:
    return typer.confirm("Are you sure you want to proceed with reset mode? This will delete files and metadata.")

def validate_stage_model(stage: str | None, model: str | None):
    if stage is None and model is not None:
        typer.echo("Error: --model requires --stage.", err=True)
        raise typer.Exit(code=1)

# ------------------------
# CLI Commands
# ------------------------

### Pipeline Commands

@app.command()
def run_pipeline(
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the pipeline on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the pipeline in (default in runtime.yml config file)."),
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the pipeline on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = typer.Option(None, "--model", help=f"Specify the model to run the pipeline on (stage must be specified)."),
    manual: bool = typer.Option(False, "--manual"),
    reset: bool = typer.Option(False, "--reset"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data pipeline. """
    validate_stage_model(stage, model)
    if reset and not confirm_reset():
        typer.echo("Reset mode aborted by the user.", err=True)
        raise typer.Exit(code=0)

    config.set_repo(repo)
    if env is not None:
        config.set_environment(env)

    from .data_pipeline import run_pipeline as run_pipeline_func
    run_pipeline_func(
        stage=stage,
        model=model,
        manual=manual,
        reset=reset,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def run_validation(
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the validation on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the validation in (default in runtime.yml config file)."),
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the validation on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = typer.Option(None, "--model", help=f"Specify the model to run the validation on (stage must be specified)."),
    raise_on_error: bool = typer.Option(False, "--raise-on-error"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data validation. """
    validate_stage_model(stage, model)

    config.set_repo(repo)
    if env is not None:
        config.set_environment(env)
    
    from .data_pipeline import run_validation as run_validation_func
    results = run_validation_func(
        stage=stage,
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )
    # TODO - Display results

@app.command()
def clean_repository(
    repo: str | None,
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to clean (default is all stages). Valid values are " + ", ".join(DataStage.as_str()))
):
    """ Clean the data repository. """
    config.set_repo(repo)
    raise NotImplementedError("The clean command is not implemented yet.")


### Application Commands

@app.command()
def run_selection(
    key: str = typer.Argument("dev"),
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the selection on (default in infrastructure.yml config file)."),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data selection. """
    config.set_repo(repo)

    from .app import run_selection as run_selection_func
    run_selection_func(
        key=key,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def clear_calendar(
    key: str = typer.Argument("dev"),
    scope: str | None = typer.Option(None, "--scope", help="Specify which events to clear: 'all', 'future', or 'past'."),
    date_from: str | None = typer.Option(None, "--date-from", help="Clear events from this date onwards (YYYY-MM-DD). Not needed if --scope is specified."),
    date_to: str | None = typer.Option(None, "--date-to", help="Clear events up to this date (YYYY-MM-DD). Not needed if --scope is specified."),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Clear events from the Google Calendar. """
    if scope is not None and scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)
    
    from .app import clear_calendar as clear_calendar_func
    clear_calendar_func(
        key=key,
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        verbose=verbose
    )


### Other Commands

@app.command()
def test(
    name: str = typer.Argument(..., help="Name of the test to run."),
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the tests on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the tests in (default in runtime.yml config file)."),
):
    """ Run a specific test. """
    config.set_repo(repo)
    if env is not None:
        config.set_environment(env)
    raise NotImplementedError(f"The test command is not implemented yet.")

@app.command()
def revert(
    run_id: str = typer.Argument(None, help="ID of the run to revert to.")
):
    """ Revert the data repository to a previous state. """
    raise NotImplementedError("The revert command is not implemented yet.")
