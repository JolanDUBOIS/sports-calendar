import typer

from .datastage import DataStage
from .config.main import config


app = typer.Typer()

# ------------------------
# Shared Utility Functions
# ------------------------

def parse_stage(value: str) -> DataStage:
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
# Option Factories
# ------------------------

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
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the pipeline on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the pipeline in (default in runtime.yml config file)."),
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the pipeline on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = model_option("pipeline"),
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

    if repo is not None:
        config.set_repo(repo)
    if env is not None:
        config.set_environment(env)
    
    from .main import run_pipeline_logic
    run_pipeline_logic(
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
    stage: DataStage | None = stage_option("validation"),
    model: str | None = model_option("validation"),
    raise_on_error: bool = typer.Option(False, "--raise-on-error"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data validation. """
    validate_stage_model(stage, model)

    if repo is not None:
        config.set_repo(repo)
    if env is not None:
        config.set_environment(env)
    
    from .main import run_validation_logic
    results = run_validation_logic(
        stage=stage,
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )
    # TODO - Display results

@app.command()
def run_selection(
    key: str = typer.Argument("dev"),
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the selection on (default in infrastructure.yml config file)."),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data selection. """
    if repo is not None:
        config.set_repo(repo)

    from .main import run_selection_logic
    run_selection_logic(
        key=key,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def test(
    name: str = typer.Argument(..., help="Name of the test to run."),
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the tests on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the tests in (default in runtime.yml config file)."),
):
    """ Run a specific test. """
    if repo is not None:
        config.set_repo(repo)
    if env is not None:
        config.set_environment(env)

    from .main import run_test_logic
    run_test_logic(name=name)

@app.command()
def clean_repository(
    repo: str | None,
    stage: str | None
):
    """ Clean the data repository. """
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
    if scope is not None and scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)
    
    from .main import clear_calendar_logic
    clear_calendar_logic(
        key=key,
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        verbose=verbose
    )
