import typer
from typer import BadParameter

from .main import run_pipeline_logic, run_validation_logic, run_selection_logic, run_test_logic
from .data_pipeline import DataStage


app = typer.Typer()

def validate_stage(stage: str | None) -> DataStage | None:
    if stage is None:
        return None
    try:
        return DataStage.from_str(stage)
    except ValueError:
        raise BadParameter(f"Invalid stage '{stage}'. Valid stages are: {DataStage.as_str()}.")

def validate_repo(repo: str) -> str:
    if repo not in ["test", "prod"]:
        raise BadParameter("Invalid repository. Valid values are 'test' or 'prod'.")
    return repo

def repo_option(workflow: str):
    return typer.Option(
        "test",
        "--repo",
        help=f"Specify the repository to run the {workflow} on (e.g. 'prod' or 'test').",
        callback=validate_repo,
    )
def stage_option(workflow: str):
    return typer.Option(
        None,
        "--stage",
        help=f"Specify the stage to run the {workflow} on (default is all stages). Valid values are {DataStage.as_str()}.",
        callback=validate_stage,
    )
def model_option(workflow: str):
    return typer.Option(
        None,
        "--model",
        help=f"Specify the model to run the {workflow} on (stage must be specified). If not specified, all models will be run."
    )

@app.command()
def run_pipeline(
    manual: bool = typer.Option(False, "--manual", help="Run the pipeline in manual mode (default is automatic)."),
    repo: str = repo_option("pipeline"),
    stage: str | None = stage_option("pipeline"),
    model: str | None = model_option("pipeline"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run the pipeline in dry run mode."),
    verbose: bool = typer.Option(False, "--verbose", help="Run the pipeline in verbose mode.")
):
    """ Run the data pipeline. """
    if stage is None and model is not None:
        typer.echo("Error: --model requires --stage to be specified.", err=True)
        raise typer.Exit(code=1)
    run_pipeline_logic(
        repo=repo,
        stage=stage,
        model=model,
        manual=manual,
        dry_run=dry_run,
        verbose=verbose
    )

@app.command()
def run_validation(
    repo: str = repo_option("validation"),
    stage: str | None = stage_option("validation"),
    model: str | None = model_option("validation"),
    raise_on_error: bool = typer.Option(False, "--raise-on-error", help="Raise an error if validation fails."),
    verbose: bool = typer.Option(False, "--verbose", help="Run the validation in verbose mode.")
):
    """ Run the data validation. """
    if stage is None and model is not None:
        typer.echo("Error: --model requires --stage to be specified.", err=True)
        raise typer.Exit(code=1)
    results = run_validation_logic(
        repo=repo,
        stage=stage,
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )
    # TODO - Display results in a user-friendly way

@app.command()
def run_selection(
    verbose: bool = typer.Option(False, "--verbose", help="Run the selection in verbose mode.")
):
    """ Run the data selection. """
    run_selection_logic()

@app.command()
def test(
    test_name: str = typer.Argument(..., help="Name of the test to run.")
):
    """ Run a specific test. """
    run_test_logic(test_name)

@app.command()
def clean(
    repo: str = repo_option("cleaning"),
    stage: str | None = stage_option("cleaning")
):
    """ Clean the data repository. """
    raise NotImplementedError("The clean command is not implemented yet.")

@app.command()
def revert(
    run_id: str = typer.Argument(None, help="ID of the run to revert to."),
):
    """ Revert the data repository to a previous state. """
    raise NotImplementedError("The revert command is not implemented yet.")
