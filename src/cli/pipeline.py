import typer

from .utils import (
    parse_stage,
    confirm_if_reset,
    require_stage_if_model,
    with_config
)
from src.config import Config
from src.utils import DataStage
from src.pipeline import run_pipeline, run_validation


pipeline_app = typer.Typer(help="Commands to run and manage the pipeline.")

@pipeline_app.command()
@confirm_if_reset
@require_stage_if_model
@with_config
def run(
    ctx: typer.Context,
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
    run_pipeline(
        config=ctx.obj.get("config"),
        stage=stage,
        model=model,
        manual=manual,
        reset=reset,
        dry_run=dry_run,
        verbose=verbose
    )

@pipeline_app.command()
@require_stage_if_model
@with_config
def validate(
    ctx: typer.Context,
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the validation on (default in infrastructure.yml config file)."),
    env: str | None = typer.Option(None, "--env", help="Specify the environment to run the validation in (default in runtime.yml config file)."),
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the validation on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = typer.Option(None, "--model", help=f"Specify the model to run the validation on (stage must be specified)."),
    raise_on_error: bool = typer.Option(False, "--raise-on-error"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data validation. """
    results = run_validation(
        config=ctx.obj.get("config"),
        stage=stage,
        model=model,
        raise_on_error=raise_on_error,
        verbose=verbose
    )
    # TODO - Display results
    raise NotImplementedError("Displaying validation results is not implemented yet.")
