import typer

from .main import run_pipeline
from sports_calendar.sc_core import DataStage
from sports_calendar.sc_core.cli_helpers import (
    parse_stage,
    confirm_if_reset,
    require_stage_if_model,
)


sync_db = typer.Typer(help="Commands to run and manage the data synchronization pipeline.")

@sync_db.callback(invoke_without_command=True)
@confirm_if_reset
@require_stage_if_model
def main(
    stage: str | None = typer.Option(None, "--stage", callback=parse_stage, help="Specify the stage to run the pipeline on (default is all stages). Valid values are " + ", ".join(DataStage.as_str())),
    model: str | None = typer.Option(None, "--model", help=f"Specify the model to run the pipeline on (stage must be specified)."),
    manual: bool = typer.Option(False, "--manual"),
    reset: bool = typer.Option(False, "--reset"),
    dry_run: bool = typer.Option(False, "--dry-run")
):
    """ Run the data pipeline. """
    run_pipeline(
        stage=stage,
        model=model,
        manual=manual,
        reset=reset,
        dry_run=dry_run
    )
