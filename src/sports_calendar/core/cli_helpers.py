import functools
import typer

from sports_calendar.core import DataStage


def confirm_if_reset(func):
    @functools.wraps(func)
    def wrapper(*args, reset: bool = False, **kwargs):
        if reset:
            if not typer.confirm(
                "Are you sure you want to proceed with reset mode? This will delete files and metadata."
            ):
                typer.echo("Reset mode aborted by the user.", err=True)
                raise typer.Exit(code=0)
        return func(*args, reset=reset, **kwargs)
    return wrapper

def require_stage_if_model(func):
    @functools.wraps(func)
    def wrapper(*args, stage: str | None = None, model: str | None = None, **kwargs):
        if stage is None and model is not None:
            typer.echo("Error: --model requires --stage.", err=True)
            raise typer.Exit(code=1)
        return func(*args, stage=stage, model=model, **kwargs)
    return wrapper

def parse_stage(value: str | None) -> DataStage | None:
    if value is None:
        return value
    try:
        return DataStage(value)
    except ValueError as e:
        raise typer.BadParameter(str(e))
