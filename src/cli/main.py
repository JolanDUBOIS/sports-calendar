import typer

from . import pipeline
from . import calendar

app = typer.Typer(help="Sports Calendar CLI Application.")
app.add_typer(pipeline.pipeline_app, name="pipeline", help="Commands to run and manage the pipeline.")
app.add_typer(calendar.calendar_app, name="calendar", help="Commands to run and manage the calendar selection and utils.")