import typer
from nicegui import ui

from .app import create_app


launch_gui = typer.Typer(help="Launch the Sports Calendar GUI application.")

@launch_gui.callback(invoke_without_command=True)
def main():
    """ Launch the Sports Calendar GUI application. """
    create_app()
    ui.run(
        title="Sports Calendar",
        reload=False,
        show=False
    )
