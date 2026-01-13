import typer

from .backend import app


launch_ui = typer.Typer(help="Launch the Sports Calendar UI application.")

@launch_ui.callback(invoke_without_command=True)
def main():
    """ Launch the Sports Calendar UI application. """
    app.launch()
