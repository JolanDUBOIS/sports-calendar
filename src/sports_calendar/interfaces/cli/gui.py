import typer

from .extras import missing_extra

launch_gui = typer.Typer(help="Commands to launch the Sports Calendar GUI application.")

@launch_gui.callback(invoke_without_command=True)
def main_launch():
    """ Launch the Sports Calendar GUI application. """
    try:
        from sports_calendar.interfaces.gui.app import run
    except ImportError as exc:
        raise missing_extra("ui", exc) from exc

    run()
