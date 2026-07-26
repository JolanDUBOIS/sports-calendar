import typer

launch_gui = typer.Typer(help="Commands to launch the Sports Calendar GUI application.")

@launch_gui.callback(invoke_without_command=True)
def main_launch():
    """ Launch the Sports Calendar GUI application. """
    from sports_calendar.interfaces.gui.app import run

    run()
