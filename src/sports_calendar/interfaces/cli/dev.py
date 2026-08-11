import logging

import typer

from sports_calendar.application.selection import SelectionService

logger = logging.getLogger(__name__)


dev_tools = typer.Typer(help="Development tools for the sports calendar application.")


@dev_tools.command()
def list_sports():
    """ Print every sport with its id and slug, ready to paste into SPORT_SLUGS.

    The GUI keys its search examples on a sport slug, but selections store the
    raw numeric id, so the two need mapping. That mapping is the one thing that
    cannot be written offline.
    """
    from sportindex import Sport, SportClient

    client = SportClient()
    sports = sorted(client.list(Sport), key=lambda sport: sport.name.lower())

    typer.echo("SPORT_SLUGS: dict[int, str] = {")
    for sport in sports:
        raw_id = Sport.decode_id(sport.id)[2]
        slug = getattr(sport, "slug", None) or sport.name.lower()
        typer.echo(f'    {raw_id}: "{slug}",  # {sport.name}')
    typer.echo("}")

@dev_tools.command()
def validate_selections():
    """ Validate the selections files and rewrites them into a standardized format if possible. """
    SelectionService.initialize_registry() # Loading the selections validates them
    logger.info("All selection files are valid.")
    selections = SelectionService.get_all_selections()
    for sel in selections:
        try:
            logger.info(f"Rewriting selection: {sel.name}")
            SelectionService.replace_selection(sel) # Rewriting the selection to standardize the format
        except Exception:
            logger.exception(f"Failed to rewrite selection: {sel.name}")
