import os, json
from pathlib import Path

from src import logger
from src.cli_utils import parse_arguments
from src.football_data_source import FootballDataConnector
from src.models_deprecated import Selection
from src.calendar_creation import FootballCalendar
from src.admin_utils import (
    fetch_and_store_teams,
    fetch_and_store_areas,
    fetch_and_store_competitions
)
from src.models import Selection
from src.live_soccer_scrapping import update_database
from src.live_soccer_scrapping.playground import get_matches_competition3


def main(selection_file: Path):
    """ TODO """
    connector = FootballDataConnector()
    with selection_file.open(mode='r') as file:
        selection_json = json.load(file)
    selection = Selection.from_json(selection_json, api_connector=connector)
    matches = selection.get_matches()
    calendar = FootballCalendar()
    calendar.add_matches(matches)
    calendar.save(Path('data/selection_calendar.ics'))

if __name__ == '__main__':
    args = parse_arguments()
    logger.info("--------------------- Programm starts ---------------------")

    if args.update_database:
        # connector = FootballDataConnector()
        # teams_storage_path = Path('data/football_teams.csv')
        # areas_storage_path = Path('data/football_areas.csv')
        # competitions_storage_path = Path('data/football_competitions.csv')

        # fetch_and_store_teams(connector, teams_storage_path)
        # fetch_and_store_areas(connector, areas_storage_path)
        # fetch_and_store_competitions(connector, competitions_storage_path)
        
        update_database(Path('data'), time_delta=7, wait_time=20)

    elif args.run_selection:
        selection_file = Path(os.getenv("SELECTION_FILE"))
        main(selection_file)
    
    elif args.test == 1:
        new_selection_path = Path('data/new_selection.json')
        with new_selection_path.open(mode='r') as file:
            selection_json = json.load(file)
        selection = Selection.from_json(selection_json, database_path=Path('data'))
        logger.debug(selection.get_matches())
    
    elif args.test == 2:
        matches = get_matches_competition3("france/coupe-de-france/", "Coupe de France", "France")
        logger.debug(matches)

    logger.info("--------------------- Programm ends ---------------------")
