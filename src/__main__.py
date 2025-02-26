import os
from datetime import datetime
from pathlib import Path

from src import logger
from src.selection import Selection
from src.cli_utils import parse_arguments
from src.live_soccer_source import SoccerLiveScraper
from src.calendar import FootballCalendar, GoogleCalendarManager


def update_database():
    logger.info("Updating database...")
    soccer_live_scraper = SoccerLiveScraper(wait_time=20)
    soccer_live_scraper.update_database()
    logger.info("Database updated successfully")

def run_selection(save_ics: bool=False):
    logger.info("Running selection...")
    selection_file_path = Path(os.getenv("SELECTION_FILE"))
    database_path = Path("data")
    selection = Selection.from_json(selection_file_path, database_path)
    matches = selection.get_matches()
    calendar = FootballCalendar()
    calendar.add_matches(matches)
    if save_ics:
        now = datetime.now().strftime("%Y-%m-%d")
        calendar.save_to_ics(database_path / f"selection_calendar_{now}.ics")
    google_calendar_manager = GoogleCalendarManager()
    google_calendar_manager.add_calendar(calendar)
    logger.info("Selection ran successfully")

if __name__ == '__main__':
    args = parse_arguments()

    if args.update_database:
        update_database()
    
    elif args.full_update:
        # TODO 
        pass

    elif args.run_selection:
        run_selection(save_ics=True)
