from pathlib import Path
from datetime import datetime

from src import logger
from src.selection import Selection
from src.scraper_db import DatabaseManager, FootballRankingScraper
from src.calendar import FootballCalendar, GoogleCalendarManager
from src.deprecated.live_soccer_source_deprecated.new_feature_playground import test as ls_test


def run_selection(selection_file: Path, google_credentials_file_path: Path, google_calendar_id: str, save_ics: bool=False):
    """ TODO """
    logger.info("Running selection...")

    db = DatabaseManager()
    calendar = FootballCalendar()
    google_calendar_manager = GoogleCalendarManager(google_credentials_file_path, google_calendar_id)

    selection = Selection.from_json(selection_file, db)
    matches = selection.get_matches()
    calendar.add_matches(matches)
    if save_ics:
        now = datetime.now().strftime("%Y-%m-%d")
        calendar.save_to_ics(Path("data") / "ics_calendars" / f"selection_calendar_{now}.ics") # TODO - Add to config
    google_calendar_manager.add_calendar(calendar)
    logger.info("Selection ran successfully")

# TODO - update_database should be in this script !!

def test(no: int):
    """ TODO """
    logger.info("Running test...")
    if no == 1:
        competitions = ls_test()
        logger.debug(competitions.keys())
        
    elif no == 2:
        google_calendar_manager = GoogleCalendarManager()
        google_calendar_manager.delete_all_events()

    elif no == 3:
        selection_file_path = Path("data/selections/new_selection.json")
        db = DatabaseManager()
        selection = Selection.from_json(selection_file_path, db)
        matches = selection.get_matches()
        logger.debug(matches)

    elif no == 4:
        scraper = FootballRankingScraper()
        rankings = scraper.get_fifa_rankings()
        logger.debug(rankings)
