from pathlib import Path

import yaml

from . import logger
from .selection import Selection
from .data_access import get_clean_matches
from .calendar import (
    FootballCalendar,
    MatchesDetails,
    GoogleCalendarManager,
    GoogleCalendarAPI,
    GoogleAuthManager
)
from ..config import get_config


def run_selection(name: str, dry_run: bool = False, **kwargs):
    """ TODO """
    logger.info(f"Running selection for selection {name}.")
    selections_path = Path(get_config("selections.path"))
    selection_file = selections_path / f"{name}.yml"
    selection = load_selection(selection_file)

    matches = selection.get_matches()
    logger.debug(f"Matches:\n{matches.tail(20)}")
    clean_matches = get_clean_matches(matches)
    logger.debug(f"Clean matches:\n{clean_matches.sort_values(by=['date_time']).tail(20)}")

    football_calendar = FootballCalendar()
    matches_details = MatchesDetails.from_dataframe(clean_matches)
    football_calendar.add_matches(matches_details)
    if dry_run:
        logger.info("Dry run mode is enabled. No events will be added to the google calendar.")
        return

    google_cal_manager = GoogleCalendarManager.from_defaults()
    google_cal_manager.add_calendar(football_calendar.calendar)
    logger.info(f"Selection {name} has been successfully processed and added to the google calendar.")

def load_selection(path: Path | str) -> Selection:
    path = Path(path)
    if not path.exists():
        logger.error(f"Selection file {path} does not exist.")
        raise FileNotFoundError(f"Selection file {path} does not exist.")
    
    with open(path, 'r') as file:
        try:
            data = yaml.safe_load(file)
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML file {path}: {e}")
            raise ValueError(f"Error loading YAML file {path}: {e}")

    if not isinstance(data, dict):
        logger.error(f"Invalid selection data format in {path}. Expected a dictionary.")
        raise ValueError(f"Invalid selection data format in {path}. Expected a dictionary.")

    return Selection.from_dict(data)
