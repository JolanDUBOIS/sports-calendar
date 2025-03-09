import traceback

from src.scraper_db import logger
from .data_scraper import SoccerLiveScraper
from .database_manager import DatabaseManager

def update_database(competitions: list[str]=None):
    """ TODO """
    logger.info("Updating database...")
    scraper = SoccerLiveScraper()
    db = DatabaseManager()

    if competitions is None:
        competitions = scraper.get_competitions()

    for competition in competitions:
        logger.info(f"Updating database for competition: {competition}")
        try:
            matches, standings = scraper.get_competition(competition)
            if matches is not None:
                db.insert("matches", matches)
                logger.debug(f"Added matches for {competition} to database.")
            if standings is not None:
                db.insert("standings", standings)
                logger.debug(f"Added standings for {competition} to database.")
        except Exception as e:
            logger.error(f"Error while updating database for competition {competition}: {e}")
            logger.debug(traceback.format_exc())
    logger.info("Database update complete.")
