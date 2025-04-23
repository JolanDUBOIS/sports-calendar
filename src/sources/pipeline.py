import traceback

from src.sources import logger
from .web import LiveSoccerScraper, FootballRankingScraper
from .database_manager import DatabaseManager

def update_database(competitions: list[str]=None):
    """ TODO """
    logger.info("Updating database...")
    ls_scraper = LiveSoccerScraper()
    fr_scraper = FootballRankingScraper()
    db = DatabaseManager()

    if competitions is None:
        competitions = ls_scraper.get_competitions()

    for competition in competitions:
        logger.info(f"Updating database for competition: {competition}")
        try:
            matches, standings = ls_scraper.get_competition(competition)
            if matches is not None:
                db.insert("matches", matches)
                logger.debug(f"Added matches for {competition} to database.")
            if standings is not None:
                db.insert("standings", standings)
                logger.debug(f"Added standings for {competition} to database.")
        except Exception as e:
            logger.error(f"Error while updating database for competition {competition}: {e}")
            logger.debug(traceback.format_exc())

    try:
        logger.info("Updating FIFA rankings...")
        fifa_rankings = fr_scraper.get_fifa_rankings()
        fifa_rankings["Competition"] = "FIFA Rankings"
        fifa_rankings["Is League"] = True
        if fifa_rankings is not None:
            db.insert("standings", fifa_rankings)
            logger.debug("Added FIFA rankings to database.")
    except Exception as e:
        logger.error(f"Error while updating FIFA rankings: {e}")
        logger.debug(traceback.format_exc())

    logger.info("Database update complete.")
