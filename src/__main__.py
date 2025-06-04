import traceback
from dotenv import load_dotenv

from . import logger
from .cli import app


load_dotenv()

if __name__ == '__main__':
    try:
        app()
    except Exception as e:
        logger.error(f"An error occurred while running the application: {e}")
        logger.debug(traceback.format_exc())
