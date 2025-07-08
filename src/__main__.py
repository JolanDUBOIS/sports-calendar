import traceback
from dotenv import load_dotenv

from . import logger


load_dotenv()

def log_run_separator():
    from datetime import datetime
    separator = "\n" + "=" * 80
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.debug(f"{separator}\nNew run started at {timestamp}{separator}")

if __name__ == '__main__':
    log_run_separator()
    try:
        from .cli import app
        app()
    except Exception as e:
        logger.error(f"An error occurred while running the application...")
        logger.debug("Traceback:\n%s", traceback.format_exc())
