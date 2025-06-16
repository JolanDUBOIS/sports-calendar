import logging

logger = logging.getLogger(__name__)

from src.db_legacy.database import create_session
from src.db_legacy.helpers import query_matches, query_standings, insert_matches, insert_standings