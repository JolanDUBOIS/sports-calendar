import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.db_legacy import logger
from src.db_legacy.models import Match, Standing
from src.db_legacy.database import create_session, get_last_version_query


def query_matches(
    session: Session = None,
    match_ids: list[str] = None,
    team_names: list[str] = None,
    competition_names: list[str] = None,
    date_from: pd.Timestamp = None,
    date_to: pd.Timestamp = None,
    latest_version: bool = False
) -> pd.DataFrame:
    """ Queries the matches based on given filter criteria and returns results as a DataFrame. """
    if session is None:
        session = create_session()

    query = session.query(Match)

    if match_ids:
        query = query.filter(Match.id.in_(match_ids))

    if team_names:
        query = query.filter(
            or_(
                Match.home_team_name.in_(team_names),
                Match.away_team_name.in_(team_names)
            )
        )

    if competition_names:
        query = query.filter(Match.competition_name.in_(competition_names))

    if date_from:
        query = query.filter(Match.date >= date_from)

    if date_to:
        query = query.filter(Match.date <= date_to)

    if latest_version:
        get_last_version_query(query, Match.match_id, Match.created_at)

    results = query.all()
    session.close()

    return pd.DataFrame([r.as_dict() for r in results])

def query_standings(
    session: Session = None,
    team_names: list[str] = None,
    competition_names: list[str] = None,
    min_position: int = None,
    latest_version: bool = False
) -> pd.DataFrame:
    """ Queries the standings table based on the given filter criteria and returns results as a DataFrame. """
    if session is None:
        session = create_session()

    query = session.query(Standing)

    if team_names:
        query = query.filter(Standing.team_name.in_(team_names))

    if competition_names:
        query = query.filter(Standing.competition_name.in_(competition_names))

    if min_position is not None:
        query = query.filter(Standing.position >= min_position)

    if latest_version:
        get_last_version_query(query, Standing.standing_id, Standing.created_at)

    results = query.all()
    session.close()

    return pd.DataFrame([r.as_dict() for r in results])

def insert_matches(matches: pd.DataFrame, session: Session = None): 
    """ Inserts a DataFrame of match records into the database. """
    if session is None:
        session = create_session()

    try:
        records = [Match(**row) for _, row in matches.iterrows()]
        session.add_all(records)
        session.commit()
    except Exception as e:
        logger.error(f"Error inserting matches: {e}")
        session.rollback()
    finally:
        session.close()

def insert_standings(standings: pd.DataFrame, session: Session = None):
    """ TODO """
    if session is None:
        session = create_session()
    
    try:
        records = [Standing(**row) for _, row in standings.iterrows()]
        session.add_all(records)
        session.commit()
    except Exception as e:
        logger.error(f"Error inserting standings: {e}")
        session.rollback()
    finally:
        session.close()
