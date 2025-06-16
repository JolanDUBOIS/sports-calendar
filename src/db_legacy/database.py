import traceback

from sqlalchemy import MetaData, func, create_engine, inspect
from sqlalchemy.orm import sessionmaker, Query

from src.db_legacy import logger
from src.config import get_config
from src.db_legacy.models import Base, Region, Competition, Team


DATABASE_URL = get_config("database.url", "sqlite:///data/football.db")

def create_session():
    """ Creates a new session for interacting with the database """
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def create_tables():
    """ Creates all tables in the database """
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)

def drop_tables():
    """ Drops all database tables """
    engine = create_engine(DATABASE_URL)
    Base.metadata.drop_all(engine)

def add_record(session, model, data):
    """ Adds a new record to the specified model """
    try:
        record = model(**data)
        session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_record(session, model, filter_conditions):
    """ Deletes a record from the specified model based on filter conditions """
    try:
        record = session.query(model).filter_by(**filter_conditions).first()
        if record:
            session.delete(record)
            session.commit()
        else:
            raise ValueError("Record not found for given conditions.")
    except Exception as e:
        session.rollback()
        raise e

def get_records(session, model, filter_conditions):
    """ Fetches records from the specified model based on filter conditions """
    try:
        return session.query(model).filter_by(**filter_conditions).all()
    except Exception as e:
        raise e

def get_all_records(session, model):
    """ Fetches all records from the specified model """
    try:
        return session.query(model).all()
    except Exception as e:
        raise e

def bulk_insert(session, model, data):
    """ Insert multiple records into the database at once using bulk save. """
    try:
        records = [model(**item) for item in data]
        session.bulk_save_objects(records)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def commit(session):
    """ Commit the current transaction to the database. """
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def get_last_version_query(query, id_column: str, version_column: str) -> Query:
    """ Returns a query that fetches the latest version (by specified version column) for each unique ID """
    return query.with_entities(
        id_column, 
        func.max(version_column).label('latest_version')
    ).group_by(id_column)

def drop_tables(engine, tables: list[str]):
    """ TODO """
    try:
        metadata = MetaData(bind=engine)
        metadata.reflect()
        for table in tables:
            if table in metadata.tables:
                metadata.tables[table].drop(bind=engine)
        logger.info(f"Tables dropped successfully: {tables}")
    except Exception as e:
        logger.error(f"Error while dropping tables: {e}")
        logger.debug(traceback.format_exc())
        raise e

def initialize_database(reset: bool = False):
    """ Initializes the database and optionally resets reference tables. """
    logger.info("Initializing database...")
    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    expected_tables = Base.metadata.tables.keys()

    if not all(table in existing_tables for table in expected_tables):
        logger.info("Creating missing tables...")
        Base.metadata.create_all(engine)
    
    if reset:
        logger.info("Resetting reference tables...")
        # drop_tables(engine, reference_tables)
        raise NotImplementedError("Implementation not finished...")
