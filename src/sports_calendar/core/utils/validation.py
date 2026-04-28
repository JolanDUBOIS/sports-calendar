import logging
from datetime import datetime


def validate(cond: bool, msg: str, logger: logging.Logger, exc_type: type[Exception] = ValueError):
    """ Check a condition, log error, and raise exception if false. """
    if not cond:
        logger.error(msg, stacklevel=2)
        raise exc_type(msg)

def validate_timestamp(ts: str, name: str, logger: logging.Logger) -> None:
    try:
        datetime.fromisoformat(ts)
    except ValueError:
        logger.error(f"{name} must be a valid ISO timestamp, got: {ts}", stacklevel=2)
        raise ValueError(f"{name} must be a valid ISO timestamp, got: {ts}") from None
