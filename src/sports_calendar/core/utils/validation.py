import logging


def validate(cond: bool, msg: str, logger: logging.Logger, exc_type=ValueError):
    """ Check a condition, log error, and raise exception if false. """
    if not cond:
        logger.error(msg)
        raise exc_type(msg)
