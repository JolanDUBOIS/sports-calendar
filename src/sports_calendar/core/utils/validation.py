import logging


def validate(cond: bool, msg: str, logger: logging.Logger, exc_type: type[Exception] = ValueError):
    """ Check a condition, log error, and raise exception if false. """
    if not cond:
        logger.error(msg, stacklevel=2)
        raise exc_type(msg)
