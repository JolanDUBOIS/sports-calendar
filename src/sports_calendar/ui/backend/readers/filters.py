from typing import get_type_hints

from . import logger
from sports_calendar.core.utils import coerce
from sports_calendar.core.selection import SelectionFilter


class FilterReader:
    @staticmethod
    def from_payload(payload: dict, original_filter: SelectionFilter) -> SelectionFilter:
        """ Create a SelectionFilter instance from a payload dictionary. """
        logger.debug(f"Creating filter from payload: {payload} with original filter: {original_filter}")
        original_dict = original_filter.to_dict()
        original_dict.pop("uid", None)  # Remove uid to avoid conflicts  

        payload_coerced = {}
        for field_name, field_type in get_type_hints(original_filter.__class__).items():
            if field_name in payload:
                try:
                    logger.debug(f"Coercing field '{field_name}' to type {field_type} with value: {payload[field_name]}")
                    payload_coerced[field_name] = coerce(payload[field_name], field_type)
                except Exception:
                    logger.error(f"Error coercing field '{field_name}' to type {field_type} with value: {payload[field_name]}")
                    raise

        combined = {**original_dict, **payload_coerced}
        logger.debug(f"SelectionFilter.from_dict called with dict:\n{combined}")

        try:
            return SelectionFilter.from_dict(
                sport=original_filter.sport,
                data=combined
            )
        except Exception:
            logger.exception("Error creating filter from payload.")
            raise
