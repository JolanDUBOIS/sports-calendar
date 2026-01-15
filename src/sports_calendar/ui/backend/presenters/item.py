from . import logger
from .filter import FilterPresenter
from sports_calendar.core.selection import SelectionItemSpec


class SelectionItemPresenter:
    @staticmethod
    def summary(item: SelectionItemSpec) -> dict:
        """ Minimal info for list views. """
        logger.debug(f"Presenting summary for item: {item.uid}")
        return {
            **item.to_dict(),
            "filters": [
                FilterPresenter.summary(flt) for flt in item.filters
            ]
        }

    @staticmethod
    def detailed(item: SelectionItemSpec) -> dict:
        """ Full info for detailed views. """
        logger.debug(f"Presenting detailed view for item: {item.uid}")
        return {
            **item.to_dict(),
            "filters": [
                FilterPresenter.detailed(flt) for flt in item.filters
            ]
        }
