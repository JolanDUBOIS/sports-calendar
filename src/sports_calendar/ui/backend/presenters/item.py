from . import logger
from .assets import SPORT_ICON_URLS
from .filter import FilterPresenter
from sports_calendar.core.selection import SelectionItem


class SelectionItemPresenter:
    @staticmethod
    def summary(item: SelectionItem) -> dict:
        """ Minimal info for list views. """
        logger.debug(f"Presenting summary for item: {item.uid}")
        return {
            **item.to_dict(),
            "n_filters": len(item.filters),
            "filters": [
                FilterPresenter.summary(flt) for flt in item.filters
            ]
        }

    @staticmethod
    def detailed(item: SelectionItem) -> dict:
        """ Full info for detailed views. """
        logger.debug(f"Presenting detailed view for item: {item.uid}")
        return {
            **item.to_dict(),
            "sport_icon_url": SPORT_ICON_URLS.get(item.sport, "/static/img/sports/default.png"),
            "n_filters": len(item.filters),
            "filters": [
                FilterPresenter.detailed(flt) for flt in item.filters
            ]
        }
