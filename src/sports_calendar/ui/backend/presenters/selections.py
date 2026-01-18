from . import logger
from .items import SelectionItemPresenter
from sports_calendar.core.selection import Selection


class SelectionPresenter:
    @staticmethod
    def summary(sel: Selection) -> dict:
        """ Minimal info for list views. """
        logger.debug(f"Presenting summary for selection: {sel.uid}")
        return {
            **sel.to_dict(),
            "sports": sel.sports,
            "n_items": len(sel.items)
        }

    @staticmethod
    def detailed(sel: Selection) -> dict:
        """ Full ino for detailed views. """
        logger.debug(f"Presenting detailed view for selection: {sel.uid}")
        return {
            **sel.to_dict(),
            "sports": sel.sports,
            "n_items": len(sel.items),
            "items": [
                SelectionItemPresenter.detailed(item) for item in sel.items
            ]
        }
