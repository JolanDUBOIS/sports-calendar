import logging

from sportindex import SportClient

from sports_calendar.application import SelectionService
from sports_calendar.core import SelectionFilter, SelectionItem

logger = logging.getLogger(__name__)


class SelectionFilterPresenter:
    def __init__(self, filter: SelectionFilter, parent_item: SelectionItem, client: SportClient):
        self.filter = filter
        self.parent_item = parent_item
        self.client = client

    @property
    def uid(self) -> str:
        return self.filter.uid

    @property
    def title(self) -> str:
        return f'{self.filter.name} ({self.filter.fields.filter_type.value.replace("_", " ").capitalize()})'
    @property
    def sport_id(self) -> int:
        return self.parent_item.sport_id

    @property
    def explanation(self) -> str:
        raise NotImplementedError

        # Empty Filter explaination example:
        # "This is an empty filter. It doesn't filter anything and is mainly used "
        # "as a starting point for creating new filters or for testing purposes."

    def delete(self) -> None:
        SelectionService.remove_filter(self.filter.uid)

    def update(self, payload: dict) -> None:
        logger.debug(f"Updating filter '{self.uid}' with payload: {payload}")
        new_filter = SelectionFilter.from_dict(payload)
        SelectionService.replace_filter(new_filter)

    def clone(self) -> ...:
        raise NotImplementedError
