from sportindex import SportClient

from sports_calendar.application import SelectionService
from sports_calendar.core import Selection, SelectionItem

from ..catalog import get_sport_name
from .filter import SelectionFilterPresenter


class SelectionItemPresenter:
    def __init__(self, item: SelectionItem, parent_selection: Selection, client: SportClient):
        self.item = item
        self.parent_selection = parent_selection
        self.client = client

    @property
    def uid(self) -> str:
        return self.item.uid

    @property
    def created_at(self) -> str:
        return self.item.created_at

    @property
    def updated_at(self) -> str:
        return self.item.updated_at

    @property
    def title(self) -> str:
        """ Sport first: that is what the card actually is. """
        sport = get_sport_name(self.client, self.item.sport_id)
        return f'{self.item.name} ({sport})' if self.item.name else sport

    @property
    def subtitle(self) -> str:
        count = len(self.item.filters)
        if count == 0:
            return 'Nothing followed yet'
        return 'Following 1 thing' if count == 1 else f'Following {count} things'

    def get_filter_presenters(self) -> list[SelectionFilterPresenter]:
        return [SelectionFilterPresenter(filter, self.item, self.client) for filter in self.item.filters]

    def delete(self) -> None:
        SelectionService.remove_item(self.item.uid)

    def create_empty_filter(self, name: str | None = None) -> SelectionFilterPresenter:
        created_filter = SelectionService.add_empty_filter(self.item.uid, name=name)
        return SelectionFilterPresenter(created_filter, self.item, self.client)

    def clone(self) -> ...:
        raise NotImplementedError
