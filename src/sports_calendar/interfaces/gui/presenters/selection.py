from typing import Literal

from sportindex import SportClient

from sports_calendar.application import SelectionService
from sports_calendar.core import Selection

from ..catalog import get_sport_name
from .selection_item import SelectionItemPresenter


class SelectionPresenter:
    def __init__(self, selection: Selection, client: SportClient):
        self.selection = selection
        self.client = client

    @property
    def uid(self) -> str:
        return self.selection.uid

    @property
    def name(self) -> str:
        return self.selection.name

    @property
    def title(self) -> str:
        return self.selection.name

    @property
    def subtitle(self) -> str:
        sport_ids = self.selection.sport_ids
        if not sport_ids:
            return 'No sports yet'
        return ', '.join(get_sport_name(self.client, sid) for sid in sport_ids)

    def get_item_presenters(
        self,
        sort_by: Literal["uid", "created_at", "updated_at"] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> list[SelectionItemPresenter]:
        items = [SelectionItemPresenter(item, self.selection, self.client) for item in self.selection.items]
        if sort_by is None:
            return items
        reverse = order == "desc"
        return sorted(items, key=lambda p: getattr(p, sort_by), reverse=reverse)

    def delete(self) -> None:
        SelectionService.remove_selection(self.selection.name)

    def create_item(self, sport_id: int, name: str = "") -> SelectionItemPresenter:
        created_item = SelectionService.add_empty_item(self.selection.name, sport_id, name=name.strip())
        selection = SelectionService.get_selection(self.selection.name)
        item = selection.get_item(created_item.uid)
        return SelectionItemPresenter(item, selection, self.client)

    @classmethod
    def create(cls, client: SportClient, name: str) -> "SelectionPresenter":
        selection = SelectionService.add_empty_selection(name)
        return cls(selection=selection, client=client)
