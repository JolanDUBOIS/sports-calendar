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
        """ Sport first: that is what the card actually is.

        A name matching the sport adds nothing — naming a football item
        "Football" used to render as "Football (Football)" — so the
        parenthetical is dropped whenever it would only repeat the heading.
        """
        sport = get_sport_name(self.client, self.item.sport_id)
        name = (self.item.name or '').strip()
        if not name or name.casefold() == sport.casefold():
            return sport
        return f'{name} ({sport})'

    @property
    def subtitle(self) -> str:
        """ "3 filters", not "Following 3 things".

        "Filter" is jargon that non-developers already use and understand, and
        it is accurate; "thing" was vaguer without being any friendlier.
        """
        count = len(self.item.filters)
        if count == 0:
            return 'No filters yet'
        return '1 filter' if count == 1 else f'{count} filters'

    def refresh(self) -> None:
        """ Re-read the item from the service.

        The presenter holds a snapshot, and every mutation goes through
        `SelectionService`, which updates the stored copy rather than this one.
        Views used to patch their local list by hand to keep the two in step —
        two sources of truth, kept aligned by remembering to. This reads the one
        that is authoritative. It is an in-memory registry lookup, not I/O.
        """
        self.item = SelectionService.get_item(self.item.uid)

    def get_filter_presenters(self) -> list[SelectionFilterPresenter]:
        return [SelectionFilterPresenter(filter, self.item, self.client) for filter in self.item.filters]

    def delete(self) -> None:
        SelectionService.remove_item(self.item.uid)

    def create_empty_filter(self, name: str | None = None) -> SelectionFilterPresenter:
        created_filter = SelectionService.add_empty_filter(self.item.uid, name=name)
        return SelectionFilterPresenter(created_filter, self.item, self.client)

    def clone(self) -> ...:
        raise NotImplementedError
