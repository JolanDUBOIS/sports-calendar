import logging

from sportindex import SportClient

from sports_calendar.application import SelectionService
from sports_calendar.core import SelectionFilter, SelectionItem
from sports_calendar.core.rounds import round_label_from_slug
from sports_calendar.core.selection import FilterType
from sports_calendar.core.sports import ranking_choices

from ..catalog import SportIndexFilterSearchProvider
from ..copy import SESSION_LABELS, filter_type_label

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
        """ The name if there is one, otherwise what the rule does.

        Previously this was always `f'{name} (Min ranking)'`, which rendered as
        a bare "(Min ranking)" for the unnamed filters that are the common case,
        and used the raw enum value where the rest of the app says "Top of the
        standings".
        """
        return self.filter.name or filter_type_label(self.filter.fields.filter_type, self.sport_id)

    @property
    def subtitle(self) -> str:
        """ Enough of the rule's contents to recognise it while collapsed.

        A card of four rules is unusable if they all read "Specific teams or
        players", so the detail line names what is actually inside — a few
        entries and a count of the rest.
        """
        parts: list[str] = []
        if self.filter.name:
            # The type moved out of the heading to make room for the name, so it
            # reappears here as context.
            parts.append(filter_type_label(self.filter.fields.filter_type, self.sport_id))

        summary = self._describe_contents()
        if summary:
            parts.append(summary)
        return ' · '.join(parts)

    def _describe_contents(self) -> str:
        """ A short, human list of what this rule points at. """
        fields = self.filter.fields
        provider = SportIndexFilterSearchProvider(self.client)

        if fields.filter_type is FilterType.COMPETITORS:
            return self._name_list(provider.get_competitor_options, fields.competitor_ids)

        if fields.filter_type is FilterType.COMPETITIONS:
            names = self._name_list(provider.get_competition_options, fields.competition_ids)
            if fields.from_round:
                # Derived from the slug rather than read from the provider: a
                # collapsed card must not hit the network to describe itself.
                label = round_label_from_slug(fields.from_round)
                return ' · '.join(part for part in (names, f'from {label}') if part)
            return names

        if fields.filter_type is FilterType.MIN_RANKING:
            names = self._name_list(provider.get_competition_options, fields.competition_ids)
            return f'Top {fields.ranking}' + (f' of {names}' if names else '')

        if fields.filter_type is FilterType.WORLD_RANKING:
            label = dict(ranking_choices(fields.sport_id)).get(fields.ranking_id, 'ranking')
            return f'Top {fields.ranking} of {label}'

        if fields.filter_type is FilterType.SESSIONS:
            sessions = ', '.join(
                SESSION_LABELS.get(tier, str(tier)) for tier in fields.sessions
            )
            competition = provider.get_competition_option(fields.competition_id) if fields.competition_id else ''
            return ' · '.join(part for part in (competition, sessions) if part)

        return ''

    @staticmethod
    def _name_list(resolve, entity_ids: list, limit: int = 3) -> str:
        """ The first few names, plus how many were left out.

        Capped because each name costs a catalog lookup, and because a card is
        meant to be glanceable rather than complete.
        """
        if not entity_ids:
            return ''
        shown = list(resolve(list(entity_ids)[:limit]).values())
        remaining = len(entity_ids) - len(shown)
        return ', '.join(shown) + (f' +{remaining}' if remaining > 0 else '')

    @property
    def sport_id(self) -> int:
        """ The filter's own sport, falling back to its parent item's.

        A SelectionFilter stores the sport it belongs to, so the presenter does
        not need a parent just to label itself — which also keeps it usable in
        isolation.
        """
        sport_id = getattr(self.filter, "sport_id", None)
        if sport_id is not None:
            return sport_id
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
        self.filter = new_filter

    def clone(self) -> ...:
        raise NotImplementedError
