from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from nicegui import ui

from sports_calendar.core.selection import FilterType, Rule, SelectionFilter
from sports_calendar.core.sports import allowed_filter_types, ranking_choices

from ...copy import (
    FIELD_HELP,
    FILTER_TYPE_HELP,
    FOLLOW_TITLE,
    FOLLOW_TYPE_LABEL,
    MODAL_LOAD_FAILED,
    MODAL_LOADING,
    OPPONENT_LABEL,
    ROUNDS_LABEL,
    ROUNDS_NONE_SHARED,
    RULE_LABELS,
    RULE_NAME_EXAMPLES,
    RULE_NAME_HELP,
    RULE_NAME_LABEL,
    SESSION_LABELS,
    filter_type_label,
    search_hint,
)
from .base import FormModal
from .fields import (
    LoadingField,
    ModalField,
    MultipleSelectField,
    NumberField,
    SearchableMultipleSelectField,
    SearchableSelectField,
    SelectField,
    TextField,
)

if TYPE_CHECKING:
    from ...catalog import FilterSearchProvider

logger = logging.getLogger(__name__)


selection_rule_options = {
    rule.value: RULE_LABELS.get(rule, rule.name.capitalize()) for rule in Rule
}
# Only the tiers that are actually sessions: StageTier also carries structural
# levels (sport, season, event, lap) that nobody would ever pick.
sessions_options = {tier.value: label for tier, label in SESSION_LABELS.items()}


def filter_type_options_for(sport_id: int, current: FilterType | None = None) -> dict[str, str]:
    """ The filter types worth offering for a sport.

    Offering all of them everywhere is what made the modal misleading: picking
    "Race sessions" for football, or "Top-ranked teams" for tennis, produced an
    empty calendar with no explanation rather than an error.

    `current` is always kept, even if the sport does not allow it, so a filter
    saved earlier can still be opened and read instead of silently losing its
    own type from the dropdown.
    """
    allowed = allowed_filter_types(sport_id)
    return {
        filter_type.value: filter_type_label(filter_type, sport_id)
        for filter_type in FilterType
        if filter_type in allowed or filter_type is current
    }

# --- Helpers ---

def _format_selection_rule(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """Extracts flat UI fields into the nested dictionary expected by the Core."""
    rule_str = raw_payload.get("selection_rule", Rule.ANY.value)
    return {
        "rule": rule_str,
        # `reference` means "the opponent", and nothing else reads it. Keeping a
        # stale one around after switching back to "any match" saved a value the
        # form no longer shows.
        "reference": raw_payload.get("selection_reference") if rule_str == Rule.OPPONENT.value else None
    }


def _rule_and_reference_fields(
    default_rule: str,
    default_reference: Any,
    search_provider: FilterSearchProvider,
    sport_id: int,
) -> list[ModalField]:
    """ The "which matches to keep" pair, shared by three filter types.

    The opponent picker only makes sense for the OPPONENT rule, so it follows
    the dropdown instead of sitting there permanently asking for a team that
    two of the three rules ignore.
    """
    reference_field = SearchableSelectField(
        name="selection_reference",
        label=OPPONENT_LABEL,
        search_fn=lambda query: search_provider.search_competitor(query, sport_id),
        search_hint=search_hint("competitor", sport_id),
        default_value=default_reference,
        help_text=FIELD_HELP["selection_reference"],
    )
    reference_field.set_visible(default_rule == Rule.OPPONENT.value)

    rule_field = SelectField(
        name="selection_rule",
        label="Which matches to keep",
        default=default_rule,
        options=selection_rule_options,
        help_text=FIELD_HELP["selection_rule"],
        on_change=lambda value: reference_field.set_visible(value == Rule.OPPONENT.value),
    )
    return [rule_field, reference_field]


# --- The Adapters ---

def adapt_empty_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {"filter_type": FilterType.EMPTY.value}
    }

def adapt_min_ranking_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {
            "filter_type": FilterType.MIN_RANKING.value,
            "ranking": raw_payload.get("ranking"),
            "competition_ids": raw_payload.get("competition_ids"),
            "selection_rule": _format_selection_rule(raw_payload)
        }
    }

def adapt_world_ranking_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {
            "filter_type": FilterType.WORLD_RANKING.value,
            "ranking": raw_payload.get("ranking"),
            "ranking_id": raw_payload.get("ranking_id"),
            # Carried into the fields as well: rankings are only reachable
            # through the sport that publishes them.
            "sport_id": sport_id,
            "selection_rule": _format_selection_rule(raw_payload)
        }
    }

def adapt_competitions_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {
            "filter_type": FilterType.COMPETITIONS.value,
            "competition_ids": raw_payload.get("competition_ids"),
            "from_round": raw_payload.get("from_round") or None,
        }
    }

def adapt_competitors_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {
            "filter_type": FilterType.COMPETITORS.value,
            "competitor_ids": raw_payload.get("competitor_ids"),
            "selection_rule": _format_selection_rule(raw_payload)
        }
    }

def adapt_sessions_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": {
            "filter_type": FilterType.SESSIONS.value,
            "competition_id": raw_payload.get("competition_id"),
            "sessions": raw_payload.get("sessions")
        }
    }


# --- Field builders ---

def build_empty_filter_fields(**kwargs) -> list[ModalField]:
    return []

def build_min_ranking_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.MIN_RANKING:
        defaults = {
            "ranking": 5,
            "competition_ids": {},
            "selection_rule": Rule.ANY.value,
            "selection_reference": None
        }
    else:
        defaults = {
            "ranking": initial_filter.fields.ranking,
            "competition_ids": search_provider.get_competition_options(initial_filter.fields.competition_ids),
            "selection_rule": initial_filter.fields.selection_rule.rule.value,
            "selection_reference": search_provider.get_competitor_option(initial_filter.fields.selection_rule.reference) if initial_filter.fields.selection_rule.reference is not None else None
        }
    return [
        NumberField(
            name="ranking",
            label="Minimum Ranking",
            default=defaults["ranking"],
            help_text=FIELD_HELP["ranking"]
        ),
        SearchableMultipleSelectField(
            name="competition_ids",
            # Not "Competitions": you are choosing which tables to read
            # positions from, and the competition is only how they are named.
            label="Standings to read",
            default_values=defaults["competition_ids"],
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id),
            help_text=FIELD_HELP["competition_ids"]
        ),
        *_rule_and_reference_fields(
            defaults["selection_rule"], defaults["selection_reference"], search_provider, sport_id
        ),
    ]

def build_world_ranking_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    choices = ranking_choices(sport_id)
    ranking_options = dict(choices)

    if initial_filter.fields.filter_type != FilterType.WORLD_RANKING:
        defaults = {
            "ranking": 10,
            # The headline ranking for the sport, which is listed first.
            "ranking_id": choices[0][0] if choices else None,
            "selection_rule": Rule.ANY.value,
            "selection_reference": None
        }
    else:
        defaults = {
            "ranking": initial_filter.fields.ranking,
            "ranking_id": initial_filter.fields.ranking_id,
            "selection_rule": initial_filter.fields.selection_rule.rule.value,
            "selection_reference": search_provider.get_competitor_option(initial_filter.fields.selection_rule.reference) if initial_filter.fields.selection_rule.reference is not None else None
        }
    return [
        SelectField(
            name="ranking_id",
            label="Ranking",
            default=defaults["ranking_id"],
            options=ranking_options,
            help_text=FIELD_HELP["ranking_id"]
        ),
        NumberField(
            name="ranking",
            label="Top how many",
            default=defaults["ranking"],
            help_text=FIELD_HELP["world_ranking"]
        ),
        *_rule_and_reference_fields(
            defaults["selection_rule"], defaults["selection_reference"], search_provider, sport_id
        ),
    ]

def build_competitions_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.COMPETITIONS:
        defaults = {"competition_ids": {}, "from_round": None}
    else:
        defaults = {
            "competition_ids": search_provider.get_competition_options(initial_filter.fields.competition_ids),
            "from_round": initial_filter.fields.from_round,
        }

    rounds_field = SelectField(
        name="from_round",
        label=ROUNDS_LABEL,
        default=defaults["from_round"],
        options=search_provider.get_shared_rounds(list(defaults["competition_ids"])),
        help_text=FIELD_HELP["from_round"],
        empty_note=ROUNDS_NONE_SHARED,
        clearable=True,
    )

    def _refresh_rounds(competition_ids: list) -> None:
        """ The offered rounds depend on which competitions are chosen. """
        rounds_field.set_options(search_provider.get_shared_rounds(competition_ids))

    return [
        SearchableMultipleSelectField(
            name="competition_ids",
            label="Competitions",
            default_values=defaults["competition_ids"],
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id),
            help_text=FIELD_HELP["competition_ids"],
            on_change=_refresh_rounds,
        ),
        rounds_field,
    ]

def build_competitors_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.COMPETITORS:
        defaults = {
            "competitor_ids": {},
            "selection_rule": Rule.ANY.value,
            "selection_reference": None
        }
    else:
        defaults = {
            "competitor_ids": search_provider.get_competitor_options(initial_filter.fields.competitor_ids),
            "selection_rule": initial_filter.fields.selection_rule.rule.value,
            "selection_reference": search_provider.get_competitor_option(initial_filter.fields.selection_rule.reference) if initial_filter.fields.selection_rule.reference is not None else None
        }
    return [
        SearchableMultipleSelectField(
            name="competitor_ids",
            label="Competitors",
            default_values=defaults["competitor_ids"],
            search_fn=lambda query: search_provider.search_competitor(query, sport_id),
            search_hint=search_hint("competitor", sport_id),
            help_text=FIELD_HELP["competitor_ids"]
        ),
        *_rule_and_reference_fields(
            defaults["selection_rule"], defaults["selection_reference"], search_provider, sport_id
        ),
    ]

def build_sessions_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.SESSIONS:
        defaults = {
            "competition_id": None,
            "sessions": {}
        }
    else:
        defaults = {
            "competition_id": search_provider.get_competition_option(initial_filter.fields.competition_id),
            "sessions": initial_filter.fields.sessions
        }
    return [
        SearchableSelectField(
            name="competition_id",
            label="Competition",
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id),
            default_value=defaults["competition_id"],
            help_text=FIELD_HELP["competition_id"]
        ),
        MultipleSelectField(
            name="sessions",
            label="Sessions",
            default=defaults["sessions"],
            options=sessions_options,
            help_text=FIELD_HELP["sessions"]
        )
    ]


# --- Maps ---

_ADAPTER_MAP = {
    FilterType.EMPTY: adapt_empty_payload,
    FilterType.MIN_RANKING: adapt_min_ranking_payload,
    FilterType.WORLD_RANKING: adapt_world_ranking_payload,
    FilterType.COMPETITIONS: adapt_competitions_payload,
    FilterType.COMPETITORS: adapt_competitors_payload,
    FilterType.SESSIONS: adapt_sessions_payload,
}

_FIELD_BUILDER_MAP = {
    FilterType.EMPTY: build_empty_filter_fields,
    FilterType.MIN_RANKING: build_min_ranking_filter_fields,
    FilterType.WORLD_RANKING: build_world_ranking_filter_fields,
    FilterType.COMPETITIONS: build_competitions_filter_fields,
    FilterType.COMPETITORS: build_competitors_filter_fields,
    FilterType.SESSIONS: build_sessions_filter_fields,
}


# --- Filter Modal ---

class FilterModal(FormModal):
    """ The dialog for building one rule.

    Its fields depend on the filter type, and their defaults come off the
    network: the names of the competitions the rule already holds, the rounds
    those competitions share. That is seconds of work for a rule naming five
    competitions, and doing it before showing the dialog meant the Edit button
    appeared to do nothing at all — long enough that clicking again, and opening
    a second dialog behind the first, was the natural response.

    So the dialog opens first and fills itself in: pass `defer_fields=True` and
    await `load_fields()` once it is on screen. Save stays disabled until the
    real fields have arrived, so a half-built form cannot overwrite a rule.
    """

    def __init__(
        self,
        initial_filter: SelectionFilter,
        search_provider: FilterSearchProvider,
        title: str = FOLLOW_TITLE,
        message: str | None = None,
        initial_filter_type: FilterType = FilterType.EMPTY,
        defer_fields: bool = False,
        **kwargs,
    ):
        self._initial_filter = initial_filter
        self._search_provider = search_provider
        self._selected_filter_type = initial_filter_type
        self._loading = defer_fields

        self._filter_type_field = SelectField(
            name="filter_type",
            label=FOLLOW_TYPE_LABEL,
            options=filter_type_options_for(initial_filter.sport_id, initial_filter_type),
            default=self._selected_filter_type.value,
            help_text=FILTER_TYPE_HELP.get(self._selected_filter_type),
            # These run to a paragraph and would overlap the next field's label
            # if rendered as a Quasar hint.
            help_as_caption=True,
        )

        self._name_field = TextField(
            name="name",
            label=RULE_NAME_LABEL,
            default=initial_filter.name or "",
            placeholder=RULE_NAME_EXAMPLES,
            help_text=RULE_NAME_HELP,
        )

        self._dynamic_fields = (
            [LoadingField(MODAL_LOADING)] if self._loading else self._build_dynamic_fields()
        )

        initial_adapter = partial(
            _ADAPTER_MAP[self._selected_filter_type],
            self._initial_filter.sport_id,
            self._initial_filter.uid
        )
        kwargs["payload_adapter"] = initial_adapter

        # Name last: the substance of the rule is what it follows, and the label
        # is optional garnish for telling two similar rules apart afterwards.
        super().__init__(
            title=title,
            fields=[self._filter_type_field, *self._dynamic_fields, self._name_field],
            message=message,
            **kwargs,
        )

    def _build_body(self) -> None:
        super()._build_body()
        self._bind_filter_type_change()

    def _bind_filter_type_change(self) -> None:
        """ Re-arm the type dropdown's handler.

        Needed after every `replace_fields`: re-rendering destroys the select
        element and builds a new one, which does not carry the old handler.
        """
        if self._filter_type_field._element is None:
            return
        self._filter_type_field._element.on_value_change(
            lambda event: self.update(
                event.value if isinstance(event.value, FilterType) else FilterType(event.value)
            )
        )

    def _build_dynamic_fields(self) -> list[ModalField]:
        """ The fields for the selected type. Hits the network — never call it
        on the event loop; see `load_fields`. """
        return _FIELD_BUILDER_MAP[self._selected_filter_type](
            initial_filter=self._initial_filter,
            search_provider=self._search_provider,
            sport_id=self._initial_filter.sport_id,
        )

    def _is_ready(self) -> bool:
        return not self._loading

    def _show_fields(self, fields: list[ModalField]) -> None:
        self._dynamic_fields = fields
        self.replace_fields([self._filter_type_field, *self._dynamic_fields, self._name_field])
        self.payload_adapter = partial(
            _ADAPTER_MAP[self._selected_filter_type],
            self._initial_filter.sport_id,
            self._initial_filter.uid
        )
        self._bind_filter_type_change()

    def _set_loading(self, loading: bool) -> None:
        self._loading = loading
        if self._confirm_button is not None:
            self._confirm_button.set_enabled(not loading)

    async def load_fields(self) -> None:
        """ Fetch the real fields and swap them in, with the dialog already up.

        Off the event loop, so the page stays responsive while it runs — the
        provider's client is synchronous and a competitions rule costs roughly a
        second per competition.
        """
        try:
            fields = await asyncio.to_thread(self._build_dynamic_fields)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to build fields for filter '%s'", self._initial_filter.uid)
            ui.notify(MODAL_LOAD_FAILED, type="negative")
            self._close()
            return

        self._set_loading(False)
        self._show_fields(fields)

    async def update(self, new_filter_type: FilterType) -> None:
        """ Rebuild the form around a newly chosen filter type. """
        self._selected_filter_type = new_filter_type
        self._filter_type_field.default = self._selected_filter_type.value
        # Explanation follows the choice, so the modal describes what is selected.
        self._filter_type_field.help_text = FILTER_TYPE_HELP.get(self._selected_filter_type)

        # Some types need the network for their defaults, so the form shows a
        # placeholder in the meantime rather than freezing on the dropdown.
        self._set_loading(True)
        self._show_fields([LoadingField(MODAL_LOADING)])
        try:
            fields = await asyncio.to_thread(self._build_dynamic_fields)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to build fields for type '%s'", new_filter_type)
            ui.notify(MODAL_LOAD_FAILED, type="negative")
            fields = []
        self._set_loading(False)
        self._show_fields(fields)

    def _close(self) -> None:
        if self._dialog is not None:
            self._dialog.close()
