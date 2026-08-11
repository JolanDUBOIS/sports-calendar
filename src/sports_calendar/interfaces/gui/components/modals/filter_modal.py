from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

from sports_calendar.core.selection import FilterType, Rule, SelectionFilter

from ...copy import (
    FIELD_HELP,
    FILTER_TYPE_HELP,
    FOLLOW_TITLE,
    FOLLOW_TYPE_LABEL,
    RULE_LABELS,
    SESSION_LABELS,
    filter_type_label,
    search_hint,
)
from .base import FormModal
from .fields import (
    ModalField,
    MultipleSelectField,
    NumberField,
    SearchableMultipleSelectField,
    SearchableSelectField,
    SelectField,
)

if TYPE_CHECKING:
    from ...catalog import FilterSearchProvider


selection_rule_options = {
    rule.value: RULE_LABELS.get(rule, rule.name.capitalize()) for rule in Rule
}
# Only the tiers that are actually sessions: StageTier also carries structural
# levels (sport, season, event, lap) that nobody would ever pick.
sessions_options = {tier.value: label for tier, label in SESSION_LABELS.items()}
filter_type_options = {
    filter_type.value: filter_type_label(filter_type) for filter_type in FilterType
}

# --- Helpers ---

def _format_selection_rule(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """Extracts flat UI fields into the nested dictionary expected by the Core."""
    rule_str = raw_payload.get("selection_rule", Rule.ANY.value)
    return {
        "rule": rule_str,
        "reference": raw_payload.get("selection_reference") # if rule_str == Rule.OPPONENT.value else None # NOTE - Strict for now
    }


# --- The Adapters ---

def adapt_empty_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "fields": {"filter_type": FilterType.EMPTY.value}
    }

def adapt_min_ranking_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "fields": {
            "filter_type": FilterType.MIN_RANKING.value,
            "ranking": raw_payload.get("ranking"),
            "competition_ids": raw_payload.get("competition_ids"),
            "selection_rule": _format_selection_rule(raw_payload)
        }
    }

def adapt_competitions_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "fields": {
            "filter_type": FilterType.COMPETITIONS.value,
            "competition_ids": raw_payload.get("competition_ids"),
            # TODO - Add stage when we have stages in the model
        }
    }

def adapt_competitors_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
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
            default=defaults["ranking"]
        ),
        SearchableMultipleSelectField(
            name="competition_ids",
            label="Competitions",
            default_values=defaults["competition_ids"],
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id)
        ),
        SelectField(
            name="selection_rule",
            label="Which matches to keep",
            default=defaults["selection_rule"],
            options=selection_rule_options,
            help_text=FIELD_HELP["selection_rule"]
        ),
        SearchableSelectField(
            name="selection_reference",
            label="Opponent Team",
            search_fn=lambda query: search_provider.search_competitor(query, sport_id),
            search_hint=search_hint("competitor", sport_id),
            default_value=defaults["selection_reference"]
        ) # Should only be shown when selection_rule is OPPONENT, but implementing that kind of dynamic field logic in the modal
          # is a bit complex, so for now it's always shown.
    ]

def build_competitions_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.COMPETITIONS:
        defaults = {
            "competition_ids": {},
        }
    else:
        defaults = {
            "competition_ids": search_provider.get_competition_options(initial_filter.fields.competition_ids),
        }
    return [
        SearchableMultipleSelectField(
            name="competition_ids",
            label="Competitions",
            default_values=defaults["competition_ids"],
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id)
        ),
        # TODO - Add stage when we have stages in the model
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
            search_hint=search_hint("competitor", sport_id)
        ),
        SelectField(
            name="selection_rule",
            label="Which matches to keep",
            default=defaults["selection_rule"],
            options=selection_rule_options,
            help_text=FIELD_HELP["selection_rule"]
        ),
        SearchableSelectField(
            name="selection_reference",
            label="Opponent Team",
            search_fn=lambda query: search_provider.search_competitor(query, sport_id),
            search_hint=search_hint("competitor", sport_id),
            default_value=defaults["selection_reference"]
        )
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
            default_value=defaults["competition_id"]
        ),
        MultipleSelectField(
            name="sessions",
            label="Sessions",
            default=defaults["sessions"],
            options=sessions_options
        )
    ]


# --- Maps ---

_ADAPTER_MAP = {
    FilterType.EMPTY: adapt_empty_payload,
    FilterType.MIN_RANKING: adapt_min_ranking_payload,
    FilterType.COMPETITIONS: adapt_competitions_payload,
    FilterType.COMPETITORS: adapt_competitors_payload,
    FilterType.SESSIONS: adapt_sessions_payload,
}

_FIELD_BUILDER_MAP = {
    FilterType.EMPTY: build_empty_filter_fields,
    FilterType.MIN_RANKING: build_min_ranking_filter_fields,
    FilterType.COMPETITIONS: build_competitions_filter_fields,
    FilterType.COMPETITORS: build_competitors_filter_fields,
    FilterType.SESSIONS: build_sessions_filter_fields,
}


# --- Filter Modal ---

class FilterModal(FormModal):
    """TODO"""

    def __init__(
        self,
        initial_filter: SelectionFilter,
        search_provider: FilterSearchProvider,
        title: str = FOLLOW_TITLE,
        message: str | None = None,
        initial_filter_type: FilterType = FilterType.EMPTY,
        **kwargs,
    ):
        self._initial_filter = initial_filter
        self._search_provider = search_provider
        self._selected_filter_type = initial_filter_type

        self._filter_type_field = SelectField(
            name="filter_type",
            label=FOLLOW_TYPE_LABEL,
            options=filter_type_options,
            default=self._selected_filter_type.value,
            help_text=FILTER_TYPE_HELP.get(self._selected_filter_type),
        )

        self._dynamic_fields = _FIELD_BUILDER_MAP[self._selected_filter_type](
            initial_filter=self._initial_filter,
            search_provider=self._search_provider,
            sport_id=self._initial_filter.sport_id,
        )

        initial_adapter = partial(
            _ADAPTER_MAP[self._selected_filter_type],
            self._initial_filter.sport_id,
            self._initial_filter.uid
        )
        kwargs["payload_adapter"] = initial_adapter

        super().__init__(title=title, fields=[self._filter_type_field, *self._dynamic_fields], message=message, **kwargs)

    def _build_body(self) -> None:
        super()._build_body()
        if self._filter_type_field._element is None:
            return
        self._filter_type_field._element.on_value_change(
            lambda event: self.update(event.value if isinstance(event.value, FilterType) else FilterType(event.value))
        )

    def update(self, new_filter_type: FilterType) -> None:
        self._selected_filter_type = new_filter_type
        self._filter_type_field.default = self._selected_filter_type.value
        # Explanation follows the choice, so the modal describes what is selected.
        self._filter_type_field.help_text = FILTER_TYPE_HELP.get(self._selected_filter_type)

        self._dynamic_fields = _FIELD_BUILDER_MAP[self._selected_filter_type](
            initial_filter=self._initial_filter,
            search_provider=self._search_provider,
            sport_id=self._initial_filter.sport_id,
        )
        new_fields = [self._filter_type_field, *self._dynamic_fields]

        self.replace_fields(new_fields)
        self.payload_adapter = partial(
            _ADAPTER_MAP[self._selected_filter_type],
            self._initial_filter.sport_id,
            self._initial_filter.uid
        )

        if self._filter_type_field._element is None:
            return
        self._filter_type_field._element.on_value_change(
            lambda event: self.update(event.value if isinstance(event.value, FilterType) else FilterType(event.value))
        )
