""" Which fields a rule's dialog shows, and what they start out holding.

One builder per filter type. Unlike the adapters in `payloads.py`, these *do*
hit the network: a rule stores entity ids, and the form has to show names, so
opening a rule that names five competitions costs five lookups. That is why
`FilterModal` runs them off the event loop rather than before showing the
dialog — see the class docstring there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sports_calendar.core.selection import FilterType, Rule, SelectionFilter
from sports_calendar.core.sports import ranking_choices

from ....copy import (
    FIELD_HELP,
    OPPONENT_LABEL,
    ROUNDS_LABEL,
    ROUNDS_NONE_SHARED,
    RULE_LABELS,
    SESSION_LABELS,
    search_hint,
)
from ..fields import (
    ModalField,
    MultipleSelectField,
    NumberField,
    SearchableMultipleSelectField,
    SearchableSelectField,
    SelectField,
)

if TYPE_CHECKING:
    from ....catalog import FilterSearchProvider


selection_rule_options = {
    rule.value: RULE_LABELS.get(rule, rule.name.capitalize()) for rule in Rule
}
# Only the tiers that are actually sessions: StageTier also carries structural
# levels (sport, season, event, lap) that nobody would ever pick.
sessions_options = {tier.value: label for tier, label in SESSION_LABELS.items()}


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


def _selection_rule_defaults(
    initial_filter: SelectionFilter, search_provider: FilterSearchProvider
) -> tuple[str, Any]:
    """ The rule and its opponent, as the "which matches to keep" pair wants them.

    Resolving the opponent's *name* is a lookup, and skipping it when there is no
    opponent is the difference between opening a dialog and waiting on the
    network for a field the form is not even going to show.
    """
    selection_rule = initial_filter.fields.selection_rule
    reference = selection_rule.reference
    return (
        selection_rule.rule.value,
        search_provider.get_competitor_option(reference) if reference is not None else None,
    )


def build_empty_filter_fields(**kwargs) -> list[ModalField]:  # noqa: ARG001
    return []


def build_min_ranking_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs,  # noqa: ARG001
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.MIN_RANKING:
        ranking, competitions = 5, {}
        rule, reference = Rule.ANY.value, None
    else:
        ranking = initial_filter.fields.ranking
        competitions = search_provider.get_competition_options(initial_filter.fields.competition_ids)
        rule, reference = _selection_rule_defaults(initial_filter, search_provider)

    return [
        NumberField(
            name="ranking",
            label="Minimum Ranking",
            default=ranking,
            help_text=FIELD_HELP["ranking"],
        ),
        SearchableMultipleSelectField(
            name="competition_ids",
            # Not "Competitions": you are choosing which tables to read
            # positions from, and the competition is only how they are named.
            label="Standings to read",
            default_values=competitions,
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id),
            help_text=FIELD_HELP["competition_ids"],
        ),
        *_rule_and_reference_fields(rule, reference, search_provider, sport_id),
    ]


def build_world_ranking_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs,  # noqa: ARG001
) -> list[ModalField]:
    choices = ranking_choices(sport_id)

    if initial_filter.fields.filter_type != FilterType.WORLD_RANKING:
        ranking = 10
        # The headline ranking for the sport, which is listed first.
        ranking_id = choices[0][0] if choices else None
        rule, reference = Rule.ANY.value, None
    else:
        ranking = initial_filter.fields.ranking
        ranking_id = initial_filter.fields.ranking_id
        rule, reference = _selection_rule_defaults(initial_filter, search_provider)

    return [
        SelectField(
            name="ranking_id",
            label="Ranking",
            default=ranking_id,
            options=dict(choices),
            help_text=FIELD_HELP["ranking_id"],
        ),
        NumberField(
            name="ranking",
            label="Top how many",
            default=ranking,
            help_text=FIELD_HELP["world_ranking"],
        ),
        *_rule_and_reference_fields(rule, reference, search_provider, sport_id),
    ]


def build_competitions_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs,  # noqa: ARG001
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.COMPETITIONS:
        competitions, from_round = {}, None
    else:
        competitions = search_provider.get_competition_options(initial_filter.fields.competition_ids)
        from_round = initial_filter.fields.from_round

    rounds_field = SelectField(
        name="from_round",
        label=ROUNDS_LABEL,
        default=from_round,
        options=search_provider.get_shared_rounds(list(competitions)),
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
            default_values=competitions,
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
    **kwargs,  # noqa: ARG001
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.COMPETITORS:
        competitors = {}
        rule, reference = Rule.ANY.value, None
    else:
        competitors = search_provider.get_competitor_options(initial_filter.fields.competitor_ids)
        rule, reference = _selection_rule_defaults(initial_filter, search_provider)

    return [
        SearchableMultipleSelectField(
            name="competitor_ids",
            label="Competitors",
            default_values=competitors,
            search_fn=lambda query: search_provider.search_competitor(query, sport_id),
            search_hint=search_hint("competitor", sport_id),
            help_text=FIELD_HELP["competitor_ids"],
        ),
        *_rule_and_reference_fields(rule, reference, search_provider, sport_id),
    ]


def build_sessions_filter_fields(
    initial_filter: SelectionFilter,
    search_provider: FilterSearchProvider,
    sport_id: int,
    **kwargs,  # noqa: ARG001
) -> list[ModalField]:
    if initial_filter.fields.filter_type != FilterType.SESSIONS:
        competition_id, sessions = None, {}
    else:
        competition_id = search_provider.get_competition_option(initial_filter.fields.competition_id)
        sessions = initial_filter.fields.sessions

    return [
        SearchableSelectField(
            name="competition_id",
            label="Competition",
            search_fn=lambda query: search_provider.search_competition(query, sport_id),
            search_hint=search_hint("competition", sport_id),
            default_value=competition_id,
            help_text=FIELD_HELP["competition_id"],
        ),
        MultipleSelectField(
            name="sessions",
            label="Sessions",
            default=sessions,
            options=sessions_options,
            help_text=FIELD_HELP["sessions"],
        ),
    ]


FIELD_BUILDER_MAP = {
    FilterType.EMPTY: build_empty_filter_fields,
    FilterType.MIN_RANKING: build_min_ranking_filter_fields,
    FilterType.WORLD_RANKING: build_world_ranking_filter_fields,
    FilterType.COMPETITIONS: build_competitions_filter_fields,
    FilterType.COMPETITORS: build_competitors_filter_fields,
    FilterType.SESSIONS: build_sessions_filter_fields,
}
