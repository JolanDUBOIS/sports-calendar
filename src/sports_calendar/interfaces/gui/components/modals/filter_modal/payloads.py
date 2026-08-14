""" Turning what the form collected into what the domain stores.

One adapter per filter type. They are the only place that knows the shape of a
`SelectionFilter` dict, so a change to the domain lands here rather than in the
dialog.

Deliberately dumb: no lookups, no validation, no network. Everything they need
is already in the payload the form handed over.
"""

from __future__ import annotations

from typing import Any

from sports_calendar.core.selection import FilterType, Rule


def _format_selection_rule(raw_payload: dict[str, Any]) -> dict[str, Any]:
    """ Extracts flat UI fields into the nested dictionary expected by the core. """
    rule_str = raw_payload.get("selection_rule", Rule.ANY.value)
    return {
        "rule": rule_str,
        # `reference` means "the opponent", and nothing else reads it. Keeping a
        # stale one around after switching back to "any match" saved a value the
        # form no longer shows.
        "reference": raw_payload.get("selection_reference") if rule_str == Rule.OPPONENT.value else None
    }


def _envelope(
    sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any], fields: dict[str, Any]
) -> dict[str, Any]:
    """ The part every filter carries, whatever its type.

    Written once rather than six times: the shared keys used to be repeated in
    every adapter, so adding one to the domain meant finding all six and
    noticing if you missed one.
    """
    return {
        "sport_id": sport_id,
        "uid": filter_uid,
        "name": raw_payload.get("name") or None,
        "fields": fields,
    }


def adapt_empty_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.EMPTY.value,
    })


def adapt_min_ranking_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.MIN_RANKING.value,
        "ranking": raw_payload.get("ranking"),
        "competition_ids": raw_payload.get("competition_ids"),
        "selection_rule": _format_selection_rule(raw_payload),
    })


def adapt_world_ranking_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.WORLD_RANKING.value,
        "ranking": raw_payload.get("ranking"),
        "ranking_id": raw_payload.get("ranking_id"),
        # Carried into the fields as well: rankings are only reachable through
        # the sport that publishes them.
        "sport_id": sport_id,
        "selection_rule": _format_selection_rule(raw_payload),
    })


def adapt_competitions_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.COMPETITIONS.value,
        "competition_ids": raw_payload.get("competition_ids"),
        "from_round": raw_payload.get("from_round") or None,
    })


def adapt_competitors_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.COMPETITORS.value,
        "competitor_ids": raw_payload.get("competitor_ids"),
        "selection_rule": _format_selection_rule(raw_payload),
    })


def adapt_sessions_payload(sport_id: int, filter_uid: str | None, raw_payload: dict[str, Any]) -> dict[str, Any]:
    return _envelope(sport_id, filter_uid, raw_payload, {
        "filter_type": FilterType.SESSIONS.value,
        "competition_id": raw_payload.get("competition_id"),
        "sessions": raw_payload.get("sessions"),
    })


ADAPTER_MAP = {
    FilterType.EMPTY: adapt_empty_payload,
    FilterType.MIN_RANKING: adapt_min_ranking_payload,
    FilterType.WORLD_RANKING: adapt_world_ranking_payload,
    FilterType.COMPETITIONS: adapt_competitions_payload,
    FilterType.COMPETITORS: adapt_competitors_payload,
    FilterType.SESSIONS: adapt_sessions_payload,
}
