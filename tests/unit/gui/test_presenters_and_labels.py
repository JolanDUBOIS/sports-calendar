""" Card headings and search labels.

These cover the things that made the UI unreadable rather than broken: two
identical "Paris Saint-Germain" rows, four rules all titled "(Min ranking)",
and a card headed "Football (Football)".
"""

from types import SimpleNamespace

import pytest

import sports_calendar.interfaces.gui.presenters.selection_item as selection_item_module
from sports_calendar.core import SelectionFilter, SelectionItem
from sports_calendar.interfaces.gui.catalog import format_entity_label
from sports_calendar.interfaces.gui.presenters.filter import SelectionFilterPresenter
from sports_calendar.interfaces.gui.presenters.selection_item import SelectionItemPresenter

PSG_TEAM = "t-cpt:1644"
ALCARAZ = "t-ath:275923"


def _entity(name: str, gender: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(name=name, gender=SimpleNamespace(value=gender) if gender else None)


# ---- Gender labelling -----------------------------------------------------

@pytest.mark.parametrize(("gender", "expected"), [("F", "PSG (F)"), ("M", "PSG (M)")])
def test_teams_are_always_marked_with_their_gender(gender: str, expected: str) -> None:
    """ A club and its women's side share a name exactly. """
    assert format_entity_label(_entity("PSG", gender), PSG_TEAM) == expected


def test_athletes_are_never_marked() -> None:
    """ "Carlos Alcaraz (M)" would be absurd. """
    assert format_entity_label(_entity("Carlos Alcaraz", "M"), ALCARAZ) == "Carlos Alcaraz"


def test_team_without_a_stated_gender_is_left_bare() -> None:
    assert format_entity_label(_entity("PSG"), PSG_TEAM) == "PSG"


def test_label_still_falls_back_to_the_raw_id() -> None:
    assert format_entity_label(None, PSG_TEAM) == PSG_TEAM


# ---- Item headings --------------------------------------------------------

def _item_presenter(monkeypatch, name: str | None) -> SelectionItemPresenter:
    # The module is imported at the top of this file, not here:
    # test_packaging_isolation purges sys.modules, so an import inside the test
    # body can land after that and fail to resolve `sports_calendar.interfaces`.
    monkeypatch.setattr(selection_item_module, "get_sport_name", lambda client, sport_id: "Football")

    item = SelectionItem.from_dict({"name": name, "sport_id": 1, "uid": "it-1", "filters": []})
    return SelectionItemPresenter(item=item, parent_selection=None, client=None)


def test_item_name_matching_the_sport_is_not_repeated(monkeypatch) -> None:
    """ Naming a football item "Football" gave "Football (Football)". """
    assert _item_presenter(monkeypatch, "Football").title == "Football"


def test_item_name_matching_the_sport_ignores_case(monkeypatch) -> None:
    assert _item_presenter(monkeypatch, "football").title == "Football"


def test_a_distinct_item_name_is_kept(monkeypatch) -> None:
    assert _item_presenter(monkeypatch, "My clubs").title == "My clubs (Football)"


def test_item_without_a_name_shows_the_sport(monkeypatch) -> None:
    assert _item_presenter(monkeypatch, None).title == "Football"


# ---- Rule headings --------------------------------------------------------

def _filter_presenter(name: str | None, fields: dict) -> SelectionFilterPresenter:
    filter_ = SelectionFilter.from_dict(
        {"name": name, "sport_id": 5, "uid": "f-1", "fields": fields}
    )
    return SelectionFilterPresenter(filter=filter_, parent_item=None, client=None)


WORLD_RANKING_FIELDS = {
    "filter_type": "world_ranking",
    "ranking": 20,
    "ranking_id": 5,
    "sport_id": 5,
    "selection_rule": {"rule": "any", "reference": None},
}


def test_unnamed_rule_is_titled_by_what_it_does() -> None:
    """ Used to render as a bare "(World ranking)". """
    assert _filter_presenter(None, WORLD_RANKING_FIELDS).title == "Top Players (world rankings)"


def test_ranking_label_says_players_for_an_individual_sport() -> None:
    """ "Top Teams" is wrong for tennis. """
    assert "Top Players" in _filter_presenter(None, WORLD_RANKING_FIELDS).title


def test_ranking_label_says_teams_for_a_team_sport() -> None:
    fields = WORLD_RANKING_FIELDS | {"ranking_id": 2, "sport_id": 1}
    presenter = SelectionFilterPresenter(
        filter=SelectionFilter.from_dict({"name": None, "sport_id": 1, "uid": "f-2", "fields": fields}),
        parent_item=None,
        client=None,
    )
    assert presenter.title == "Top Teams (world rankings)"


def test_named_rule_leads_with_its_name() -> None:
    assert _filter_presenter("Title race", WORLD_RANKING_FIELDS).title == "Title race"


def test_unnamed_rule_describes_its_contents() -> None:
    assert _filter_presenter(None, WORLD_RANKING_FIELDS).subtitle == "Top 20 of ATP (men)"


def test_named_rule_keeps_the_type_as_context() -> None:
    subtitle = _filter_presenter("Title race", WORLD_RANKING_FIELDS).subtitle
    assert subtitle == "Top Players (world rankings) · Top 20 of ATP (men)"


def test_empty_rule_has_a_title_and_no_contents() -> None:
    presenter = _filter_presenter(None, {"filter_type": "empty"})
    assert presenter.title == "Not set yet"
    assert presenter.subtitle == ""
