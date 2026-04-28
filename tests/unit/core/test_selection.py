from sports_calendar.core import SportType
from sports_calendar.core.selection import (
    Selection, EmptyFilterFields,
    MinRankingFilterFields, CompetitorsFilterFields,
    SessionsFilterFields, Rule
)


def test_selection_serialization(raw_selection_data, football_competition_ids, football_team_ids):
    selection = Selection.from_dict(raw_selection_data)

    assert selection.name == raw_selection_data["name"]
    assert len(selection.items) == len(raw_selection_data["items"])

    # Football item
    football_item = selection.get_item("it-001")
    assert football_item.sport == SportType.FOOTBALL

    empty_filter = football_item.get_filter("it-001-filt-001")
    assert isinstance(empty_filter.fields, EmptyFilterFields)

    min_ranking_filter = football_item.get_filter("it-001-filt-002")
    assert isinstance(min_ranking_filter.fields, MinRankingFilterFields)
    assert min_ranking_filter.fields.ranking == 5
    assert len(min_ranking_filter.fields.competition_ids) == len(football_competition_ids)
    assert min_ranking_filter.fields.selection_rule.rule == Rule.ANY

    competitors_filter = football_item.get_filter("it-001-filt-003")
    assert isinstance(competitors_filter.fields, CompetitorsFilterFields)
    assert len(competitors_filter.fields.competitor_ids) == len(football_team_ids)
    assert competitors_filter.fields.selection_rule.rule == Rule.OPPONENT
    assert competitors_filter.fields.selection_rule.reference == 40

    # F1 item
    f1_item = selection.get_item("it-002")
    assert f1_item.sport == SportType.F1

    sessions_filter = f1_item.get_filter("it-002-filt-001")
    assert isinstance(sessions_filter.fields, SessionsFilterFields)
    assert sessions_filter.fields.competition_id == 20000000040
    assert len(sessions_filter.fields.sessions) == 2


def test_selection_clone(raw_selection_data):
    selection = Selection.from_dict(raw_selection_data)
    cloned = selection.clone(new_name="cloned_dev")

    assert cloned.name == "cloned_dev"
    assert len(cloned.items) == len(selection.items)
    assert cloned.items is not selection.items, "Cloned selection should be a deep copy of the original"
    assert cloned.items[0].uid != selection.items[0].uid, "Cloned items should have different UIDs"


def test_empty_selection_creation():
    selection = Selection.empty(name="empty_selection")
    assert selection.name == "empty_selection"
    assert len(selection.items) == 0
