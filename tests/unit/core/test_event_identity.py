""" Events carry the provider's id, so a sync can recognise what it wrote.

Without it there is no way to tell an event already on the calendar from a new
one, which forces the sync to wipe and rewrite everything each night. The id
must come from the provider rather than from the event's own details: a Ligue 1
fixture is pencilled in for a Sunday and only given its real kick-off about a
month out, and it is the same match throughout.
"""

from datetime import UTC, datetime

from sports_calendar.core.calendar.events.match import MatchEvent
from sports_calendar.core.calendar.events.stage import StageEvent

START = datetime(2026, 8, 23, 18, 45, tzinfo=UTC)


def _match(**overrides) -> MatchEvent:
    fields = {
        "start": START,
        "sport": "football",
        "home_competitor_name": "Paris Saint-Germain",
        "away_competitor_name": "Stade Rennais",
        "competition_name": "Ligue 1",
        "source_id": "mch:12345",
    }
    return MatchEvent(**{**fields, **overrides})


def test_the_provider_id_reaches_the_calendar_event() -> None:
    assert str(_match().get_event()["uid"]) == "mch:12345"


def test_the_id_survives_a_reschedule() -> None:
    """ The whole point: a moved fixture is the same event, not a new one. """
    original = _match()
    moved = _match(start=datetime(2026, 8, 24, 20, 0, tzinfo=UTC))

    assert original.source_id == moved.source_id
    assert original.get_event()["uid"] == moved.get_event()["uid"]
    assert original.get_event()["dtstart"] != moved.get_event()["dtstart"]


def test_an_event_without_a_source_id_writes_no_uid() -> None:
    """ Rather than writing an empty one, which would collide across events. """
    assert "uid" not in _match(source_id=None).get_event()


def test_a_stage_event_carries_one_too() -> None:
    stage = StageEvent(
        start=START,
        end=None,
        sport="motorsport",
        name="Netherlands GP",
        session="Race",
        source_id="stg:214140",
    )
    assert str(stage.get_event()["uid"]) == "stg:214140"


def test_the_other_fields_are_unaffected() -> None:
    event = _match().get_event()
    assert str(event["summary"]) == _match().summary
    assert event["dtstart"].dt == START
