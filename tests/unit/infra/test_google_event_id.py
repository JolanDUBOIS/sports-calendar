""" Turning a provider event id into one Google will accept.

Google takes a caller-supplied event id, but only in base32hex — lowercase `a`
to `v` and the digits — so `mch:12345` cannot be used as it stands. Verified
against the live API: an event inserted under the encoded id is retrievable by
it, and re-inserting a *deleted* id returns 409, which is why the sync cancels
events rather than deleting them.
"""

import string

import pytest

from sports_calendar.infra.google_calendar.api_client import (
    google_event_id,
    source_id_from_google_id,
)

LEGAL = set(string.digits + "abcdefghijklmnopqrstuv")


@pytest.mark.parametrize("source_id", [
    "mch:12345",
    "stg:214140",
    "mch:1",
    "t-cpt:1644:mch:987654321",
])
def test_the_id_only_uses_characters_google_accepts(source_id) -> None:
    encoded = google_event_id(source_id)
    assert set(encoded) <= LEGAL, f"illegal characters: {set(encoded) - LEGAL}"
    assert 5 <= len(encoded) <= 1024


def test_it_matches_what_the_live_api_accepted() -> None:
    """ Pinned from the probe run against the real calendar. """
    assert google_event_id("mch:12345") == "dlhmgehh68pj8d8"


def test_the_same_fixture_always_encodes_the_same_way() -> None:
    """ A sync recognises its own events by this, so it cannot drift. """
    assert google_event_id("mch:12345") == google_event_id("mch:12345")


def test_different_fixtures_do_not_collide() -> None:
    ids = {google_event_id(f"mch:{n}") for n in range(500)}
    assert len(ids) == 500


def test_an_event_can_be_traced_back_to_its_fixture() -> None:
    assert source_id_from_google_id(google_event_id("mch:12345")) == "mch:12345"


def test_an_event_we_did_not_write_decodes_to_nothing() -> None:
    """ A sync must leave the user's own calendar entries alone. """
    assert source_id_from_google_id("zzzzzzzz") is None


@pytest.mark.parametrize("hand_made", ["4b2c9d8e7f", "0123456789abcdef"])
def test_undecodable_ids_are_reported_rather_than_raising(hand_made) -> None:
    """ Whatever Google hands back, this must not blow up a sync. """
    result = source_id_from_google_id(hand_made)
    assert result is None or isinstance(result, str)
