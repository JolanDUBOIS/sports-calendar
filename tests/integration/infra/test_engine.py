from sportindex import EventCollection, SportClient

from sports_calendar.infra.engine import Resolver


def test_resolver(sample_selection):
    resolver = Resolver()
    client = SportClient()

    resolved = resolver.resolve_selection(sample_selection, client)
    assert isinstance(resolved, list), "Resolved result should be a list"
    assert all(isinstance(pair, tuple) and len(pair) == 2 for pair in resolved), "Each resolved item should be a tuple of (EventCollection, int)"
    assert all(isinstance(pair[0], EventCollection) for pair in resolved), "First element of each resolved pair should be an EventCollection"
    assert all(isinstance(pair[1], int) for pair in resolved), "Second element of each resolved pair should be the item's sport_id"
    assert len(resolved) > 0, "Resolved collection should contain one entry per selection item"
    for collection, sport_id in resolved:
        print(f"Sport ID: {sport_id}, Events: {list(collection)}")
