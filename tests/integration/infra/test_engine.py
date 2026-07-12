# from sportindex import SportClient, EventCollection

# from sports_calendar.infra.engine import Resolver


# def test_resolver(sample_selection):
#     resolver = Resolver()
#     client = SportClient()

#     resolved = resolver.resolve_selection(sample_selection, client)
#     assert isinstance(resolved, list), "Resolved result should be a list"
#     assert all(isinstance(pair, tuple) and len(pair) == 2 for pair in resolved), "Each resolved item should be a tuple of (EventCollection, SportType)"
#     assert all(isinstance(pair[0], EventCollection) for pair in resolved), "First element of each resolved pair should be an EventCollection"
#     assert len(resolved) > 0, "Resolved collection should contain events"
#     print(resolved)
#     for collection, sport in resolved:
#         print(f"Sport: {sport}, Events: {collection.to_list()}")
