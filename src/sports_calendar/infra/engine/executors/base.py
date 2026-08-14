import logging
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from sportindex import Competition, EventCollection, SportClient
from sportindex.exceptions import DomainError, ProviderNotFoundError, SportIndexError

from sports_calendar.core import EntityId
from sports_calendar.core.selection import FilterFields

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=FilterFields)

class BaseExecutor(ABC, Generic[T]):
    """ Base class for all executors. """

    @staticmethod
    def fetch_current_fixtures(competition_id: EntityId, client: SportClient) -> EventCollection:
        """ Fixtures for a competition's current season, or empty if unavailable.

        A saved filter outlives the competition it names: the id starts
        404-ing, or the competition survives with no seasons attached. Neither
        should take down a whole calendar build — one stale filter would then
        cost the user every other event they follow — so this logs and yields
        nothing instead of raising.

        The same is true of a request that simply fails. `seasons` and
        `get_fixtures` both fetch lazily, and a single flaky one used to raise
        out of the whole build: one failure on `/unique-stage/40/seasons` cost
        every event in every sport.

        **This hides failures from the caller.** A build that quietly returns
        less than it should must never be published on top of a good one — see
        `run_selection`, which clears future events before adding.
        """
        try:
            competition = client.get(competition_id, Competition)
            if competition is None:
                logger.warning(f"Competition {competition_id} not found. Skipping.")
                return EventCollection()

            seasons = competition.seasons
            if not seasons:
                logger.warning(f"Competition {competition_id} has no seasons. Skipping.")
                return EventCollection()

            return seasons[0].get_fixtures()

        except ProviderNotFoundError:
            logger.warning(f"Competition {competition_id} is no longer available. Skipping.")
            return EventCollection()

        except (SportIndexError, DomainError):
            logger.exception(f"Could not read fixtures for competition {competition_id}. Skipping.")
            return EventCollection()

    @classmethod
    @abstractmethod
    def fetch(cls, filter_fields: T, client: SportClient) -> EventCollection:
        """ Fetch events using the given filter fields and the provided SportClient. """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def apply(cls, filter_fields: T, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the given filter fields to the provided events. """
        raise NotImplementedError
