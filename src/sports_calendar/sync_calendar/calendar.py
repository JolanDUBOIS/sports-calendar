from pathlib import Path

from icalendar import Calendar

from . import logger
from .events import SportsEventCollection, SportsEvent


class SportsCalendar:
    """ Abstract base class for sports calendars """

    def __init__(self, calendar: Calendar = None):
        """ Initialize the sports calendar with an optional icalendar Calendar object """
        self.calendar = calendar if calendar else Calendar()

    def add_events(self, events: SportsEventCollection) -> None:
        """ Add events to the calendar """
        for event in events:
            self.add_event(event)

    def add_event(self, event: SportsEvent) -> None:
        """ Add a single event to the calendar """
        self.calendar.add_component(event.get_event())

    def save_to_ics(self, path: Path) -> None:
        """ Save the calendar to an ICS file """
        path_suffix = path.suffix.lower()
        if path_suffix != '.ics':
            logger.error(f"Invalid file extension: {path_suffix}. Expected .ics")
            raise ValueError(f"Invalid file extension: {path_suffix}. Expected .ics")
        with open(path, 'wb') as f:
            f.write(self.calendar.to_ical())
        logger.info(f"Calendar saved to {path}")

    def __str__(self):
        """ String representation of the calendar (first 10 events) """
        events = list(self.calendar.subcomponents)
        event_strs = [str(event) for event in events[:10]]
        if len(events) > 10:
            event_strs.append(f"... and {len(events) - 10} more events.")
        return "\n".join(event_strs)
