from sportindex import SportClient

from sports_calendar.application.selection import SelectionService


class AppContext:
    def __init__(self):
        self.client: SportClient | None = None

    def boot(self):
        """Called exactly once on application startup."""
        self.client = SportClient()
        SelectionService.initialize_registry()

app_context = AppContext()
