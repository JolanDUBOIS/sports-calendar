from src.clients.api.base_api_client import BaseApiClient


class ESPNApiClient(BaseApiClient):
    """ TODO """
    
    base_url = "https://site.api.espn.com/apis/site/v2/sports/soccer/"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    def query_matches(self, competition_slug: str, date_str: str) -> list[dict]:
        """ TODO """
        url = f"{self.base_url}{competition_slug}/scoreboard?dates={date_str}"
        response = self.query_api(url)
        if not response:
            return []
        return response.get('events', [])
