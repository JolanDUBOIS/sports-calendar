import pandas as pd

from .processor_base_class import Processor
from src.data_processing import logger


class Parsing(Processor):
    """ TODO """

    def process(self, data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
        """ TODO """
        parse_function_map = {
            "live_soccer_matches": self.parse_livesoccer_matches,
            "live_soccer_standings": self.parse_livesoccer_standings,
            "football_ranking_fifa_ranking": self.parse_football_ranking_fifa_ranking,
        }
        parse_function = parse_function_map.get(source_key)

        if not parse_function:
            logger.debug(f"Unknown source name: {source_key}")
            return data

        return parse_function(data)

    def parse_livesoccer_matches(self, data: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        new_columns = data['title'].apply(self._extract_livesoccer_match_data).apply(pd.Series)
        data = pd.concat([data, new_columns], axis=1)
        new_columns = data['competition_endpoint'].apply(self._parse_livesoccer_competition_endpoint).apply(pd.Series)
        data = pd.concat([data, new_columns], axis=1)
        return data

    def parse_livesoccer_standings(self, data: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        new_columns = data["competition_endpoint"].apply(self._parse_livesoccer_competition_endpoint).apply(pd.Series)
        data = pd.concat([data, new_columns], axis=1)
        return data

    def parse_football_ranking_fifa_ranking(self, data: pd.DataFrame) -> pd.DataFrame:
        """ TODO """
        new_columns = data["team"].apply(self._parse_football_ranking_team).apply(pd.Series)
        data = pd.concat([data, new_columns], axis=1)
        return data

    @staticmethod
    def _parse_livesoccer_competition_endpoint(endpoint: str) -> dict:
        """ TODO """
        return {
            "area": endpoint.split("/")[1],
            "competition": endpoint.split("/")[2].replace("-", " ")
        }

    @staticmethod
    def _extract_livesoccer_match_data(match_title: str) -> dict:
        """ TODO - List the keys of the output dictionary:
        - home_team
        - away_team
        - home_score
        - away_score
        - won_penalty
        - is_final
        """
        match_data = {}
        try:
            match_data["is_final"] = (match_title.split("*")[1].lower() == "final")
        except IndexError:
            match_data["is_final"] = False

        match_title = match_title.split("*")[0].strip()
        if " - " in match_title:
            # Match has started or is finished
            home_data, away_data = match_title.split(" - ")
            
            if home_data[-1] == "P":
                home_data = home_data[:-1]
                match_data["won_penalty"] = "home"
            if home_data[-1].isdigit():
                match_data["home_score"] = int(home_data[-1])
                home_data = home_data[:-1]
            match_data["home_team"] = home_data.strip()
            
            if away_data[0].isdigit():
                match_data["away_score"] = int(away_data[0])
                away_data = away_data[1:]
            if away_data[0] == "P" and away_data[1].isupper():
                match_data["won_penalty"] = "away"
                away_data = away_data[1:]
            match_data["away_team"] = away_data.strip()
        
        elif " vs " in match_title:
            # Match has not started yet
            home_data, away_data = match_title.split(" vs ")
            match_data["home_team"] = home_data.strip()
            match_data["away_team"] = away_data.strip()
        
        else:
            logger.debug(f"Unexpected match title format: {match_title}")
        
        return match_data

    @staticmethod
    def _parse_football_ranking_team(team: str) -> dict:
        """ TODO """
        team = team.split(" (")
        team_name = team[0].strip()
        team_code = team[1].replace(")", "").strip() if len(team) > 1 else None
        return {
            "team_name": team_name,
            "team_code": team_code
        }