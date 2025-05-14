import numpy as np
import pandas as pd

from .processor_base_class import Processor
from src.data_processing import logger


class ReshapeMatches(Processor):
    """ Reshape matches data """

    def process(self, data: pd.DataFrame, source_key, **kwargs) -> pd.DataFrame:
        """ TODO """
        reshape_function_map = {
            "espn_matches": self.reshape_espn_matches,
        }
        reshape_function = reshape_function_map.get(source_key)

        if not reshape_function:
            logger.debug(f"Unknown source name: {source_key}")
            return data
        
        return reshape_function(data)

    # @staticmethod
    # def reshape_espn_matches(data: pd.DataFrame) -> pd.DataFrame:
    #     """ Restructure ESPN matches data """
    #     home, away = "A", "B" if data["team_A_homeAway"] == "home" else "B", "A"
        
    #     data["home_team_id"] = data[f"team_{home}_id"]
    #     data["home_team_winner"] = data[f"team_{home}_winner"]
    #     data["home_team_score"] = data[f"team_{home}_score"]
    #     data["home_team_abbreviation"] = data[f"team_{home}_abbreviation"]
    #     data["home_team_displayName"] = data[f"team_{home}_displayName"]
    #     data["home_team_shortDisplayName"] = data[f"team_{home}_shortDisplayName"]
    #     data["home_team_name"] = data[f"team_{home}_name"]
    #     data["home_team_location"] = data[f"team_{home}_location"]
    #     data["home_team_venue_id"] = data[f"team_{home}_venue_id"]

    #     data["away_team_id"] = data[f"team_{away}_id"]
    #     data["away_team_winner"] = data[f"team_{away}_winner"]
    #     data["away_team_score"] = data[f"team_{away}_score"]
    #     data["away_team_abbreviation"] = data[f"team_{away}_abbreviation"]
    #     data["away_team_displayName"] = data[f"team_{away}_displayName"]
    #     data["away_team_shortDisplayName"] = data[f"team_{away}_shortDisplayName"]
    #     data["away_team_name"] = data[f"team_{away}_name"]
    #     data["away_team_location"] = data[f"team_{away}_location"]
    #     data["away_team_venue_id"] = data[f"team_{away}_venue_id"]

    #     cols_to_drop = [
    #         "team_A_id", "team_B_id",
    #         "team_A_homeAway", "team_B_homeAway",
    #         "team_A_winner", "team_B_winner",
    #         "team_A_score", "team_B_score",
    #         "team_A_abbreviation", "team_B_abbreviation",
    #         "team_A_displayName", "team_B_displayName",
    #         "team_A_shortDisplayName", "team_B_shortDisplayName",
    #         "team_A_name", "team_B_name",
    #         "team_A_location", "team_B_location",
    #         "team_A_venue_id", "team_B_venue_id"
    #     ]
    #     data.drop(columns=cols_to_drop, inplace=True)
    #     return data

    @staticmethod
    def reshape_espn_matches(data: pd.DataFrame) -> pd.DataFrame: # TODO - Test
        home_is_A = data["team_A_homeAway"] == "home"
        home = np.where(home_is_A, "A", "B")
        away = np.where(home_is_A, "B", "A")

        for attr in [
            "id", "winner", "score", "abbreviation", "displayName",
            "shortDisplayName", "name", "location", "venue_id"
        ]:
            data[f"home_team_{attr}"] = [
                data.at[i, f"team_{h}_{attr}"] for i, h in zip(data.index, home)
            ]
            data[f"away_team_{attr}"] = [
                data.at[i, f"team_{a}_{attr}"] for i, a in zip(data.index, away)
            ]

        data.drop(columns=[col for col in data.columns if col.startswith("team_")], inplace=True)

        return data
  