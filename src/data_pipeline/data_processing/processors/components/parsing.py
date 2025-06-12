import re

import pandas as pd

from . import logger


def parse(data: pd.DataFrame, source_key: str, **kwargs) -> pd.DataFrame:
    """ Select and apply the appropriate parsing function based on the source key. """
    parse_function_map = {
        "live_soccer_matches": parse_livesoccer_matches,
        "live_soccer_standings": parse_livesoccer_standings,
        "live_soccer_competitions": parse_livesoccer_competitions,
        "football_ranking_fifa_rankings": parse_football_ranking_fifa_ranking,
    }
    parse_function = parse_function_map.get(source_key)

    if not parse_function:
        logger.debug(f"Unknown source name: {source_key}")
        return data

    return parse_function(data)

def parse_livesoccer_matches(data: pd.DataFrame) -> pd.DataFrame:
    """ Parse live soccer matches data, extracting match details and competition info. """
    new_columns = data['title'].apply(_extract_livesoccer_match_data).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    new_columns = data['competition_endpoint'].apply(_parse_livesoccer_competition_endpoint).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    return data

def parse_livesoccer_standings(data: pd.DataFrame) -> pd.DataFrame:
    """ Parse live soccer standings data by extracting competition details. """
    new_columns = data["competition_endpoint"].apply(_parse_livesoccer_competition_endpoint).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    return data

def parse_livesoccer_competitions(data: pd.DataFrame) -> pd.DataFrame:
    """ Parse live soccer competitions data by extracting area and competition name. """
    new_columns = data["section_title"].apply(_parse_livesoccer_competition_section_title).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    new_columns = data["name"].apply(_parse_livesoccer_competition_name).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    return data

def parse_football_ranking_fifa_ranking(data: pd.DataFrame) -> pd.DataFrame:
    """ Parse FIFA football ranking data by extracting team name and code. """
    new_columns = data["team"].apply(_parse_football_ranking_team).apply(pd.Series)
    data = pd.concat([data, new_columns], axis=1)
    return data

def _parse_livesoccer_competition_endpoint(endpoint: str) -> dict:
    """ Extract area and competition name from competition endpoint string. """
    return {
        "area": endpoint.split("/")[1],
        "competition": endpoint.split("/")[2].replace("-", " ")
    }

def _extract_livesoccer_match_data(match_title: str) -> dict:
    """
    Extract detailed match data from a match title string.
    Always returns the keys: home_team, away_team, home_score, away_score, won_penalty, is_final.
    """
    match_data = {
        "home_team": None,
        "away_team": None,
        "home_score": None,
        "away_score": None,
        "won_penalty": None,
        "is_final": False,
    }

    try:
        match_data["is_final"] = (match_title.split("*")[1].lower() == "final")
    except IndexError:
        match_data["is_final"] = False

    match_title = match_title.split("*")[0].strip()
    if " - " in match_title:
        # Match has started or is finished
        home_data, away_data = match_title.split(" - ")

        if home_data[-1].isdigit():
            match_data["home_score"] = int(home_data[-1])
            home_data = home_data[:-1]
            if home_data[-1] == "P":
                home_data = home_data[:-1]
                match_data["won_penalty"] = "home"
        match_data["home_team"] = home_data.strip()

        if away_data[0].isdigit():
            match_data["away_score"] = int(away_data[0])
            away_data = away_data[1:]
            if away_data != "PSG" and away_data[0] == "P" and away_data[1].isupper(): # Added the condition to handle PSG which was causing issues
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

def _parse_livesoccer_competition_section_title(section_title: str) -> dict:
    """ Extract region and confederation from a section title string. """
    match = re.match(r"^(.*?)\s*\((.*?)\)$", section_title)
    if match:
        return {
            "region": match.group(1).strip(),
            "confederation": match.group(2).strip()
        }
    else:
        return {
            "region": section_title.strip(),
            "confederation": section_title.strip()
        }

def _parse_livesoccer_competition_name(competition_name: str) -> dict:
    """ Extract area and competition name from a competition name string. """
    parts = competition_name.split(" - ")
    if len(parts) == 2:
        competition = parts[1].strip()
        if competition not in ["Men", "Women"]:
            return {
                "area": parts[0].strip(),
                "competition": parts[1].strip()
            }
    return {
        "area": None,
        "competition": competition_name.strip()
    }

def _parse_football_ranking_team(team: str) -> dict:
    """ Extract team name and optional team code from a team string. """
    team = team.split(" (")
    team_name = team[0].strip()
    team_code = team[1].replace(")", "").strip() if len(team) > 1 else None
    return {
        "team_name": team_name,
        "team_code": team_code
    }
