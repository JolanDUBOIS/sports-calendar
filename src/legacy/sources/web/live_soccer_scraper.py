import pytz, traceback
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup # type: ignore

from src.sources import logger
from .base_scraper import BaseScraper


class LiveSoccerScraper(BaseScraper):
    """ TODO """

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)

    @property
    def base_url(self) -> str:
        """ Getter for the base_url property. """
        return "https://www.livesoccertv.com"

    @property
    def source_name(self):
        """ Getter for the source_name property. """
        return "livesoccertv"

    def get_matches(
        self,
        competition_id: str = None,
        team_id: str = None,
        date_from: str = None,
        date_to: str = None,
        **kwargs
    ) -> pd.DataFrame:
        """ TODO """
        # TODO - split this function into smaller functions
        # TODO - add error handling

        # Temporary I hope
        if not competition_id:
            logger.error("No competition_id provided. Returning empty DataFrame.")
            return pd.DataFrame()
        if team_id:
            logger.warning("LiveSoccerScraper only supports competition_id. Ignoring team_id.")

        if date_from or date_to:
            logger.warning("LiveSoccerScraper does not support date filtering. Ignoring date_from and date_to.")

        competition_route = self.competition_registry.get_route(self.source_name, competition_id)
        competition_name = self.competition_registry.get_name_by_id(competition_id)
        url = f"{self.base_url}/competitions{competition_route}"

        counter = 0
        matches_df = pd.DataFrame() # TODO - ensure proper columns 
        while counter < 5:
            soup = self.scrape_url(url)
            new_matches = self._get_matches(soup, competition_id)
            if new_matches.empty:
                logger.debug(f"No more matches found for competition: {competition_name}")
                break
            matches_df = pd.concat([matches_df, new_matches], ignore_index=True)
            last_match = matches_df.iloc[-1]
            url = self.get_next_url(soup, last_match, url)
            if not url:
                logger.debug(f"No next URL found for competition: {competition_name}")
                break
            counter += 1

        return matches_df        

    def _get_matches(self, soup: BeautifulSoup, competition_id: str) -> pd.DataFrame:
        """ TODO """
        current_date, matches = None, []
        for row in soup.find_all('tr'):
            # Check if row is a match
            if 'matchrow' in row.get('class', []):                
                try:
                    if 'repeatrow' in row.get('class', []):
                        # Skip repeated rows
                        continue

                    # Match time
                    time_cell = row.find('span', class_='timecell')
                    time_value = time_cell.find('span', class_='ts')['dv'] if time_cell else None
                    time_value_sl = time_cell.get_text(strip=True) if time_cell else None  # SoccerLiveTV weird timezone time

                    if time_value:
                        # Convert timestamp (milliseconds) to seconds for datetime
                        timestamp = int(time_value) / 1000  # Convert milliseconds to seconds

                        # Get UTC time and convert it to the Europe/Paris timezone
                        paris_tz = pytz.timezone('Europe/Paris')
                        utc_time = datetime.fromtimestamp(timestamp, tz=pytz.utc)  # Using fromtimestamp with UTC

                        # Now localized_time will give you the correct time in Paris time zone
                        localized_time = utc_time.astimezone(paris_tz)

                        # Format time as 'HH:MM'
                        time_value = localized_time.strftime('%H:%M')

                    # Teams
                    match = row.find('td', id='match')
                    match_text = match.get_text(strip=True) if match else None
                    if " - " in match_text:  # Match is in progress or over
                        home_info, away_info = match_text.split(" - ")
                        home_team = home_info[:-1]
                        away_team = away_info[1:]
                    elif " vs " in match_text:
                        home_team, away_team = match_text.split(" vs ")
                    else:
                        home_team, away_team = None, None

                    # Channels
                    channels_div = row.find('td', id='channels')
                    channels = [a.get_text(strip=True) for a in channels_div.find_all("a")] if channels_div else []

                    # Store the data
                    matches.append({
                        "title": match_text,
                        "date_time": current_date,
                        "ls_date_time": time_value,
                        "original_time(ls_tz)": time_value_sl,
                        "home_team_source_name": home_team,
                        "away_team_source_name": away_team,
                        "home_team_id": self.team_registry.get_id_by_alias(home_team),
                        "away_team_id": self.team_registry.get_id_by_alias(away_team),
                        "competition_id": competition_id,
                        "channels": channels
                    })
                except Exception as e:
                    logger.debug(f"Error parsing match row: {e}")
                    logger.debug(traceback.format_exc())
                    continue

            # Check if the row contains a date (not a match row)
            elif row.find('a') and ',' in row.get_text():
                parsed_date = datetime.strptime(row.get_text(strip=True), "%A, %d %B")
                today = datetime.today()
                final_date = parsed_date.replace(year=today.year)
                lower_bound = today - timedelta(days=60)   # Approx. -2 months
                upper_bound = today + timedelta(days=300)  # Approx. +10 months
                if final_date < lower_bound:
                    final_date = final_date.replace(year=today.year + 1)
                elif final_date > upper_bound:
                    final_date = final_date.replace(year=today.year - 1)
                current_date = final_date.strftime("%Y-%m-%d")

        return pd.DataFrame(matches)
        

    def get_standings(self, league_id: str, date: str = None, **kwargs) -> pd.DataFrame:
        """ TODO """
        league_route = self.competition_registry.get_route(self.source_name, league_id)
        league_name = self.competition_registry.get_name_by_id(league_id)
        url = f"{self.base_url}/competitions{league_route}"
        soup = self.scrape_url(url)
        if not soup:
            logger.debug(f"Failed to fetch standings for competition: {league_name}")
            return pd.DataFrame()
        table = soup.find('table', {'class': 'standings'})
        if not table:
            logger.debug(f"No standings table found for competition: {league_name}")
            return pd.DataFrame()
        standings = []
        for row in table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) < 11:
                if "group" in row.find('th').text.strip().lower():
                    # Skip group rows, not implemented
                    logger.debug(f"Skipping group row in table: {row}")
                else:
                    logger.debug(f"Unexpected row format in table: {row}")
                continue

            team_name = cols[2].text.strip()
            standings.append({
                'competition_id': league_id,
                'position': cols[0].text.strip(),
                'team_source_name': team_name,
                'team_id': self.team_registry.get_id_by_alias(team_name),
                'matches_played': cols[3].text.strip(),
                'wins': cols[4].text.strip(),
                'draws': cols[5].text.strip(),
                'losses': cols[6].text.strip(),
                'goals_for': cols[7].text.strip(),
                'goals_against': cols[8].text.strip(),
                'goal_difference': cols[9].text.strip(),
                'points': cols[10].text.strip()
            })

        standings_df = pd.DataFrame(standings)
        return standings_df

    def get_next_url(self, soup: BeautifulSoup, last_match: pd.Series, competition_url: str) -> str|None:
        """ TODO """
        match_date = last_match['Date']
        match_time_ls = last_match['Original Time (SL TZ)']
        home_team = last_match['Home Team']
        away_team = last_match['Away Team']

        pageid = self.extract_pageid(soup)
        if not pageid:
            return None

        match_datetime = f"{match_date}%20{match_time_ls}:00"
        game = f"{home_team} vs {away_team}"
        game = quote_plus(game)

        next_url = f"{self.base_url}/xschedule.php?direction=next&pagetype=competition&pageid={pageid}&start={match_datetime}&game={game}&xml=1&tab=_live&page=1&pageurl={quote_plus(competition_url)}"
        return next_url

    @staticmethod
    def extract_pageid(soup: BeautifulSoup) -> int:
        """ TODO """
        next_button = soup.find('div', class_='pagination-right')

        if next_button:
            onclick_values = next_button.get('onclick', '').split(',')
            if len(onclick_values) >= 3:
                try:
                    pageid = int(onclick_values[2].strip().strip("'"))
                    return pageid
                except ValueError:
                    logger.debug("Failed to extract pageid from the page.")
                    return None

        logger.debug("Failed to extract pageid from the page.")
        return None