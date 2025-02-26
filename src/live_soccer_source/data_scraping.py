import re, json, time, pytz, traceback
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import cloudscraper # type: ignore
import pandas as pd
from bs4 import BeautifulSoup # type: ignore

from src.live_soccer_source import logger


class SoccerLiveScraper:
    """ TODO """

    BASE_URL = "https://www.livesoccertv.com/"
    competitions_data_file_path = Path(__file__).resolve().parent / "competitions_data.json"
    database_path = Path("data") / "live_soccer"

    def __init__(self, wait_time: int=20):
        """ TODO """
        self.wait_time = wait_time
        self._competitions_data = None
    
    @property
    def competitions_data(self) -> dict:
        """ TODO """
        if self._competitions_data is None:
            with self.competitions_data_file_path.open(mode='r') as file:
                self._competitions_data = json.load(file)
        return self._competitions_data
    
    def get_url_response(self, url: str, max_retries=5) -> BeautifulSoup:
        """ TODO """
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)

            # Retry if rate limit is hit (HTTP 429)
            retries = 0
            while response.status_code == 429 and retries < max_retries:  # Too many requests
                logger.warning(f"Rate limit hit, waiting for {self.wait_time} seconds...")
                time.sleep(self.wait_time)
                response = scraper.get(url)
                retries += 1

            if retries == max_retries:
                logger.error(f"Max retries reached ({max_retries}). Could not fetch URL: {url}")
                return None

            # Handle other error codes
            if response.status_code != 200:
                logger.error(f"Error {response.status_code} while accessing URL: {url}")
                return None

        except Exception as e:
            logger.error(f"Error while scraping: {e}")
            logger.debug(traceback.format_exc())
            return None

        return response

    def get_competition(self, competition: str) -> tuple[pd.DataFrame|None, pd.DataFrame|None]:
        """ TODO - Add more error handling """
        logger.debug(f"Fetching data for competition: {competition}")
        url = f"{self.BASE_URL}competitions/{self.competitions_data[competition]['endpoint']}"
    
        response = self.get_url_response(url)
        if not response:
            return None, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Get standings
        standings_df = self.get_standings(soup, competition)

        # Get matches
        matches_df = self.get_matches(soup, competition)
        
        # Get next page
        if not matches_df.empty:
            last_match = matches_df.iloc[-1]
            next_url = self.get_next_url(soup, last_match, url)
            response = self.get_url_response(next_url)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                next_matches = self.get_matches(soup, competition)
                matches_df = pd.concat([matches_df, next_matches], ignore_index=True)

        return matches_df, standings_df

    def get_standings(self, soup: BeautifulSoup, competition: str) -> pd.DataFrame|None:
        """ TODO """
        table = soup.find('table', {'class': 'standings'})

        if not table:
            logger.debug(f"No standings table found for competition: {competition}")
            return None

        else:
            data = []
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) < 11:
                    if "group" in row.find('th').text.strip().lower():
                        # Skip group rows, not implemented
                        logger.debug(f"Skipping group row in table: {row}")
                    else:
                        logger.error(f"Unexpected row format in table: {row}")
                    continue

                data.append({
                    'Competition': competition,
                    'Position': cols[0].text.strip(),
                    'Team': cols[2].text.strip(),
                    'Matches Played': cols[3].text.strip(),
                    'Wins': cols[4].text.strip(),
                    'Draws': cols[5].text.strip(),
                    'Losses': cols[6].text.strip(),
                    'Goals For': cols[7].text.strip(),
                    'Goals Against': cols[8].text.strip(),
                    'Goal Difference': cols[9].text.strip(),
                    'Points': cols[10].text.strip()
                })

            return pd.DataFrame(data)
    
    def get_matches(self, soup: BeautifulSoup, competition: str) -> pd.DataFrame:
        """ TODO """
        current_date, matches = None, []
        for row in soup.find_all('tr'):
            # Check if row is a match
            if 'matchrow' in row.get('class', []):
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
                    "Title": match_text,
                    "Date": current_date,
                    "SL Time": time_value,
                    "Time": self.closest_lower_time(time_value, time_step=10),
                    "Original Time (SL TZ)": time_value_sl,
                    "Home Team": home_team,
                    "Away Team": away_team,
                    "Competition": competition,
                    "Region": self.competitions_data[competition]["region"],
                    "Channels": channels
                })

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
        
        base_url = f"{self.BASE_URL}xschedule.php"
        next_url = f"{base_url}?direction=next&pagetype=competition&pageid={pageid}&start={match_datetime}&game={game}&xml=1&tab=_live&page=1&pageurl={quote_plus(competition_url)}"
        return next_url

    def update_database(self, competitions: list[str]=None):
        """ TODO """
        logger.info("---------- Updating database... ----------")

        matches_file_path = self.database_path / f"live_soccer_matches.csv"
        standings_file_path = self.database_path / f"live_soccer_standings.csv"
        
        if competitions is None:
            competitions = list(self.competitions_data.keys())

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        all_standings = pd.DataFrame()
        for competition in competitions:
            logger.info(f"Updating database for competition: {competition}")
            try:
                matches, standings = self.get_competition(competition)
                if matches is not None:
                    matches['created_at'] = now
                    if matches_file_path.exists():
                        matches.to_csv(matches_file_path, mode='a', header=False, index=False)
                    else:
                        matches.to_csv(matches_file_path, index=False)
                    logger.debug(f"Added matches for {competition} to database.")
                if standings is not None:
                    all_standings = pd.concat([all_standings, standings], ignore_index=True)
                    logger.debug(f"Added standings for {competition} to database.")
            except Exception as e:
                logger.error(f"Error while updating database for competition {competition}: {e}")
                logger.debug(traceback.format_exc())
        
        if not all_standings.empty:
            all_standings['created_at'] = now
            try:
                old_standings = pd.read_csv(standings_file_path)
            except FileNotFoundError:
                old_standings = pd.DataFrame(columns=['Competition', 'Position', 'Team', 'Matches Played', 'Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against', 'Goal Difference', 'Points', 'created_at'])
            updated_standings = pd.concat(
                [old_standings[~old_standings['Competition'].isin(all_standings['Competition'])], all_standings],
                ignore_index=True
            )
            updated_standings.to_csv(standings_file_path, index=False)
        
        logger.info("---------- Database updated successfully ----------")
    
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
                    logger.error("Failed to extract pageid from the page.")
                    return None

        logger.error("Failed to extract pageid from the page.")
        return None

    @staticmethod
    def closest_lower_time(time_str: str, time_step: int=10) -> str:
        """ TODO """
        time_obj = datetime.strptime(time_str, "%H:%M")
        
        minutes = (time_obj.minute // time_step) * time_step
        closest_time = time_obj.replace(minute=minutes, second=0, microsecond=0)
        
        return closest_time.strftime("%H:%M")
