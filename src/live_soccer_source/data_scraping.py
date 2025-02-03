import json, time, pytz, traceback
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import cloudscraper # type: ignore
from bs4 import BeautifulSoup # type: ignore

from src.live_soccer_source import logger


class SoccerLiveScraper:
    """ TODO """

    base_url = "https://www.livesoccertv.com/"
    competitions_data_file_path = Path(__file__).resolve().parent / "competitions_data.json"
    database_path = Path("data")

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

    def get_competition(self, competition: str) -> tuple[pd.DataFrame|None, pd.DataFrame|None]:
        """ TODO - Add more error handling """
        logger.info(f"Fetching data for competition: {competition}")
        url = f"{self.base_url}competitions/{self.competitions_data[competition]['endpoint']}"
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)

            if response.status_code != 200:
                if response.status_code == 429:  # Too many requests
                    logger.warning(f"Rate limit hit, waiting for {self.wait_time} seconds...")
                    time.sleep(self.wait_time)
                    return self.get_competition(competition)
                logger.error(f"Error {response.status_code} while accessing URL: {url}")
                return None, None

        except Exception as e:
            logger.error(f"Error while scraping standings: {e}")
            logger.debug(traceback.format_exc())
            return None, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Get matches
        current_date, matches = None, []
        for row in soup.find_all('tr'):
            # Check if row is a match
            if 'matchrow' in row.get('class', []):
                # Match time
                time_cell = row.find('span', class_='timecell')
                # time_value = time_cell.get_text() if time_cell else None

                # NEW
                time_value = time_cell.find('span', class_='ts')['dv'] if time_cell else None

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

        matches_df = pd.DataFrame(matches)

        # Get standings
        table = soup.find('table', {'class': 'standings'})

        if not table:
            logger.info(f"No standings table found for competition: {competition}")
            return matches_df, None

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

        standings_df = pd.DataFrame(data)

        return matches_df, standings_df

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
            logger.info(f"----- Updating database for competition: {competition} -----")
            try:
                matches, standings = self.get_competition(competition)
                if matches is not None:
                    matches['created_at'] = now
                    if matches_file_path.exists():
                        matches.to_csv(matches_file_path, mode='a', header=False, index=False)
                    else:
                        matches.to_csv(matches_file_path, index=False)
                    logger.info(f"Added matches for {competition} to database.")
                if standings is not None:
                    all_standings = pd.concat([all_standings, standings], ignore_index=True)
                    logger.info(f"Added standings for {competition} to database.")
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
    def closest_lower_time(time_str: str, time_step: int=10) -> str:
        """ TODO """
        time_obj = datetime.strptime(time_str, "%H:%M")
        
        minutes = (time_obj.minute // time_step) * time_step
        closest_time = time_obj.replace(minute=minutes, second=0, microsecond=0)
        
        return closest_time.strftime("%H:%M")
