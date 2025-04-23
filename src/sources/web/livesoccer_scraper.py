import json, time, pytz, traceback, requests
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import cloudscraper # type: ignore
import pandas as pd
from bs4 import BeautifulSoup # type: ignore

from src.sources import logger


class LiveSoccerScraper:
    """ TODO """

    urls_config_file_path = Path(__file__).resolve().parent.parent / "config" / "livesoccertv_urls.json"

    def __init__(self, wait_time: int=20):
        """ TODO """
        self.wait_time = wait_time
        
        self._urls_data = None
        self._competitions_endpoints = None

    @property
    def urls_data(self) -> dict:
        """ TODO """
        if self._urls_data is None:
            with self.urls_config_file_path.open(mode='r') as file:
                self._urls_data = json.load(file)
        return self._urls_data

    @property
    def base_url(self) -> str:
        """ TODO """
        return self.urls_data["base_url"]
    
    @property
    def competitions_endpoints(self) -> dict:
        """ TODO """
        if self._competitions_endpoints is None:
            self._competitions_endpoints = {
                competition_name: competition_data['endpoint']
                for _, region_competitions in self.urls_data["competitions"].items()
                for competition_name, competition_data in region_competitions.items()
            }
        return self._competitions_endpoints
    
    def get_url_response(self, url: str, max_retries=5) -> requests.Response|None:
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

    def get_competition(self, competition: str, max_iter: int=4) -> tuple[pd.DataFrame|None, pd.DataFrame|None]:
        """ TODO - Add more error handling """
        logger.debug(f"Fetching data for competition: {competition}")
        url = f"{self.base_url}{self.competitions_endpoints[competition]}"

        response = self.get_url_response(url)
        if not response:
            return None, None

        soup = BeautifulSoup(response.text, 'html.parser')

        # Get standings
        standings_df = self.get_standings(soup, competition)
                    
        # Get matches
        matches_df = self.get_matches(soup, competition)
        try:
            for i in range(max_iter):
                last_match = matches_df.iloc[-1]
                next_url = self.get_next_url(soup, last_match, url)
                if next_url:
                    response = self.get_url_response(next_url)
                    if response:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        next_matches = self.get_matches(soup, competition)
                        matches_df = pd.concat([matches_df, next_matches], ignore_index=True)
                    else:
                        break
                else:
                    break
        except Exception as e:
            logger.error(f"Error while fetching next matches: {e}")
            logger.debug(traceback.format_exc())

        return matches_df, standings_df

    # def get_competitions(self) -> pd.DataFrame:
    #     """ TODO """
    #     logger.debug("Fetching all competitions...")
    #     url = f"{self.BASE_URL}competitions/"

    #     response = self.get_url_response(url)
    #     if not response:
    #         return None

    #     soup = BeautifulSoup(response.text, 'html.parser')
    #     competitions = []
    #     for section in soup.find_all("div", class_="r-section"):
    #         region = section.find("h2").text.strip()
    #         for comp in section.find_all("a", class_="flag world"):
    #             name = comp.text.strip()
    #             url = comp["href"]
    #             competitions.append({"Region": region, "Competition": name, "URL": url})

    #     return pd.DataFrame(competitions)

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
                        logger.debug(f"Unexpected row format in table: {row}")
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
                    'Points': cols[10].text.strip(),
                    'Is League': self.is_league(competition)
                })

            return pd.DataFrame(data)
    
    def get_matches(self, soup: BeautifulSoup, competition: str) -> pd.DataFrame:
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
                        "Title": match_text,
                        "Date": current_date,
                        "SL Time": time_value,
                        "Time": self.closest_lower_time(time_value, time_step=10),
                        "Original Time (SL TZ)": time_value_sl,
                        "Home Team": home_team,
                        "Away Team": away_team,
                        "Competition": competition,
                        "Region": self.get_region(competition),
                        "Channels": channels
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
    
    def get_region(self, competition: str) -> str:
        """ TODO """
        for region, competitions in self.urls_data["competitions"].items():
            if competition in competitions:
                return region
        logger.warning(f"Competition '{competition}' not found in any region.")
        return None
    
    def get_competitions(self, region: str=None) -> list[str]:
        """ TODO """
        if region is None:
            return list(self.competitions_endpoints.keys())
        return list(self.urls_data["competitions"][region].keys())

    def is_league(self, competition: str) -> bool:
        """ TODO """
        for region, competitions in self.urls_data["competitions"].items():
            if competition in competitions:
                return competitions[competition].get("is_league", False)
        logger.warning(f"Competition '{competition}' not found in any region.")
        return False

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

    @staticmethod
    def closest_lower_time(time_str: str, time_step: int=10) -> str:
        """ TODO """
        time_obj = datetime.strptime(time_str, "%H:%M")
        
        minutes = (time_obj.minute // time_step) * time_step
        closest_time = time_obj.replace(minute=minutes, second=0, microsecond=0)
        
        return closest_time.strftime("%H:%M")
