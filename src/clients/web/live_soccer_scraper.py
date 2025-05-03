import traceback
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup, Tag

from src.clients import logger
from src.clients.web.base_scraper import BaseScraper


class LiveSoccerScraper(BaseScraper):
    """ TODO """
    
    base_url = "https://www.livesoccertv.com"

    def __init__(self, **kwargs):
        """ TODO """
        super().__init__(**kwargs)
        
    # Scrape competitions
    
    def scrape_competitions(self) -> pd.DataFrame:
        """ TODO """
        logger.info("Scraping competitions...")
        url = f"{self.base_url}/competitions"
        soup = self.scrape_url(url)
        if not soup:
            logger.debug("Failed to fetch competitions.")
            return pd.DataFrame()

        competitions = []
        for section in soup.select('div.r-section'):
            section_title = section.find('h2').text.strip() if section.find('h2') else None

            for a in section.select('ul.competitions a'):
                name = a.text.strip()
                href = a.get('href')
                if href:
                    competitions.append({
                        'section_title': section_title,
                        'name': name,
                        'endpoint': href
                    })

        return pd.DataFrame(competitions)

    # Scrape standings
    
    def scrape_standings(self, competition_endpoint: str) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Scraping standings for competition at endpoint: {competition_endpoint}")
        url = f"{self.base_url}/competitions{competition_endpoint}"
        soup = self.scrape_url(url)
        if not soup:
            logger.debug(f"Failed to fetch standings for competition at endpoint: {competition_endpoint}")
            return pd.DataFrame()
        table = soup.find('table', {'class': 'standings'})
        if not table:
            logger.debug(f"No standings table found for competitionat endpoint: {competition_endpoint}")
            return pd.DataFrame()
        standings = []
        for row in table.find_all('tr')[1:]:
            parsed_row = self._parse_standings_row(row)
            if parsed_row:
                standings.append(parsed_row)

        standings_df = pd.DataFrame(standings)
        standings_df["competition_endpoint"] = competition_endpoint
        return standings_df

    def _parse_standings_row(self, row: Tag) -> dict | None:
        """ TODO """
        cols = row.find_all('td')
        if len(cols) < 11:
            if "group" in row.find('th').text.strip().lower():
                logger.debug(f"Skipping group row in table: {row}")
            else:
                logger.debug(f"Unexpected row format in table: {row}")
            return None
        
        return {
            'position': cols[0].text.strip(),
            'team_source_name': cols[2].text.strip(),
            'matches_played': cols[3].text.strip(),
            'wins': cols[4].text.strip(),
            'draws': cols[5].text.strip(),
            'losses': cols[6].text.strip(),
            'goals_for': cols[7].text.strip(),
            'goals_against': cols[8].text.strip(),
            'goal_difference': cols[9].text.strip(),
            'points': cols[10].text.strip()
        }

    # Scrape matches

    def scrape_matches(self, competition_endpoint: str) -> pd.DataFrame:
        """ TODO """
        logger.info(f"Scraping matches for competition at endpoint: {competition_endpoint}")
        url = f"{self.base_url}/competitions{competition_endpoint}"

        counter = 0
        matches = [] 
        while counter < 5:
            logger.debug(f"Scraping URL: {url}")
            soup = self.scrape_url(url)
            if not soup:
                logger.debug(f"Failed to fetch matches for competition at endpoint: {competition_endpoint}")
                break
            logger.debug(f"Scraping matches")
            new_matches = self._scrape_matches(soup)
            if not new_matches:
                logger.debug(f"No more matches found at endpoint: {competition_endpoint}")
                break
            matches.extend(new_matches)
            last_match = matches[-1]
            logger.debug(f"Getting next URL for matches")
            url = self.get_next_url(soup, last_match, url)
            if not url:
                logger.debug(f"No next URL found at endpoint: {competition_endpoint}")
                break
            counter += 1

        matches_df = pd.DataFrame(matches)
        matches_df["competition_endpoint"] = competition_endpoint
        return matches_df

    def _scrape_matches(self, soup: BeautifulSoup) -> list[dict]:
        """ TODO """
        current_date, matches = None, []
        for row in soup.find_all('tr'):

            # Check if row is a match
            if 'matchrow' in row.get('class', []):                
                try:
                    if 'repeatrow' in row.get('class', []):
                        continue  # Skip repeated rows
                    match_data = self._parse_match_row(row, current_date)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    logger.debug(f"Error parsing match row: {e}")
                    logger.debug(traceback.format_exc())
                    continue

            # Check if the row contains a date (not a match row)
            elif row.find('a') and ',' in row.get_text():
                current_date = self._parse_date_row(row)

        return matches

    def _parse_match_row(self, row: Tag, current_date: str) -> dict:
        """ TODO """
        # Match time
        time_cell = row.find('span', class_='timecell')
        time_value = time_cell.find('span', class_='ts')['dv'] if time_cell else None
        time_value_tz = time_cell.get_text(strip=True) if time_cell else None  # SoccerLiveTV weird timezone time

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
        return {
            "title": match_text,
            "match_date": current_date,
            "source_time": time_value,
            "source_tz_time": time_value_tz,
            "home_team_source_name": home_team,
            "away_team_source_name": away_team,
            "channels": channels
        }

    def _parse_date_row(self, row: Tag) -> str:
        """ TODO """
        parsed_date = datetime.strptime(row.get_text(strip=True), "%A, %d %B")
        today = datetime.today()
        final_date = parsed_date.replace(year=today.year)
        lower_bound = today - timedelta(days=60)   # Approx. -2 months
        upper_bound = today + timedelta(days=300)  # Approx. +10 months
        if final_date < lower_bound:
            final_date = final_date.replace(year=today.year + 1)
        elif final_date > upper_bound:
            final_date = final_date.replace(year=today.year - 1)
        return final_date.strftime("%Y-%m-%d")

    def get_next_url(self, soup: BeautifulSoup, last_match: dict, competition_url: str) -> str|None:
        """ TODO """
        match_date = last_match['match_date']
        match_time_ls = last_match['source_tz_time']
        home_team = last_match['home_team_source_name']
        away_team = last_match['away_team_source_name']

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
                    logger.debug(f"Successfully extracted pageid from the page.")
                    pageid = int(onclick_values[2].strip().strip("'"))
                    return pageid
                except ValueError:
                    logger.debug("Failed to extract pageid from the page.")
                    return None

        logger.debug("Failed to extract pageid from the page.")
        return None


if __name__ == "__main__":
    logger.info("Trying Live Soccer Scraper...")
    competition_endpoints = [
        "/france/ligue-1",
        "/international/uefa-champions-league",
        "/england/premier-league"
    ]

    liver_soccer_scraper = LiveSoccerScraper()

    # logger.info("Scraping matches...")
    # for endpoint in competition_endpoints:
    #     matches_df = liver_soccer_scraper.scrape_matches(endpoint)
    #     logger.info(f"Number of matches scraped from {endpoint}: {len(matches_df)}")
    #     logger.info(matches_df[['match_date', 'source_time', 'home_team_source_name', 'away_team_source_name']].head(2))

    # logger.info("Scraping standings...")
    # for endpoint in competition_endpoints:
    #     standings_df = liver_soccer_scraper.scrape_standings(endpoint)
    #     logger.info(f"Number of standings scraped from {endpoint}: {len(standings_df)}")
    #     logger.info(standings_df[['position', 'team_source_name', 'points']].head(4))

    logger.info("Scraping competitions...")
    competitions_df = liver_soccer_scraper.scrape_competitions()
    logger.info(f"Number of competitions scraped: {len(competitions_df)}")
    logger.info(competitions_df.head(4))
