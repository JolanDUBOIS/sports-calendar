from bs4 import BeautifulSoup # type: ignore

from .data_scraping import SoccerLiveScraper


def test():
    scraper = SoccerLiveScraper()
    response = scraper.get_url_response(url="https://www.livesoccertv.com/competitions/")
    soup = BeautifulSoup(response.text, 'html.parser')
    competitions_dict = {}

    # Find all sections
    for section in soup.find_all("div", class_="r-section"):
        region = section.find("h2").text.strip()  # Extract region name
        competitions = []

        # Find all competition links
        for comp in section.find_all("a", class_="flag world"):
            name = comp.text.strip()
            url = comp["href"]
            competitions.append({"name": name, "url": url})

        competitions_dict[region] = competitions
    
    return competitions_dict
