# factory.py

from scrapers.greenhouse_scraper import GreenhouseScraper
from scrapers.lever_scraper import LeverScraper
from scrapers.ashby_scraper import AshbyScraper
from scrapers.jobvite_scraper import JobviteScraper


class ScraperFactory:

    @staticmethod
    def get_scraper(scraper_type, board_token, *args, **kwargs):
        if scraper_type == 'greenhouse':
            return GreenhouseScraper(board_token, *args, **kwargs)
        elif scraper_type == 'lever':
            return LeverScraper(board_token, *args, **kwargs)
        elif scraper_type == 'lever_eu':
            return LeverScraper(board_token, is_eu=True, *args, **kwargs)
        elif scraper_type == 'ashby':
            return AshbyScraper(board_token, *args, **kwargs)
        elif scraper_type == 'jobvite':
            return JobviteScraper(board_token, *args, **kwargs)
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")
