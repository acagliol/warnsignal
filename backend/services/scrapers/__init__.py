from services.scrapers.base_scraper import BaseScraper
from services.scrapers.ca_scraper import CAScraper
from services.scrapers.tx_scraper import TXScraper
from services.scrapers.ny_scraper import NYScraper
from services.scrapers.fl_scraper import FLScraper
from services.scrapers.il_scraper import ILScraper

ALL_SCRAPERS = [CAScraper, TXScraper, NYScraper, FLScraper, ILScraper]
