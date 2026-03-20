from services.scrapers.base_scraper import BaseScraper
from services.scrapers.ca_scraper import CAScraper
from services.scrapers.tx_scraper import TXScraper
from services.scrapers.ny_scraper import NYScraper
from services.scrapers.fl_scraper import FLScraper
from services.scrapers.il_scraper import ILScraper
from services.scrapers.nj_scraper import NJScraper
from services.scrapers.va_scraper import VAScraper
from services.scrapers.md_scraper import MDScraper
from services.scrapers.in_scraper import INScraper
from services.scrapers.oh_scraper import OHScraper
from services.scrapers.mo_scraper import MOScraper
from services.scrapers.ct_scraper import CTScraper
from services.scrapers.or_scraper import ORScraper
from services.scrapers.pa_scraper import PAScraper
from services.scrapers.nc_scraper import NCScraper
from services.scrapers.az_scraper import AZScraper
from services.scrapers.co_scraper import COScraper
from services.scrapers.ga_scraper import GAScraper

ALL_SCRAPERS = [
    CAScraper, TXScraper, NYScraper, FLScraper, ILScraper,
    NJScraper, VAScraper, MDScraper, INScraper, OHScraper,
    MOScraper, CTScraper, ORScraper, PAScraper, NCScraper,
    AZScraper, COScraper, GAScraper,
]

# WA scraper not yet implemented
