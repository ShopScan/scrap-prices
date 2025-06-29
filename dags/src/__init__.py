"""
Paquete de scrapers para Carrefour y Vea
"""

from .config import ScrapingConfig
from .scraper import WebScraper, scrape_product_category
from .carrefour_product_configs import CarrefourProductConfigs
from .vea_product_configs import VeaProductConfigs

__all__ = [
    'WebScraper',
    'ScrapingConfig',
    'CarrefourProductConfigs',
    'VeaProductConfigs',
    'scrape_product_category'
]
