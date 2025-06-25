"""
Paquete de scrapers para Carrefour
"""

from .carrefour_scraper import CarrefourScraper, ScrapingConfig, ProductConfigs, scrape_product_category

__all__ = [
    'CarrefourScraper',
    'ScrapingConfig',
    'ProductConfigs',
    'scrape_product_category'
]
