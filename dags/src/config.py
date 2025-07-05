"""
Configuraciones base para el scraping
"""

from dataclasses import dataclass
from typing import List


@dataclass
class ScrapingConfig:
    """Configuración para el scraping de productos"""
    base_url: str
    product_selectors: List[str]
    no_results_selectors: List[str]
    max_empty_pages: int = 2
    max_scroll_attempts: int = 8
    target_elements: int = 16
