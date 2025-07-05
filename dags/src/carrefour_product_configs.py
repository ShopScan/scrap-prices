"""
Configuraciones predefinidas para diferentes productos de Carrefour
"""

from typing import List
from .config import ScrapingConfig


class CarrefourProductConfigs:
    """Configuraciones predefinidas para diferentes productos de Carrefour"""
    
    # Selectores comunes para la mayoría de productos
    COMMON_PRODUCT_SELECTORS = [
        '.valtech-carrefourar-search-result-3-x-galleryItem',
        'div[class*="galleryItem"]',
        'article'
    ]
    
    COMMON_NO_RESULTS_SELECTORS = [
        'text="Sin resultados"',
        'text="No se encontraron productos"',
        'text="No hay productos"',
        'text="Página no encontrada"',
        'text="404"',
        '[class*="titleNotFound"]',
        '[class*="not-found"]',
        '[class*="no-results"]',
        '[class*="empty"]'
    ]
    
    @classmethod
    def get_dulce_de_leche_config(cls) -> ScrapingConfig:
        """Configuración para dulce de leche"""
        return ScrapingConfig(
            base_url="https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Dulce-de-leche",
            product_selectors=cls.COMMON_PRODUCT_SELECTORS,
            no_results_selectors=cls.COMMON_NO_RESULTS_SELECTORS,
            max_empty_pages=2,
            max_scroll_attempts=3,
            target_elements=16
        )