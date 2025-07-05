"""
Configuraciones predefinidas para diferentes productos de Vea
"""

from typing import List
from .config import ScrapingConfig


class VeaProductConfigs:
    """Configuraciones predefinidas para diferentes productos de Vea"""
    
    # Selectores comunes para la mayoría de productos
    COMMON_PRODUCT_SELECTORS = [
        '.veaargentina-cmedia-integration-cencosud-1-x-galleryItem',
        '.vtex-product-summary-2-x-container',
        'article.vtex-product-summary-2-x-element',
        'section[aria-label*="Producto"]'
    ]
    
    COMMON_NO_RESULTS_SELECTORS = [
        'text="Sin resultados para tu búsqueda"',
        'text="No se encontraron productos"',
        'text="No hay productos disponibles"',
        'text="Página no encontrada"',
        'text="Error 404"',
        '.vtex-search-result-3-x-notFound',
        '.vtex-search-result-3-x-notFoundOops',
        '.search-not-found-oops',
        '.search-not-found-term',
        '.gallery--empty',
        '.search-result--empty'
    ]
    
    @classmethod
    def get_dulce_de_leche_config(cls) -> ScrapingConfig:
        """Configuración para dulce de leche"""
        return ScrapingConfig(
            base_url="https://www.vea.com.ar/lacteos/dulce-de-leche",
            product_selectors=cls.COMMON_PRODUCT_SELECTORS,
            no_results_selectors=cls.COMMON_NO_RESULTS_SELECTORS,
            max_empty_pages=0,
            max_scroll_attempts=2,
            target_elements=20
        )
