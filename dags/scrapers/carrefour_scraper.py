"""
Módulo de scraping modular para Carrefour
Contiene la clase CarrefourScraper y configuraciones relacionadas
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict
from dataclasses import dataclass
from playwright.async_api import async_playwright


@dataclass
class ScrapingConfig:
    """Configuración para el scraping de productos"""
    base_url: str
    product_selectors: List[str]
    no_results_selectors: List[str]
    max_empty_pages: int = 2
    max_scroll_attempts: int = 8
    target_elements: int = 16


class CarrefourScraper:
    """Clase para scraping de productos de Carrefour"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    async def page_has_products(self, page) -> bool:
        """Determinar si una página tiene productos de manera más precisa"""
        total_products = 0
        
        # Verificar productos usando los selectores configurados
        for selector in self.config.product_selectors:
            elements = await page.query_selector_all(selector)
            if len(elements) > 0:
                for element in elements[:5]:
                    try:
                        text = await element.inner_text()
                        if '$' in text and len(text.strip()) > 20:
                            total_products += 1
                    except:
                        continue
        
        # Verificar mensajes de "sin resultados"
        visible_no_results = False
        for selector in self.config.no_results_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    is_visible = await element.is_visible()
                    if is_visible:
                        visible_no_results = True
                        break
            except:
                pass
        
        # Verificar URL sospechosa
        current_url = page.url
        invalid_url_indicators = ['404', 'error', 'not-found', 'pagina-no-encontrada']
        url_indicates_no_page = any(indicator in current_url.lower() for indicator in invalid_url_indicators)
        
        self.logger.info(f"Productos encontrados: {total_products}, Mensaje 'sin resultados' visible: {visible_no_results}, URL sospechosa: {url_indicates_no_page}")
        
        return total_products > 0 and not visible_no_results and not url_indicates_no_page
    
    async def scroll_and_load_elements(self, page, selector: str) -> List:
        """Realizar scroll estratégico para cargar elementos"""
        current_elements = []
        
        for attempt in range(self.config.max_scroll_attempts):
            current_elements = await page.query_selector_all(selector)
            elements_count = len(current_elements)
            
            self.logger.info(f"Intento {attempt + 1}/{self.config.max_scroll_attempts}: {elements_count} elementos encontrados")
            
            if elements_count >= self.config.target_elements:
                self.logger.info(f"¡Objetivo alcanzado! Se encontraron {elements_count} elementos (>= {self.config.target_elements})")
                break
            
            if attempt >= 3 and elements_count == 0:
                self.logger.info(f"Después de {attempt + 1} intentos, no se encontraron elementos. Página podría estar vacía.")
                break
            
            if attempt < self.config.max_scroll_attempts - 1:
                await self._perform_scroll(page, attempt)
        
        return current_elements
    
    async def _perform_scroll(self, page, attempt: int):
        """Realizar scroll estratégico"""
        self.logger.info(f"Haciendo scroll estratégico para cargar más elementos...")
        
        if attempt < 3:
            await page.evaluate("window.scrollBy(0, 1500)")
            await asyncio.sleep(4)
        else:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(5)
            
            await page.evaluate("window.scrollBy(0, -1000)")
            await asyncio.sleep(3)
            await page.evaluate("window.scrollBy(0, 2000)")
            await asyncio.sleep(4)
        
        await page.evaluate("""
            window.dispatchEvent(new Event('scroll'));
            window.dispatchEvent(new Event('resize'));
        """)
        await asyncio.sleep(2)
    
    async def scrape_products(self) -> Dict:
        """Función principal de scraping"""
        items = []
        
        async with async_playwright() as pw:
            chrome = await pw.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            page_browser = await chrome.new_page()
            
            await page_browser.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            page_browser.set_default_timeout(60000)
            
            page_num = 1
            consecutive_empty_pages = 0
            
            self.logger.info("🚀 Iniciando recolección automática de TODAS las páginas disponibles...")
            self.logger.info(f"📋 Criterio de parada: {self.config.max_empty_pages} páginas vacías consecutivas")
            
            try:
                while True:
                    url = f"{self.config.base_url}&page={page_num}" if "?" in self.config.base_url else f"{self.config.base_url}?page={page_num}"
                    self.logger.info(f"Procesando página {page_num}: {url}")
                    
                    try:
                        # Cargar página
                        response = await self._load_page(page_browser, url)
                        
                        if response and response.status != 200:
                            self.logger.error(f"Error HTTP {response.status} en página {page_num}")
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= self.config.max_empty_pages:
                                self.logger.info(f"Demasiados errores consecutivos. Finalizando...")
                                break
                            page_num += 1
                            continue

                        has_products = await self.page_has_products(page_browser)
                        
                        if not has_products:
                            self.logger.info(f"Página {page_num}: No hay productos disponibles")
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= self.config.max_empty_pages:
                                self.logger.info(f"Se encontraron {consecutive_empty_pages} páginas sin productos consecutivas. Finalizando...")
                                break
                            page_num += 1
                            continue
                        
                        # Procesar productos en la página
                        page_items = await self._process_page_products(page_browser)
                        
                        if not page_items:
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= self.config.max_empty_pages:
                                self.logger.info(f"Se encontraron {consecutive_empty_pages} páginas sin productos válidos consecutivas. Finalizando...")
                                break
                        else:
                            consecutive_empty_pages = 0
                            items.extend(page_items)
                        
                        self.logger.info(f"Total de items recolectados hasta ahora: {len(items)}")
                        
                        await asyncio.sleep(3)
                        page_num += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error al procesar página {page_num}: {e}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= self.config.max_empty_pages:
                            self.logger.info(f"Demasiados errores consecutivos. Finalizando...")
                            break
                        page_num += 1
                        continue
            
            except Exception as e:
                self.logger.error(f"Error general durante la ejecución: {e}")
                raise
            
            finally:
                self.logger.info(f"PROCESO COMPLETADO")
                self.logger.info(f"Total de páginas procesadas: {page_num - 1}")
                self.logger.info(f"Total de items recolectados: {len(items)}")
                
                await chrome.close()
        
        return {
            'items': items,
            'pages_processed': page_num - 1,
            'total_items': len(items),
            'timestamp': datetime.now().isoformat()
        }
    
    async def _load_page(self, page, url):
        """Cargar una página con diferentes estrategias"""
        try:
            response = await page.goto(url, wait_until='load', timeout=45000)
            self.logger.info(f"Página cargada con 'load'")
            await asyncio.sleep(5)
            return response
        except:
            try:
                response = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
                self.logger.info(f"Página cargada con 'domcontentloaded'")
                await asyncio.sleep(5)
                return response
            except:
                response = await page.goto(url, timeout=20000)
                self.logger.info(f"Página cargada sin wait_until")
                await asyncio.sleep(5)
                return response
    
    async def _process_page_products(self, page) -> List[str]:
        """Procesar productos de una página específica"""
        self.logger.info(f"Productos detectados inicialmente. Procediendo con scroll...")
        
        await asyncio.sleep(3)
        
        # Usar el primer selector de productos para hacer scroll
        primary_selector = self.config.product_selectors[0]
        current_elements = await self.scroll_and_load_elements(page, primary_selector)
        
        page_items = []
        if current_elements:
            self.logger.info(f"Procesando {len(current_elements)} elementos!")
            
            valid_products_found = 0
            for i, element in enumerate(current_elements, 1):
                try:
                    element_text = await element.inner_text()
                    
                    if '$' in element_text and len(element_text.strip()) > 10:
                        page_items.append(element_text)
                        valid_products_found += 1
                    else:
                        self.logger.debug(f"Elemento #{i} no parece ser un producto válido")
                
                except Exception as e:
                    self.logger.error(f"Error al procesar elemento #{i}: {e}")
            
            self.logger.info(f"Productos válidos encontrados en esta página: {valid_products_found}")
        
        return page_items


# Configuraciones predefinidas para diferentes productos
class ProductConfigs:
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
            max_scroll_attempts=8,
            target_elements=16
        )
    
    @classmethod
    def get_carnes_config(cls) -> ScrapingConfig:
        """Configuración para carnes"""
        return ScrapingConfig(
            base_url="https://www.carrefour.com.ar/Carnes",
            product_selectors=cls.COMMON_PRODUCT_SELECTORS,
            no_results_selectors=cls.COMMON_NO_RESULTS_SELECTORS,
            max_empty_pages=3,
            max_scroll_attempts=10,
            target_elements=20
        )
    
    @classmethod
    def get_lacteos_config(cls) -> ScrapingConfig:
        """Configuración para lácteos"""
        return ScrapingConfig(
            base_url="https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Lacteos",
            product_selectors=cls.COMMON_PRODUCT_SELECTORS,
            no_results_selectors=cls.COMMON_NO_RESULTS_SELECTORS,
            max_empty_pages=2,
            max_scroll_attempts=8,
            target_elements=16
        )
    
    @classmethod
    def get_bebidas_config(cls) -> ScrapingConfig:
        """Configuración para bebidas"""
        return ScrapingConfig(
            base_url="https://www.carrefour.com.ar/Bebidas",
            product_selectors=cls.COMMON_PRODUCT_SELECTORS,
            no_results_selectors=cls.COMMON_NO_RESULTS_SELECTORS,
            max_empty_pages=3,
            max_scroll_attempts=8,
            target_elements=16
        )
    
    @classmethod
    def get_custom_config(cls, base_url: str, **kwargs) -> ScrapingConfig:
        """Crear configuración personalizada"""
        return ScrapingConfig(
            base_url=base_url,
            product_selectors=kwargs.get('product_selectors', cls.COMMON_PRODUCT_SELECTORS),
            no_results_selectors=kwargs.get('no_results_selectors', cls.COMMON_NO_RESULTS_SELECTORS),
            max_empty_pages=kwargs.get('max_empty_pages', 2),
            max_scroll_attempts=kwargs.get('max_scroll_attempts', 8),
            target_elements=kwargs.get('target_elements', 16)
        )


# Función de utilidad para scraping rápido
async def scrape_product_category(config: ScrapingConfig) -> Dict:
    """Función de utilidad para scrapear cualquier categoría de productos"""
    scraper = CarrefourScraper(config)
    return await scraper.scrape_products()
