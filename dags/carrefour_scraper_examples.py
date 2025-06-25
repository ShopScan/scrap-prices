"""
Ejemplos de uso de la clase CarrefourScraper para diferentes productos
"""

from scrapers.carrefour_scraper import CarrefourScraper, ProductConfigs, ScrapingConfig
import asyncio

# Usar configuraciones predefinidas
async def scrape_product_category(config: ScrapingConfig, category_name: str):
    """Función genérica para scrapear cualquier categoría de productos"""
    print(f"🚀 Iniciando scraping de {category_name}...")
    
    scraper = CarrefourScraper(config)
    result = await scraper.scrape_products()
    
    print(f"✅ Scraping de {category_name} completado:")
    print(f"   - Productos encontrados: {result['total_items']}")
    print(f"   - Páginas procesadas: {result['pages_processed']}")
    print(f"   - Timestamp: {result['timestamp']}")
    
    return result

# Ejemplo de uso con configuraciones predefinidas
async def main():
    """Ejemplo de cómo usar los diferentes scrapers con configuraciones predefinidas"""
    
    # Scrapear dulce de leche
    dulce_config = ProductConfigs.get_dulce_de_leche_config()
    dulce_result = await scrape_product_category(dulce_config, "Dulce de Leche")
    
    # Scrapear carnes (descomenta para usar)
    # carnes_config = ProductConfigs.get_carnes_config()
    # carnes_result = await scrape_product_category(carnes_config, "Carnes")
    
    # Scrapear lácteos (descomenta para usar)
    # lacteos_config = ProductConfigs.get_lacteos_config()
    # lacteos_result = await scrape_product_category(lacteos_config, "Lácteos")
    
    # Scrapear bebidas (descomenta para usar)
    # bebidas_config = ProductConfigs.get_bebidas_config()
    # bebidas_result = await scrape_product_category(bebidas_config, "Bebidas")

# Ejemplo de configuración personalizada
async def example_custom_config():
    """Ejemplo de cómo crear una configuración personalizada"""
    
    # Configuración personalizada para cereales (ejemplo)
    cereales_config = ProductConfigs.get_custom_config(
        base_url="https://www.carrefour.com.ar/Almacen/Cereales-y-legumbres",
        max_empty_pages=3,  # Más tolerancia a páginas vacías
        target_elements=20  # Más elementos por página
    )
    
    result = await scrape_product_category(cereales_config, "Cereales (Personalizado)")
    return result

if __name__ == "__main__":
    # Para pruebas locales
    asyncio.run(main())
