import asyncio
from playwright.async_api import async_playwright
import json

meat_items = []

def save_to_json(data, filename='meat_prices.json'):
    """
    Save the meat items data to a JSON file.
    
    Args:
        data (list): The list of meat items to save.
        filename (str): The name of the JSON file to save the data to.
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Datos guardados en {filename}")


async def page_has_products(page):
    """
    Determinar si una página tiene productos de manera más precisa
    """
    # Buscar elementos de productos
    product_selectors = [
        '.valtech-carrefourar-search-result-3-x-galleryItem',
        'div[class*="galleryItem"]',
        'article'
    ]
    
    total_products = 0
    for selector in product_selectors:
        elements = await page.query_selector_all(selector)
        if len(elements) > 0:
            # Verificar que realmente sean productos (que tengan precio)
            for element in elements[:5]:  # Verificar más elementos para estar seguros
                try:
                    text = await element.inner_text()
                    if '$' in text and len(text.strip()) > 20:  # Verificar que tenga contenido sustancial
                        total_products += 1
                except:
                    continue
    
    # También verificar si hay mensajes visibles de "sin resultados" o "página no encontrada"
    no_results_selectors = [
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
    
    visible_no_results = False
    for selector in no_results_selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                is_visible = await element.is_visible()
                if is_visible:
                    visible_no_results = True
                    break
        except:
            pass
    
    # Verificar si la URL indica que no hay más páginas (algunos sitios redirigen)
    current_url = page.url
    invalid_url_indicators = ['404', 'error', 'not-found', 'pagina-no-encontrada']
    url_indicates_no_page = any(indicator in current_url.lower() for indicator in invalid_url_indicators)
    
    print(f"Productos encontrados: {total_products}, Mensaje 'sin resultados' visible: {visible_no_results}, URL sospechosa: {url_indicates_no_page}")
    
    return total_products > 0 and not visible_no_results and not url_indicates_no_page

async def main():
    async with async_playwright() as pw:
        # Configuración más robusta del navegador
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
        
        # Configurar user agent y headers para parecer un navegador real
        await page_browser.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Configurar timeouts más largos
        page_browser.set_default_timeout(60000)  # 60 segundos
        
        # Variables para el recorrido dinámico
        page_num = 1
        consecutive_empty_pages = 0
        max_empty_pages = 2  # Si encuentra 2 páginas vacías consecutivas, se detiene
        
        print(f"🚀 Iniciando recolección automática de TODAS las páginas disponibles...")
        print(f"📋 Criterio de parada: {max_empty_pages} páginas vacías consecutivas")
        print(f"{'='*80}")
        
        try:
            while True:
                url = f"https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Dulce-de-leche?page={page_num}"
                print(f"URL: {url}")
                
                try:
                    # Intentar cargar la página
                    print(f"Intentando cargar página {page_num}...")
                    
                    try:
                        response = await page_browser.goto(url, wait_until='load', timeout=45000)
                        print(f"Página cargada con 'load'")
                    except:
                        try:
                            response = await page_browser.goto(url, wait_until='domcontentloaded', timeout=30000)
                            print(f"Página cargada con 'domcontentloaded'")
                        except:
                            response = await page_browser.goto(url, timeout=20000)
                            print(f"Página cargada sin wait_until")

                    # Esperar un poco más para asegurar que cargue
                    await asyncio.sleep(5)
                    
                    # Verificar si la página cargó correctamente
                    if response and response.status != 200:
                        print(f"Error HTTP {response.status} en página {page_num}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            print(f"Demasiados errores consecutivos. Finalizando...")
                            break
                        page_num += 1
                        continue
                    # Verificar si la página tiene productos usando nuestra función mejorada
                    has_products = await page_has_products(page_browser)
                    
                    if not has_products:
                        print(f"Página {page_num}: No hay productos disponibles")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            print(f"Se encontraron {consecutive_empty_pages} páginas sin productos consecutivas. Finalizando...")
                            break
                        page_num += 1
                        continue
                    
                    # Si llegamos aquí, la página tiene productos, pero vamos a verificar nuevamente después del scroll
                    print(f"Página {page_num}: Productos detectados inicialmente. Procediendo con scroll...")
                    
                    
                    max_attempts = 8
                    target_elements = 16
                    
                    # Esperar inicial para que la página cargue completamente
                    await asyncio.sleep(3)
                    
                    current_elements = []
                    for attempt in range(max_attempts):
                        selector = '.valtech-carrefourar-search-result-3-x-galleryItem'
                        current_elements = await page_browser.query_selector_all(selector)
                        elements_count = len(current_elements)
                        
                        print(f"Intento {attempt + 1}/{max_attempts}: {elements_count} elementos encontrados con {selector}")
                        
                        if elements_count >= target_elements:
                            print(f"¡Objetivo alcanzado! Se encontraron {elements_count} elementos (>= {target_elements})")
                            break
                        
                        # Si después de varios intentos no encontramos elementos, considerar página vacía
                        if attempt >= 3 and elements_count == 0:
                            print(f"Después de {attempt + 1} intentos, no se encontraron elementos. Página podría estar vacía.")
                            break
                        
                        if attempt < max_attempts - 1:  # No hacer scroll en el último intento
                            print(f"Haciendo scroll estratégico para cargar más elementos...")
                            if attempt < 3:
                                # Primeros intentos: scroll gradual
                                await page_browser.evaluate("window.scrollBy(0, 1500)")
                                await asyncio.sleep(4)
                            else:
                                # Intentos posteriores: scroll más agresivo
                                await page_browser.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await asyncio.sleep(5)
                                
                                # Scroll hacia arriba y abajo para activar lazy loading
                                await page_browser.evaluate("window.scrollBy(0, -1000)")
                                await asyncio.sleep(3)
                                await page_browser.evaluate("window.scrollBy(0, 2000)")
                                await asyncio.sleep(4)
                            
                            # Disparar eventos de scroll para asegurar lazy loading
                            await page_browser.evaluate("""
                                window.dispatchEvent(new Event('scroll'));
                                window.dispatchEvent(new Event('resize'));
                            """)
                            await asyncio.sleep(2)
                        else:
                            print(f"Máximo de intentos alcanzado. Elementos finales: {elements_count}")

                    if current_elements:
                        print(f"Procesando {len(current_elements)} elementos en página {page_num}!")
                        
                        valid_products_found = 0
                        for i, element in enumerate(current_elements, 1):
                            print(f"Página {page_num} - Elemento #{i}")
                            
                            try:
                                element_text = await element.inner_text()
                                
                                # Verificar que el elemento contenga información de producto válida
                                if '$' in element_text and len(element_text.strip()) > 10:
                                    element_with_source = {
                                        "text": element_text,
                                        "source": 'Carrefour',
                                        "url": url,
                                        "page": page_num
                                    }
                                    meat_items.append(element_with_source)
                                    valid_products_found += 1
                                else:
                                    print(f"Elemento #{i} no parece ser un producto válido")
                            
                            except Exception as e:
                                print(f"Error al procesar elemento #{i}: {e}")
                        
                        # Si no se encontraron productos válidos en esta página
                        if valid_products_found == 0:
                            print(f"No se encontraron productos válidos en página {page_num}")
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= max_empty_pages:
                                print(f"Se encontraron {consecutive_empty_pages} páginas sin productos válidos consecutivas. Finalizando...")
                                break
                        else:
                            consecutive_empty_pages = 0  # Reiniciar contador si encontramos productos válidos
                    else:
                        print(f"No se encontraron elementos en página {page_num}")
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= max_empty_pages:
                            print(f"Se encontraron {consecutive_empty_pages} páginas sin elementos consecutivas. Finalizando...")
                            break
                    
                    print(f"Total de items recolectados hasta ahora: {len(meat_items)}")
                    
                    # Pequeña pausa entre páginas para no sobrecargar el servidor
                    print("Esperando antes de la siguiente página...")
                    await asyncio.sleep(3)
                    
                    # Incrementar página para el siguiente ciclo
                    page_num += 1
                    
                except Exception as e:
                    print(f"Error al procesar página {page_num}: {e}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_empty_pages:
                        print(f"Demasiados errores consecutivos. Finalizando...")
                        break
                    page_num += 1
                    continue
        
        except Exception as e:
            print(f"Error general durante la ejecución: {e}")
        
        finally:
            print(f"PROCESO COMPLETADO")
            print(f"Total de páginas procesadas: {page_num - 1}")
            print(f"Total de items recolectados: {len(meat_items)}")
            
            if meat_items:
                filename = f'meat_prices_carrefour_fixed_{len(meat_items)}_items.json'
                save_to_json(meat_items, filename)
                print(f"Datos guardados exitosamente en {filename}")
            else:
                # Guardar un JSON vacío pero con metadata
                empty_data = {
                    "metadata": {
                        "total_items": 0,
                        "pages_processed": page_num - 1,
                        "source": "Carrefour",
                        "url": "https://www.carrefour.com.ar/Lacteos-y-productos",
                        "timestamp": "2025-06-17",
                        "reason": "No se encontraron productos o se detuvieron por páginas vacías consecutivas"
                    },
                    "items": []
                }
                filename = 'meat_prices_carrefour_empty.json'
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(empty_data, f, ensure_ascii=False, indent=4)
                print(f"No se recolectaron items, pero se guardó archivo con metadata: {filename}")
            
            await chrome.close()

if __name__ == "__main__":
    asyncio.run(main())
