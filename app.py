import requests
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures

# Función para obtener el número de CVs inscritos en el proceso
def obtener_num_cvs(divs):
    for div in divs:
        p_tag = div.find('p', class_='m-0')
        if p_tag and 'CVs inscritos en el proceso:' in p_tag.text:
            import re
            match = re.search(r'\d+', p_tag.text)
            if match:
                return match.group()
    return ''  # Retorna vacío si no se encuentra el número de CVs

# Función para obtener todos los enlaces de las ofertas de una página
def obtener_enlaces_pagina(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar que la solicitud fue exitosa
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', class_="font-weight-bold text-cyan-700")
        return [link.get('href') for link in links]
    except requests.exceptions.HTTPError as e:
        print(f"Error al acceder a la página {url}: {e}")
        return []

# Función para procesar una oferta de empleo individual
def procesar_oferta(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar que la solicitud fue exitosa
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find('h1').get_text(strip=True)

        divs = soup.find_all('div', class_="d-flex py-2")
        num_cvs = obtener_num_cvs(divs)

        return {'Título': title, 'Enlace': url, 'CVs inscritos': num_cvs}
    except requests.exceptions.HTTPError as e:
        print(f"Error al acceder a la página {url}: {e}")
        return {'Título': 'Oferta no disponible', 'Enlace': url, 'CVs inscritos': ''}

# Función principal para realizar el scraping de Tecnoempleo
def scrape_tecnoempleo(num_paginas):
    # Nombre del archivo CSV
    csv_file = 'tecnoempleo_ofertas.csv'

    # URL base de Tecnoempleo
    base_url = "https://www.tecnoempleo.com/ofertas-trabajo/?pagina="

    # Lista para almacenar los enlaces de todas las ofertas
    all_links = []

    # Iterar sobre las primeras num_paginas páginas y obtener los enlaces
    for number in range(1, num_paginas + 1):
        url = base_url + str(number)
        enlaces_pagina = obtener_enlaces_pagina(url)
        all_links.extend(enlaces_pagina)

    # Procesar cada oferta de empleo utilizando multi-threading
    data_to_write = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(procesar_oferta, url) for url in all_links]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Procesando ofertas"):
            result = future.result()
            data_to_write.append(result)

    # Escribir los datos en el archivo CSV
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ['Título', 'Enlace', 'CVs inscritos']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()  # Escribir la cabecera (nombres de las columnas)
        for data in data_to_write:
            writer.writerow(data)  # Escribir cada fila de datos

# Ejecutar el scraping de las primeras 200 páginas
scrape_tecnoempleo(200)
