import requests
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Función para obtener el número de CVs inscritos en el proceso
def obtener_num_cvs(divs):
    for div in divs:
        p_tag = div.find('p', class_='m-0')
        if p_tag and 'CVs inscritos en el proceso:' in p_tag.text:
            match = re.search(r'\d+', p_tag.text)
            if match:
                return match.group()
    return ''  # Retorna vacío si no se encuentra el número de CVs

# Función para obtener los enlaces de las ofertas de una página
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

# Función para obtener la información específica de cada oferta de empleo
def obtener_informacion_especifica(soup):
    section = soup.select_one('#wrapper > section:nth-of-type(2) > div:nth-of-type(1) > div > div:nth-of-type(2)')

    specific_data = {
        'Ubicación': '',
        'Funciones': '',
        'Jornada': '',
        'Experiencia': '',
        'Tipo contrato': '',
        'Salario': '',
        'Descripción': ''
    }

    if section:
        list_items = section.find_all('li', class_='list-item clearfix border-bottom py-2')

        for item in list_items:
            span = item.find('span', class_='float-end')
            if span:
                data = span.text.strip()
                if 'Ubicación' in item.text:
                    specific_data['Ubicación'] = data
                elif 'Funciones' in item.text:
                    specific_data['Funciones'] = data
                elif 'Jornada' in item.text:
                    specific_data['Jornada'] = data
                elif 'Experiencia' in item.text:
                    specific_data['Experiencia'] = data
                elif 'Tipo contrato' in item.text:
                    specific_data['Tipo contrato'] = data
                elif 'Salario' in item.text:
                    specific_data['Salario'] = data

    # Obtener la descripción
    descripcion = ' '.join([p.text.strip() for p in soup.find_all('p', class_='fs--16 text-gray-800')])
    specific_data['Descripción'] = descripcion

    return specific_data

# Función para convertir la experiencia a un valor numérico
def convertir_experiencia(experiencia):
    if experiencia is None:
        return None
    experiencia = experiencia.lower()
    if 'más de 10 años' in experiencia:
        return 10
    elif 'más de 5 años' in experiencia:
        return 5
    elif '3-5 años' in experiencia:
        return 4  # Tomamos el punto medio del rango
    elif 'menos de un año' in experiencia:
        return 0
    elif 'sin experiencia' in experiencia:
        return 0
    else:
        match = re.search(r'\d+', experiencia)
        if match:
            return int(match.group())
        return None

# Función para procesar una oferta de empleo individual
def procesar_oferta(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verificar que la solicitud fue exitosa
        soup = BeautifulSoup(response.text, 'html.parser')

        title = soup.find('h1').get_text(strip=True)
        specific_info = obtener_informacion_especifica(soup)
        divs = soup.find_all('div', class_="d-flex py-2")
        num_cvs = obtener_num_cvs(divs)

        salario_min = ''
        salario_max = ''
        if specific_info and specific_info.get('Salario'):
            salario = specific_info['Salario']
            match = re.findall(r'[\d\.]+', salario.replace('.', '').replace('€', '').replace(',', ''))
            if len(match) == 2:
                salario_min = int(match[0])
                salario_max = int(match[1])

        experiencia = convertir_experiencia(specific_info.get('Experiencia', '')) if specific_info else None

        return {
            'Título': title, 
            'Enlace': url, 
            'CVs inscritos': num_cvs,
            'Ubicación': specific_info.get('Ubicación', '') if specific_info else '',
            'Funciones': specific_info.get('Funciones', '') if specific_info else '',
            'Jornada': specific_info.get('Jornada', '') if specific_info else '',
            'Experiencia': experiencia,
            'Tipo contrato': specific_info.get('Tipo contrato', '') if specific_info else '',
            'Salario Mínimo': salario_min,
            'Salario Máximo': salario_max,
            'Descripción': specific_info.get('Descripción', '') if specific_info else ''
        }
    
    except requests.exceptions.HTTPError as e:
        print(f"Error al acceder a la página {url}: {e}")
        return {
            'Título': 'Oferta no disponible', 
            'Enlace': url, 
            'CVs inscritos': '', 
            'Ubicación': '', 
            'Funciones': '', 
            'Jornada': '', 
            'Experiencia': '', 
            'Tipo contrato': '', 
            'Salario Mínimo': '', 
            'Salario Máximo': '',
            'Descripción': ''
        }

# Función principal para realizar el scraping de Tecnoempleo
def scrape_tecnoempleo(num_paginas):
    csv_file = 'tecnoempleo_ofertas.csv'
    base_url = "https://www.tecnoempleo.com/ofertas-trabajo/?pagina="
    all_links = []

    for number in range(1, num_paginas + 1):
        url = base_url + str(number)
        enlaces_pagina = obtener_enlaces_pagina(url)
        all_links.extend(enlaces_pagina)

    data_to_write = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(procesar_oferta, url) for url in all_links]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Procesando ofertas"):
            result = future.result()
            data_to_write.append(result)

    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = [
            'Título', 'Enlace', 'CVs inscritos', 
            'Ubicación', 'Funciones', 'Jornada', 
            'Experiencia', 'Tipo contrato', 
            'Salario Mínimo', 'Salario Máximo',
            'Descripción'
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for data in data_to_write:
            writer.writerow(data)

# Ejecutar el scraping de las primeras 200 páginas
scrape_tecnoempleo(2)
