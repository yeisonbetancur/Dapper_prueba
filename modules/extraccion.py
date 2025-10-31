import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests



#constantes para el scraping
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
URL_BASE = "https://www.ani.gov.co/informacion-de-la-ani/normatividad?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="

# Clasificaciones de documentos
CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}

DEFAULT_RTYPE_ID = 14

def clean_quotes(text):
    if not text:
        return text
    quotes_map = {
        '\u201C': '', '\u2018': '', '\u2019': '', '\u00AB': '', '\u00BB': '',
        '\u201E': '', '\u201A': '', '\u2039': '', '\u203A': '', '"': '',
        "'": '', '´': '', '`': '', '′': '', '″': '',
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned_text = re.sub(quotes_pattern, '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

# Obtener el rtype_id basado en el título del documento
def get_rtype_id(title):
    """
    Obtiene el rtype_id basado en el título del documento.
    """
    title_lower = title.lower()
    
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    
    return DEFAULT_RTYPE_ID

# Validar el campo created_at
def is_valid_created_at(created_at_value):
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False

def normalize_datetime(dt):
    """
    Normaliza un datetime para quitar información de timezone.
    """
    if dt is None:
        return None
    
    # Si es un datetime con timezone, convertir a naive
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    
    return dt

def extract_title_and_link(row, norma_data, verbose, row_num):
    """
    Extrae título y enlace de una fila
    
    Returns:
        bool: True si se extrajo correctamente, False si debe saltarse
    """
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        if verbose:
            print(f"No se encontró celda de título en la fila {row_num}. Saltando.")
        return False
    
    title_link = title_cell.find('a')
    if not title_link:
        if verbose:
            print(f"No se encontró enlace en la fila {row_num}. Saltando.")
        return False
    
    # Procesar título
    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)
    
    # Validar longitud del título
    if len(cleaned_title) > 65:
        if verbose:
            print(f"Saltando norma con título demasiado largo: '{cleaned_title}' (longitud: {len(cleaned_title)})")
        return False
    
    norma_data['title'] = cleaned_title
    
    # Procesar enlace
    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link
    
    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None
    
    # Validar que tenga enlace
    if not norma_data['external_link']:
        if verbose:
            print(f"Saltando norma '{norma_data['title']}' por no tener enlace externo válido.")
        return False
    
    return True

def extract_summary(row, norma_data):
    """
    Extrae el resumen/descripción de una fila
    """
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize()
        norma_data['summary'] = formatted_summary
    else:
        norma_data['summary'] = None

def extract_creation_date(row, norma_data, verbose, row_num):
    """
    Extrae la fecha de creación de una fila
    
    Returns:
        bool: True si se extrajo correctamente, False si debe saltarse
    """
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            # Procesar diferentes formatos de fecha
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None
    
    # Validar fecha
    if not is_valid_created_at(norma_data['created_at']):
        if verbose:
            print(f"Saltando norma '{norma_data['title']}' por no tener fecha de creación válida (created_at: {norma_data['created_at']}).")
        return False
    
    return True

def scrape_page(page_num, verbose=False):
    """
    Scrapea una página específica de ANI
    
    Args:
        page_num (int): Número de página a scrapear
        verbose (bool): Si mostrar logs detallados
    
    Returns:
        list: Lista de diccionarios con los datos extraídos
    """
    # Construir URL de la página
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"
    
    if verbose:
        print(f"Scrapeando página {page_num}: {page_url}")
    
    try:
        # Realizar solicitud HTTP
        response = requests.get(page_url, timeout=15)
        response.raise_for_status()
        
        # Parsear HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        tbody = soup.find('tbody')
        
        if not tbody:
            if verbose:
                print(f"No se encontró tabla en página {page_num}")
            return []
        
        rows = tbody.find_all('tr')
        if verbose:
            print(f"Encontradas {len(rows)} filas en página {page_num}")
        
        # Procesar filas
        page_data = []
        for i, row in enumerate(rows, 1):
            try:
                # Estructura base del registro
                norma_data = {
                    'created_at': None,
                    'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': True,
                    'title': None,
                    'gtype': None,
                    'entity': ENTITY_VALUE,
                    'external_link': None,
                    'rtype_id': None,
                    'summary': None,
                    'classification_id': FIXED_CLASSIFICATION_ID,
                }
                
                # Extraer datos
                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue
                
                extract_summary(row, norma_data)
                
                if not extract_creation_date(row, norma_data, verbose, i):
                    continue
                
                # Establecer rtype_id basado en título
                norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
                
                page_data.append(norma_data)
                
            except Exception as e:
                if verbose:
                    print(f"Error procesando fila {i} en página {page_num}: {str(e)}")
                continue
        
        return page_data
        
    except requests.RequestException as e:
        print(f"Error HTTP en página {page_num}: {e}")
        return []
    except Exception as e:
        print(f"Error procesando página {page_num}: {e}")
        return []