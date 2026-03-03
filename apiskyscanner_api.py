# -*- coding: utf-8 -*-
"""
Skyscanner v3 + Google Sheets - VERSIÓN DOCKER LOOP + CACHE
============================================================
- Lee TODA la configuración desde .env
- Escritura incremental (tiempo real)
- Polling inteligente basado en status
- EntityID para máxima precisión
- LOOP CONTINUO con intervalo configurable (ya no depende de Task Scheduler)
- Retry inteligente en errores 429 de Google Sheets
- Soporte PUEBLA B: solo extras con límite de precio y checkbox
- SheetManager: Caché de conexiones a Spreadsheets/Worksheets (reduce GetSpreadsheet 99%)
"""

import os, requests, gspread, time, json, hashlib, logging, re, sys
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime, timedelta
from functools import wraps
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from logging.handlers import RotatingFileHandler

# ============================================================================
# CARGAR .ENV
# ============================================================================
load_dotenv()

def get_env(key: str, default: str = None) -> str:
    return os.getenv(key, default)

def get_env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key, str(default)).lower()
    return val in ('true', '1', 'yes', 'on')

def get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default

def get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default

def get_env_list(key: str, default: str = "") -> List[str]:
    val = os.getenv(key, default)
    if not val:
        return []
    return [x.strip() for x in val.split(',') if x.strip()]

# ============================================================================
# CONFIGURACIÓN DESDE .ENV
# ============================================================================

# Credenciales
API_KEY = get_env('SKYSCANNER_API_KEY')
KEYFILE = get_env('GOOGLE_KEYFILE', '/app/credentials/service-account.json')

# Google Sheets URLs
SHEET_ORIGEN_URL = get_env('SHEET_ORIGEN_URL')
SHEET_DESTINO_URL = get_env('SHEET_DESTINO_URL')

# Archivos
ENTITY_CACHE_FILE = get_env('SS_ENTITY_CACHE', '/app/data/entity_cache.json')
LOCKFILE_PATH = get_env('SS_LOCKFILE', '/app/data/.script.lock')
LOG_FILE = get_env('SS_LOG_FILE', '/app/logs/skyscanner_api.log')

# Rate limiting
MAX_CALLS_PER_MIN = get_env_int('SS_MAX_CALLS_PER_MIN', 80)
RATE_LIMIT_WINDOW = get_env_int('SS_RATE_LIMIT_WINDOW', 60)

# Polling - CRÍTICO PARA PRECISIÓN
POLL_CONFIG = {
    "MIN_GUARANTEED_POLLS": get_env_int('SS_MIN_GUARANTEED_POLLS', 6),
    "MAX_POLL_ROUNDS": get_env_int('SS_MAX_POLL_ROUNDS', 15),
    "POLL_SLEEP_SECONDS": get_env_float('SS_POLL_SLEEP_SECONDS', 2.0),
    "POLL_DEADLINE_SECONDS": get_env_int('SS_POLL_DEADLINE_SECONDS', 55),
    "WAIT_FOR_COMPLETE": get_env_bool('SS_WAIT_FOR_COMPLETE', True),
}

# Configuración de vuelos
ADULTOS = get_env_int('ADULTOS', 1)
CHILDREN = get_env_int('CHILDREN', 0)
CABIN = get_env('CABIN', 'CABIN_CLASS_ECONOMY')
MARKET = get_env('MARKET', 'MX')
LOCALE = get_env('LOCALE', 'es-MX')
CURRENCY = get_env('CURRENCY', 'MXN')

# Técnico
USE_ENTITY_ID = get_env_bool('USE_ENTITY_ID', True)
WRITE_IMMEDIATELY = get_env_bool('WRITE_IMMEDIATELY', True)
FORCE_BEST_TO_CHEAPEST = get_env_bool('FORCE_BEST_TO_CHEAPEST', True)

# Hojas
ORIGENES_FILAS = [int(x) for x in get_env_list('ORIGENES_FILAS', '39,41,43,45,47,49,51,53,55,57')]
EXTRAS_COL = get_env('EXTRAS_COL', 'D')

# Multi-hoja
PRIORIDAD_PROCESO = get_env_list('PRIORIDAD_PROCESO', 'V1,V2,V3')
SOLO_UNA_ACTIVA = get_env_bool('SOLO_UNA_ACTIVA', True)
POLITICA_MULTIPLE = get_env('POLITICA_MULTIPLE', 'FIRST')

# Log level
LOG_LEVEL = get_env('SS_LOG_LEVEL', 'INFO')

# === LOOP CONTINUO ===
LOOP_ENABLED = get_env_bool('LOOP_ENABLED', True)
LOOP_INTERVAL_SECONDS = get_env_int('LOOP_INTERVAL_SECONDS', 300)       # 5 min entre ciclos
SHEETS_CHECK_DELAY = get_env_float('SHEETS_CHECK_DELAY', 3.0)           # 3 seg entre cada check de switch
PAUSE_BETWEEN_SHEETS = get_env_int('PAUSE_BETWEEN_SHEETS', 120)         # 2 min pausa entre hojas procesadas

# ============================================================================
# CONFIGURACIÓN DE HOJAS DESDE .ENV
# ============================================================================

@dataclass
class SheetConfig:
    captura_sheet: str
    resultado_sheet: str
    switch_cell: str
    off_switch_cell: str
    stats_cell: str
    origen_cell: str
    destino_cell: str
    origen_url: str = ""
    solo_extras: bool = False
    extras_filas: List[int] = field(default_factory=list)
    extras_col: str = "D"
    extras_limit_col: str = ""
    extras_check_col: str = ""


def load_sheet_configs() -> Dict[str, SheetConfig]:
    configs = {}
    for ver in PRIORIDAD_PROCESO:
        prefix = f"{ver}_"
        extras_filas_str = get_env(f'{prefix}EXTRAS_FILAS', '')
        extras_filas = [int(x) for x in extras_filas_str.split(',') if x.strip()] if extras_filas_str else []
        
        configs[ver] = SheetConfig(
            captura_sheet=get_env(f'{prefix}CAPTURA_SHEET', f'REDONDO {ver}'),
            resultado_sheet=get_env(f'{prefix}RESULTADO_SHEET', f'Resultados' if ver == 'V1' else f'Resultados {ver[-1]}'),
            switch_cell=get_env(f'{prefix}SWITCH_CELL', 'F66'),
            off_switch_cell=get_env(f'{prefix}OFF_SWITCH_CELL', 'F74'),
            stats_cell=get_env(f'{prefix}STATS_CELL', 'F83'),
            origen_cell=get_env(f'{prefix}ORIGEN_CELL', 'E2'),
            destino_cell=get_env(f'{prefix}DESTINO_CELL', 'F2'),
            origen_url=get_env(f'{prefix}ORIGEN_URL', ''),
            solo_extras=get_env_bool(f'{prefix}SOLO_EXTRAS', False),
            extras_filas=extras_filas,
            extras_col=get_env(f'{prefix}EXTRAS_COL', EXTRAS_COL),
            extras_limit_col=get_env(f'{prefix}EXTRAS_LIMIT_COL', ''),
            extras_check_col=get_env(f'{prefix}EXTRAS_CHECK_COL', ''),
        )
    return configs

SHEET_CONFIGS = load_sheet_configs()

# URLs API Skyscanner v3
BASE_V3 = "https://partners.api.skyscanner.net/apiservices/v3"
URL_AUTOSUGGEST = f"{BASE_V3}/autosuggest/flights"
URL_LIVE_CREATE = f"{BASE_V3}/flights/live/search/create"
URL_LIVE_POLL = f"{BASE_V3}/flights/live/search/poll/{{token}}"

# ============================================================================
# LOGGING ROBUSTO (con rotación para 24/7)
# ============================================================================

for dir_path in [os.path.dirname(LOG_FILE), os.path.dirname(ENTITY_CACHE_FILE), os.path.dirname(LOCKFILE_PATH)]:
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    try:
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logging.warning(f"No se pudo crear log rotativo: {e}")

setup_logging()

# ============================================================================
# CLASES AUXILIARES
# ============================================================================

class APIMetrics:
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.cache_hits = 0
        self.start_time = datetime.now()
        self.searches_completed = 0
        self.avg_poll_rounds = []
    
    def record_call(self, success=True, from_cache=False):
        self.total_calls += 1
        if from_cache: self.cache_hits += 1
        if success: self.successful_calls += 1
        else: self.failed_calls += 1
    
    def record_search(self, poll_rounds: int):
        self.searches_completed += 1
        self.avg_poll_rounds.append(poll_rounds)
    
    def get_stats(self):
        avg_polls = sum(self.avg_poll_rounds) / len(self.avg_poll_rounds) if self.avg_poll_rounds else 0
        return {
            'total_calls': self.total_calls,
            'successful_calls': self.successful_calls,
            'cache_hits': self.cache_hits,
            'success_rate': self.successful_calls / max(self.total_calls, 1) * 100,
            'cache_hit_rate': self.cache_hits / max(self.total_calls, 1) * 100,
            'runtime_minutes': (datetime.now() - self.start_time).total_seconds() / 60,
            'searches_completed': self.searches_completed,
            'avg_poll_rounds': round(avg_polls, 1)
        }


class RateLimiter:
    def __init__(self, max_calls=None, time_window=None):
        self.max_calls = max_calls or MAX_CALLS_PER_MIN
        self.time_window = time_window or RATE_LIMIT_WINDOW
        self.calls = []
    
    def wait_if_needed(self):
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.time_window]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.time_window - (now - self.calls[0]) + 0.5
            logging.info(f"Rate limit, esperando {sleep_time:.1f}s")
            time.sleep(max(0.0, sleep_time))
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.time_window]
        self.calls.append(now)


class EntityCache:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or ENTITY_CACHE_FILE
        self.cache = self._load_cache()
    
    def _load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logging.info(f" Cache entidades: {len(data)} entradas")
                    return data
        except Exception as e:
            logging.error(f" Error cargando cache: {e}")
        return {}
    
    def _save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f" Error guardando cache: {e}")
    
    def get(self, iata: str) -> Optional[Dict]:
        return self.cache.get((iata or "").upper())
    
    def set(self, iata: str, entity_id: str, name: str, **kwargs):
        key = (iata or "").upper()
        self.cache[key] = {'entity_id': entity_id, 'name': name, 'iata': key, 'cached_at': datetime.now().isoformat(), **kwargs}
        self._save_cache()


metrics = APIMetrics()
rate_limiter = RateLimiter()
entity_cache = EntityCache()

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_sheet_url(cfg: SheetConfig) -> str:
    return cfg.origen_url if cfg.origen_url else SHEET_ORIGEN_URL

def validar_iata_code(iata: str) -> bool:
    return bool(iata and len(iata) == 3 and iata.isalpha())

def validar_fecha(fecha_str: str):
    try:
        d = datetime.strptime(fecha_str, '%Y-%m-%d').date()
        return (d >= datetime.now().date(), None if d >= datetime.now().date() else "Fecha pasada")
    except ValueError:
        return False, "Formato inválido"

def sanitizar_entrada(valor: str) -> str:
    if not valor: return ""
    return re.sub(r'[^A-Z0-9\-]', '', valor.upper().strip())

def _price_to_mxn(amount, unit: str) -> float:
    try: x = float(amount)
    except Exception: return 0.0
    u = (unit or "").upper()
    if "MICRO" in u: return x / 1_000_000.0
    if "MILLI" in u: return x / 1_000.0
    if "CENTI" in u: return x / 100.0
    return x

def retry_with_backoff(max_retries=3, base_delay=1.0):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    if attempt == max_retries - 1:
                        logging.error(f" Falló tras {max_retries} intentos: {e}")
                        raise
                    delay = base_delay * (2 ** attempt)
                    logging.warning(f" Reintento en {delay:.1f}s...")
                    time.sleep(delay)
        return wrapper
    return deco

def sheets_retry(func):
    """Decorator para reintentar operaciones de Google Sheets en caso de 429."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if '429' in str(e) and attempt < max_retries - 1:
                    wait = (attempt + 1) * 30  # 30s, 60s, 90s
                    logging.warning(f" ⏳ Google Sheets 429 - esperando {wait}s ({attempt + 1}/{max_retries})")
                    time.sleep(wait)
                else:
                    raise
    return wrapper

# ============================================================================
# GOOGLE SHEETS - CONEXIÓN BASE
# ============================================================================

def conectar_sheets(max_retries=3):
    for attempt in range(max_retries):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE, scope)
            client = gspread.authorize(creds)
            logging.info(" Conexión Google Sheets OK")
            return client
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f" Error conectando Sheets: {e}")
                raise
            logging.warning(f" Reintento conexión Sheets en {2 ** attempt}s...")
            time.sleep(2 ** attempt)

# ============================================================================
# SHEET MANAGER - CACHÉ DE CONEXIONES (NUEVO)
# ============================================================================

class SheetManager:
    """Cachea conexiones a spreadsheets y worksheets para evitar
    abrir desde cero en cada operación (reduce GetSpreadsheet 99%).
    
    ANTES: cada lectura/escritura hacía open_by_url() + worksheet() = 2-3 requests extra
    AHORA: solo hace acell()/update() = 1 request (la conexión ya está cacheada)
    """
    
    def __init__(self, client):
        self.client = client
        self._spreadsheets = {}   # cache: url → spreadsheet object
        self._worksheets = {}     # cache: (url, sheet_name) → worksheet object
        logging.info(" SheetManager inicializado (caché de conexiones activo)")
    
    def get_spreadsheet(self, url: str):
        """Abre el spreadsheet UNA vez y lo reutiliza en llamadas posteriores."""
        if url not in self._spreadsheets:
            self._spreadsheets[url] = self.client.open_by_url(url)
            logging.info(f"  Spreadsheet abierto y cacheado: ...{url[-30:]}")
        return self._spreadsheets[url]
    
    def get_worksheet(self, url: str, sheet_name: str):
        """Abre el worksheet UNA vez y lo reutiliza en llamadas posteriores."""
        key = (url, sheet_name)
        if key not in self._worksheets:
            ss = self.get_spreadsheet(url)
            self._worksheets[key] = ss.worksheet(sheet_name)
            logging.info(f"  Worksheet cacheado: {sheet_name}")
        return self._worksheets[key]
    
    def invalidar(self, url: str = None):
        """Limpia caché completo o de una URL específica (útil tras errores)."""
        if url:
            self._spreadsheets.pop(url, None)
            self._worksheets = {k: v for k, v in self._worksheets.items() if k[0] != url}
            logging.info(f"  Caché invalidado para: ...{url[-30:]}")
        else:
            self._spreadsheets.clear()
            self._worksheets.clear()
            logging.info("  Caché de sheets completamente invalidado")
    
    def reconectar(self):
        """Reconecta el cliente de Google Sheets y limpia todo el caché."""
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE, scope)
        self.client = gspread.authorize(creds)
        self.invalidar()
        logging.info(" SheetManager reconectado exitosamente")
    
    def get_cache_stats(self) -> str:
        """Retorna estadísticas del caché para logging."""
        return (f"Spreadsheets cacheados: {len(self._spreadsheets)} | "
                f"Worksheets cacheados: {len(self._worksheets)}")

# ============================================================================
# GOOGLE SHEETS - OPERACIONES (AHORA USAN SheetManager)
# ============================================================================

@sheets_retry
def is_enabled(sm: SheetManager, cfg: SheetConfig) -> bool:
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        raw = (ws.acell(cfg.switch_cell).value or "").strip().upper()
        return raw == "ON"
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f"Error leyendo switch: {e}")
        return False

@sheets_retry
def apagar_switch(sm: SheetManager, cfg: SheetConfig):
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        ws.update_acell(cfg.switch_cell, 'OFF')
        ws.update_acell(cfg.off_switch_cell, 'OFF')
        logging.info(f" {cfg.captura_sheet}: Switch apagado")
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f" Error apagando switch: {e}")

@sheets_retry
def actualizar_fecha(sm: SheetManager, cfg: SheetConfig):
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s = metrics.get_stats()
        msg = (f"Última: {ts}\n"
               f"Búsquedas: {s['searches_completed']} | "
               f"Polls prom: {s['avg_poll_rounds']}")
        ws.update_acell(cfg.stats_cell, msg)
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f" Error actualizando stats: {e}")


class IncrementalWriter:
    def __init__(self, ws_resultados, start_row: int = 2):
        self.ws = ws_resultados
        self.current_row = start_row
        self.rows_written = 0
        self.batch_buffer = []
    
    def write_row(self, data: List[Any]):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                row_range = f"A{self.current_row}:J{self.current_row}"
                self.ws.update(row_range, [data], value_input_option="USER_ENTERED")
                self.current_row += 1
                self.rows_written += 1
                return
            except gspread.exceptions.APIError as e:
                if '429' in str(e) and attempt < max_retries - 1:
                    wait = (attempt + 1) * 30
                    logging.warning(f"  Sheets 429 al escribir - esperando {wait}s")
                    time.sleep(wait)
                else:
                    logging.error(f" Error escribiendo fila: {e}")
                    self.batch_buffer.append(data)
                    return
            except Exception as e:
                logging.error(f" Error escribiendo fila: {e}")
                self.batch_buffer.append(data)
                return
    
    def flush_buffer(self):
        if self.batch_buffer:
            pending = list(self.batch_buffer)
            self.batch_buffer = []
            for data in pending:
                self.write_row(data)
    
    def get_rows_written(self) -> int:
        return self.rows_written

@sheets_retry
def limpiar_resultados_seguro(ws_resultados):
    try:
        frozen = ws_resultados._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
        start_row = max(2, frozen + 1)
        last_row = ws_resultados.row_count
        if last_row >= start_row:
            ws_resultados.batch_clear([f"A{start_row}:J{last_row}"])
            logging.info(f" Limpiado: filas {start_row}-{last_row}")
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f"Error limpiando: {e}")

# ============================================================================
# SKYSCANNER API
# ============================================================================

@retry_with_backoff(max_retries=3)
def obtener_entity_info(iata: str) -> tuple:
    if not validar_iata_code(iata):
        logging.error(f" IATA inválido: {iata}")
        return None, iata
    iata = sanitizar_entrada(iata)
    cached = entity_cache.get(iata)
    if cached and cached.get("entity_id"):
        logging.info(f" Cache: {iata} → {cached.get('name')}")
        metrics.record_call(True, True)
        return cached["entity_id"], cached.get("name", iata)
    
    rate_limiter.wait_if_needed()
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}
    payload = {
        "query": {"market": MARKET, "locale": LOCALE, "searchTerm": iata,
                   "includedEntityTypes": ["PLACE_TYPE_AIRPORT", "PLACE_TYPE_CITY"]},
        "limit": 20, "isDestination": False
    }
    try:
        r = requests.post(URL_AUTOSUGGEST, json=payload, headers=headers, timeout=12)
        r.raise_for_status()
        places = r.json().get("places", []) or []
        for p in places:
            if p.get("type") == "PLACE_TYPE_AIRPORT" and (p.get("iataCode") or "").upper() == iata:
                entity_id = p.get("entityId")
                nombre = p.get("name", iata)
                entity_cache.set(iata, entity_id, nombre, type="PLACE_TYPE_AIRPORT")
                metrics.record_call(True)
                logging.info(f" {iata} → EntityID: {entity_id} ({nombre})")
                return entity_id, nombre
        for p in places:
            if p.get("type") == "PLACE_TYPE_CITY":
                ai = p.get("airportInformation") or {}
                if (ai.get("iataCode") or "").upper() == iata and ai.get("entityId"):
                    entity_id = ai["entityId"]
                    nombre = p.get("name", iata)
                    entity_cache.set(iata, entity_id, nombre)
                    metrics.record_call(True)
                    return entity_id, nombre
        logging.error(f" No se encontró EntityID para {iata}")
        metrics.record_call(False)
        return None, iata
    except requests.RequestException as e:
        logging.error(f" Error Autosuggest {iata}: {e}")
        metrics.record_call(False)
        return None, iata


def _extraer_precios_de_respuesta(resp_json: dict) -> dict:
    out = {'best': None, 'cheapest': None, 'fastest': None, 'status': None}
    try:
        content = resp_json.get("content", {})
        results = content.get("results", {})
        itins = results.get("itineraries", {}) or {}
        sorting = content.get("sortingOptions", {}) or {}
        out['status'] = content.get("status")
        
        def get_price(itin_id: str) -> Optional[int]:
            it = itins.get(itin_id)
            if not it: return None
            price_obj = it.get("price") or (it.get("pricingOptions", [{}])[0].get("price"))
            if not price_obj: return None
            mxn = _price_to_mxn(price_obj.get("amount", "0"), price_obj.get("unit", "PRICE_UNIT_MICRO"))
            return int(round(mxn)) if mxn > 0 else None
        
        for api_key, out_key in [("best", "best"), ("cheapest", "cheapest"), ("fastest", "fastest")]:
            lista = sorting.get(api_key, []) or []
            if lista:
                price = get_price(lista[0].get("itineraryId"))
                if price: out[out_key] = price
        
        if out['cheapest'] is None and itins:
            min_price = None
            for it in itins.values():
                price_obj = it.get("price") or (it.get("pricingOptions", [{}])[0].get("price"))
                if not price_obj: continue
                mxn = _price_to_mxn(price_obj.get("amount", "0"), price_obj.get("unit", "PRICE_UNIT_MICRO"))
                if mxn > 0 and (min_price is None or mxn < min_price): min_price = mxn
            if min_price: out['cheapest'] = int(round(min_price))
        return out
    except Exception as e:
        logging.warning(f" Error extrayendo precios: {e}")
        return out


def _is_search_complete(status: str) -> bool:
    return status == "RESULT_STATUS_COMPLETE"


@retry_with_backoff(max_retries=2)
def buscar_precios_skyscanner(entity_orig, entity_dest, ida, vuelta, iata_orig, iata_dest) -> dict:
    ok_i, _ = validar_fecha(ida)
    ok_v, _ = validar_fecha(vuelta)
    if not ok_i or not ok_v: return {'best': None, 'cheapest': None}
    if datetime.strptime(vuelta, "%Y-%m-%d") < datetime.strptime(ida, "%Y-%m-%d"):
        logging.warning(" Vuelta anterior a ida")
        return {'best': None, 'cheapest': None}
    
    if USE_ENTITY_ID:
        origin_place = {"entityId": entity_orig}
        dest_place = {"entityId": entity_dest}
        route_log = f"{iata_orig}({entity_orig}) → {iata_dest}({entity_dest})"
    else:
        origin_place = {"iata": iata_orig}
        dest_place = {"iata": iata_dest}
        route_log = f"{iata_orig} → {iata_dest}"
    
    rate_limiter.wait_if_needed()
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}
    payload = {
        "query": {
            "market": MARKET, "locale": LOCALE, "currency": CURRENCY,
            "queryLegs": [
                {"originPlaceId": origin_place, "destinationPlaceId": dest_place,
                 "date": {"year": int(ida[:4]), "month": int(ida[5:7]), "day": int(ida[8:10])}},
                {"originPlaceId": dest_place, "destinationPlaceId": origin_place,
                 "date": {"year": int(vuelta[:4]), "month": int(vuelta[5:7]), "day": int(vuelta[8:10])}}
            ],
            "adults": ADULTOS, "cabinClass": CABIN,
            "childrenAges": [] if CHILDREN == 0 else [8] * CHILDREN
        }
    }
    try:
        logging.info(f" Búsqueda: {route_log} | {ida} → {vuelta}")
        search_start = time.time()
        r = requests.post(URL_LIVE_CREATE, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        best_result = _extraer_precios_de_respuesta(j)
        token = j.get("sessionToken")
        if not token:
            logging.warning(" Sin sessionToken, retornando resultado inicial")
            metrics.record_search(0)
            return _apply_force_best(best_result)
        
        poll_count = 0
        status = best_result.get('status')
        deadline = search_start + POLL_CONFIG["POLL_DEADLINE_SECONDS"]
        while True:
            elapsed = time.time() - search_start
            if time.time() >= deadline:
                logging.info(f"   ⏱ Deadline ({elapsed:.1f}s), polls: {poll_count}")
                break
            if (POLL_CONFIG["WAIT_FOR_COMPLETE"] and _is_search_complete(status) and
                poll_count >= POLL_CONFIG["MIN_GUARANTEED_POLLS"]):
                logging.info(f"    COMPLETE tras {poll_count} polls")
                break
            if poll_count >= POLL_CONFIG["MAX_POLL_ROUNDS"]:
                logging.info(f"    Máximo polls ({poll_count})")
                break
            time.sleep(POLL_CONFIG["POLL_SLEEP_SECONDS"])
            rate_limiter.wait_if_needed()
            try:
                poll_r = requests.post(URL_LIVE_POLL.format(token=token), headers=headers, json={}, timeout=45)
                if poll_r.status_code == 429:
                    retry_after = int(poll_r.headers.get("Retry-After", "3"))
                    logging.warning(f" Rate limit, esperando {retry_after}s")
                    time.sleep(min(retry_after, 10))
                    continue
                poll_r.raise_for_status()
                poll_count += 1
                new_result = _extraer_precios_de_respuesta(poll_r.json())
                status = new_result.get('status')
                new_cheap = new_result.get('cheapest')
                old_cheap = best_result.get('cheapest')
                if new_cheap is not None:
                    if old_cheap is None or new_cheap < old_cheap:
                        logging.info(f"    Poll {poll_count}: ${old_cheap or '?'} → ${new_cheap}")
                        best_result = new_result
            except requests.RequestException as e:
                logging.warning(f"    Error poll {poll_count}: {e}")
                continue
        
        metrics.record_search(poll_count)
        final_result = _apply_force_best(best_result)
        logging.info(f"    FINAL: Cheapest=${final_result.get('cheapest')} | Best=${final_result.get('best')} | Polls={poll_count}")
        return final_result
    except requests.RequestException as e:
        logging.error(f" Error búsqueda: {e}")
        metrics.record_call(False)
        return {'best': None, 'cheapest': None}


def _apply_force_best(result: dict) -> dict:
    if not FORCE_BEST_TO_CHEAPEST: return result
    c = result.get('cheapest')
    b = result.get('best')
    if c is not None and (b is None or b > c): result['best'] = c
    return result

# ============================================================================
# LECTURA DE PARÁMETROS (AHORA USAN SheetManager)
# ============================================================================

@sheets_retry
def leer_parametros_y_pares(sm: SheetManager, cfg: SheetConfig) -> List[tuple]:
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        valores = ws.get_all_values()
        pares = []
        for i, fila in enumerate(valores[11:], start=12):
            if len(fila) >= 3:
                ida = (fila[1] or "").strip()
                vuelta = (fila[2] or "").strip()
                if re.match(r"\d{4}-\d{2}-\d{2}$", ida) and re.match(r"\d{4}-\d{2}-\d{2}$", vuelta):
                    ok_i, _ = validar_fecha(ida)
                    ok_v, _ = validar_fecha(vuelta)
                    if ok_i and ok_v and datetime.strptime(vuelta, "%Y-%m-%d") >= datetime.strptime(ida, "%Y-%m-%d"):
                        pares.append((ida, vuelta))
        logging.info(f" {cfg.captura_sheet}: {len(pares)} pares de fechas")
        return pares
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f" Error leyendo parámetros: {e}")
        return []

@sheets_retry
def obtener_origenes_extras(sm: SheetManager, cfg: SheetConfig) -> List[str]:
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        origenes = []
        filas = cfg.extras_filas if cfg.extras_filas else ORIGENES_FILAS
        col = cfg.extras_col or EXTRAS_COL
        for fila in filas:
            v = ws.acell(f"{col}{fila}").value
            if v and v.strip():
                iata = sanitizar_entrada(v.strip())
                if validar_iata_code(iata): origenes.append(iata)
        return origenes
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f" Error orígenes extra: {e}")
        return []

@sheets_retry
def obtener_origenes_extras_con_limite(sm: SheetManager, cfg: SheetConfig) -> List[Dict]:
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        all_values = ws.get_all_values()
        filas = cfg.extras_filas if cfg.extras_filas else ORIGENES_FILAS
        col_iata = ord(cfg.extras_col.upper()) - ord('A')
        col_limit = ord(cfg.extras_limit_col.upper()) - ord('A') if cfg.extras_limit_col else -1
        col_check = ord(cfg.extras_check_col.upper()) - ord('A') if cfg.extras_check_col else -1
        origenes = []
        for fila in filas:
            if fila - 1 >= len(all_values): continue
            row = all_values[fila - 1]
            iata_val = row[col_iata] if len(row) > col_iata else ""
            if not iata_val or not iata_val.strip(): continue
            iata = sanitizar_entrada(iata_val.strip())
            if not validar_iata_code(iata): continue
            if col_check >= 0:
                check_val = row[col_check] if len(row) > col_check else ""
                if str(check_val).upper() != "TRUE":
                    logging.info(f"    {iata} (fila {fila}): checkbox desactivado, omitido")
                    continue
            limite = None
            if col_limit >= 0:
                limit_val = row[col_limit] if len(row) > col_limit else ""
                if limit_val:
                    try:
                        clean = str(limit_val).replace(',', '').replace('$', '').replace(' ', '').strip()
                        limite = int(float(clean))
                    except (ValueError, TypeError):
                        logging.warning(f"    {iata} (fila {fila}): límite no numérico '{limit_val}'")
                        limite = None
            origenes.append({'iata': iata, 'limite': limite, 'fila': fila})
            logging.info(f"    {iata} (fila {fila}): límite=${limite:,}" if limite else f"    {iata} (fila {fila}): sin límite")
        logging.info(f" {cfg.captura_sheet}: {len(origenes)} orígenes con checkbox activo")
        return origenes
    except gspread.exceptions.APIError:
        raise
    except Exception as e:
        logging.error(f" Error leyendo orígenes con límite: {e}")
        return []

def filtrar_extras_unicos(extras: List[str], iata_principal: str) -> List[str]:
    vistos = {iata_principal}
    out = []
    for iata in extras:
        if iata not in vistos:
            vistos.add(iata)
            out.append(iata)
    return out

# ============================================================================
# PROCESO PRINCIPAL (AHORA USA SheetManager)
# ============================================================================

def procesar_hoja(sm: SheetManager, version: str, cfg: SheetConfig) -> bool:
    logging.info(f"\n{'='*60}")
    logging.info(f" PROCESANDO {version} ({cfg.captura_sheet} → {cfg.resultado_sheet})")
    logging.info(f"{'='*60}")
    logging.info(f"  {sm.get_cache_stats()}")
    try:
        if not is_enabled(sm, cfg):
            logging.info(f" {version}: Deshabilitada")
            return False
        if cfg.solo_extras:
            return _procesar_hoja_solo_extras(sm, version, cfg)
        return _procesar_hoja_normal(sm, version, cfg)
    except Exception as e:
        logging.error(f" Error en {version}: {e}")
        try: apagar_switch(sm, cfg)
        except: pass
        return False


def _procesar_hoja_normal(sm: SheetManager, version: str, cfg: SheetConfig) -> bool:
    try:
        ws_captura = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        valor_origen = ws_captura.acell(cfg.origen_cell).value
        valor_destino = ws_captura.acell(cfg.destino_cell).value
        if not valor_origen or not valor_destino:
            logging.error(f" Origen/destino vacío")
            apagar_switch(sm, cfg)
            return False
        iata_origen = sanitizar_entrada(valor_origen)
        iata_destino = sanitizar_entrada(valor_destino)
        if not (validar_iata_code(iata_origen) and validar_iata_code(iata_destino)):
            logging.error(f" IATA inválido")
            apagar_switch(sm, cfg)
            return False
        logging.info(f" Ruta: {iata_origen} → {iata_destino}")
        entity_orig, nombre_orig = obtener_entity_info(iata_origen)
        entity_dest, nombre_dest = obtener_entity_info(iata_destino)
        if not entity_orig or not entity_dest:
            logging.error(f" No se obtuvo EntityID")
            apagar_switch(sm, cfg)
            return False
        pares = leer_parametros_y_pares(sm, cfg)
        if not pares:
            logging.error(f" Sin fechas válidas")
            apagar_switch(sm, cfg)
            return False
        ws_resultados = sm.get_worksheet(SHEET_DESTINO_URL, cfg.resultado_sheet)
        limpiar_resultados_seguro(ws_resultados)
        writer = IncrementalWriter(ws_resultados, start_row=2)
        
        logging.info(f"\n Ruta principal ({len(pares)} fechas)...")
        for idx, (ida, vuelta) in enumerate(pares, 1):
            logging.info(f"\n[{idx}/{len(pares)}] {iata_origen}→{iata_destino} | {ida} - {vuelta}")
            precios = buscar_precios_skyscanner(entity_orig, entity_dest, ida, vuelta, iata_origen, iata_destino)
            cheapest = precios.get('cheapest')
            best = precios.get('best')
            if cheapest:
                fila = [iata_origen, nombre_orig, entity_orig, iata_destino, nombre_dest, entity_dest,
                        ida, vuelta, f"${cheapest:,} MXN", f"${best:,} MXN" if best else ""]
                if WRITE_IMMEDIATELY:
                    writer.write_row(fila)
                    logging.info(f"   Escrito: Cheapest=${cheapest:,} | Best=${best:,} MXN")
        
        extras = filtrar_extras_unicos(obtener_origenes_extras(sm, cfg), iata_origen)
        if extras:
            logging.info(f"\n {len(extras)} orígenes extra: {extras}")
            for iata_extra in extras:
                entity_ex, nombre_ex = obtener_entity_info(iata_extra)
                if not entity_ex: continue
                for idx, (ida, vuelta) in enumerate(pares, 1):
                    logging.info(f"\n[{iata_extra}] [{idx}/{len(pares)}] {ida} - {vuelta}")
                    precios = buscar_precios_skyscanner(entity_ex, entity_dest, ida, vuelta, iata_extra, iata_destino)
                    cheapest = precios.get('cheapest')
                    best = precios.get('best')
                    if cheapest:
                        fila = [iata_extra, nombre_ex or iata_extra, entity_ex, iata_destino, nombre_dest, entity_dest,
                                ida, vuelta, f"${cheapest:,} MXN", f"${best:,} MXN" if best else ""]
                        if WRITE_IMMEDIATELY: writer.write_row(fila)
        
        writer.flush_buffer()
        logging.info(f"\n {version}: {writer.get_rows_written()} filas escritas")
        actualizar_fecha(sm, cfg)
        apagar_switch(sm, cfg)
        return True
    except Exception as e:
        logging.error(f" Error en {version}: {e}")
        try: apagar_switch(sm, cfg)
        except: pass
        return False


def _procesar_hoja_solo_extras(sm: SheetManager, version: str, cfg: SheetConfig) -> bool:
    try:
        logging.info(f" Modo: SOLO EXTRAS con límite de precio")
        ws_captura = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        valor_destino = ws_captura.acell(cfg.destino_cell).value
        if not valor_destino:
            logging.error(f" Destino vacío en {cfg.destino_cell}")
            apagar_switch(sm, cfg)
            return False
        iata_destino = sanitizar_entrada(valor_destino)
        if not validar_iata_code(iata_destino):
            logging.error(f" IATA destino inválido: {iata_destino}")
            apagar_switch(sm, cfg)
            return False
        logging.info(f" Destino: {iata_destino}")
        entity_dest, nombre_dest = obtener_entity_info(iata_destino)
        if not entity_dest:
            logging.error(f" No se obtuvo EntityID para destino {iata_destino}")
            apagar_switch(sm, cfg)
            return False
        pares = leer_parametros_y_pares(sm, cfg)
        if not pares:
            logging.error(f" Sin fechas válidas")
            apagar_switch(sm, cfg)
            return False
        extras = obtener_origenes_extras_con_limite(sm, cfg)
        if not extras:
            logging.warning(f" Sin orígenes extras activos (todos desactivados o vacíos)")
            apagar_switch(sm, cfg)
            return False
        ws_resultados = sm.get_worksheet(SHEET_DESTINO_URL, cfg.resultado_sheet)
        limpiar_resultados_seguro(ws_resultados)
        writer = IncrementalWriter(ws_resultados, start_row=2)
        total_buscados = 0
        total_filtrados = 0
        
        for extra_info in extras:
            iata_extra = extra_info['iata']
            limite = extra_info['limite']
            entity_ex, nombre_ex = obtener_entity_info(iata_extra)
            if not entity_ex:
                logging.warning(f" No se obtuvo EntityID para {iata_extra}, saltando")
                continue
            limite_str = f"${limite:,}" if limite else "sin límite"
            logging.info(f"\n{'─'*50}")
            logging.info(f" {iata_extra} → {iata_destino} | Límite: {limite_str}")
            logging.info(f"{'─'*50}")
            for idx, (ida, vuelta) in enumerate(pares, 1):
                logging.info(f"  [{idx}/{len(pares)}] {iata_extra}→{iata_destino} | {ida} - {vuelta}")
                precios = buscar_precios_skyscanner(entity_ex, entity_dest, ida, vuelta, iata_extra, iata_destino)
                cheapest = precios.get('cheapest')
                best = precios.get('best')
                total_buscados += 1
                if cheapest:
                    if limite and cheapest > limite:
                        total_filtrados += 1
                        logging.info(f"  ${cheapest:,} > límite ${limite:,} → omitido")
                        continue
                    fila = [iata_extra, nombre_ex or iata_extra, entity_ex, iata_destino, nombre_dest, entity_dest,
                            ida, vuelta, f"${cheapest:,} MXN", f"${best:,} MXN" if best else ""]
                    if WRITE_IMMEDIATELY:
                        writer.write_row(fila)
                        logging.info(f"   Escrito: ${cheapest:,} MXN (límite: {limite_str})")
        
        writer.flush_buffer()
        logging.info(f"\n{'='*50}")
        logging.info(f" {version} COMPLETADO:")
        logging.info(f"   Filas escritas: {writer.get_rows_written()}")
        logging.info(f"   Búsquedas: {total_buscados}")
        logging.info(f"   Filtrados por límite: {total_filtrados}")
        logging.info(f"{'='*50}")
        actualizar_fecha(sm, cfg)
        apagar_switch(sm, cfg)
        return True
    except Exception as e:
        logging.error(f" Error en {version} (solo_extras): {e}")
        try: apagar_switch(sm, cfg)
        except: pass
        return False


# ============================================================================
# MAIN - LOOP CONTINUO (AHORA USA SheetManager)
# ============================================================================

def ejecutar_ciclo(sm: SheetManager) -> bool:
    """Ejecuta UN ciclo: revisa switches y procesa hojas activas."""
    metrics.reset()
    
    # Detectar hojas activas CON PAUSA entre cada check
    activos = []
    for ver in PRIORIDAD_PROCESO:
        cfg = SHEET_CONFIGS[ver]
        if is_enabled(sm, cfg):
            activos.append(ver)
        time.sleep(SHEETS_CHECK_DELAY)  # 3 seg entre cada lectura de switch
    
    logging.info(f" Hojas activas: {activos}")
    logging.info(f"  {sm.get_cache_stats()}")
    if not activos:
        return False
    
    # Resolver exclusividad
    if SOLO_UNA_ACTIVA and len(activos) > 1:
        if POLITICA_MULTIPLE == "ABORT":
            logging.warning(" Múltiples ON + ABORT")
            return False
        ver_elegida = activos[0]
        logging.warning(f" Múltiples ON, solo: {ver_elegida}")
        versiones = [ver_elegida]
    else:
        versiones = activos
    
    # Procesar con pausa entre hojas
    for i, ver in enumerate(versiones):
        cfg = SHEET_CONFIGS[ver]
        procesar_hoja(sm, ver, cfg)
        if i < len(versiones) - 1:
            logging.info(f"  Pausa de {PAUSE_BETWEEN_SHEETS}s antes de siguiente hoja...")
            time.sleep(PAUSE_BETWEEN_SHEETS)
    
    # Stats del ciclo
    s = metrics.get_stats()
    logging.info(f"\n{'='*60}")
    logging.info(" ESTADÍSTICAS DEL CICLO")
    logging.info(f"{'='*60}")
    logging.info(f"   Búsquedas: {s['searches_completed']}")
    logging.info(f"   Polls promedio: {s['avg_poll_rounds']}")
    logging.info(f"   Llamadas API: {s['total_calls']}")
    logging.info(f"   Éxito: {s['success_rate']:.1f}%")
    logging.info(f"   Tiempo: {s['runtime_minutes']:.2f} min")
    return True


def main():
    logging.info(" Iniciando Skyscanner Bot - VERSIÓN DOCKER LOOP + CACHE")
    logging.info(f"   Polling: MIN={POLL_CONFIG['MIN_GUARANTEED_POLLS']} | MAX={POLL_CONFIG['MAX_POLL_ROUNDS']} | Deadline={POLL_CONFIG['POLL_DEADLINE_SECONDS']}s")
    logging.info(f"   EntityID: {USE_ENTITY_ID} | Escritura inmediata: {WRITE_IMMEDIATELY}")
    logging.info(f"   Loop: {'CONTINUO' if LOOP_ENABLED else 'UNA VEZ'} | Intervalo: {LOOP_INTERVAL_SECONDS}s")
    logging.info(f"   Pausa entre checks: {SHEETS_CHECK_DELAY}s | Entre hojas: {PAUSE_BETWEEN_SHEETS}s")
    logging.info(f"   Hojas configuradas: {list(SHEET_CONFIGS.keys())}")
    logging.info(f"   SheetManager: Caché de conexiones ACTIVO")
    
    if not API_KEY:
        logging.error(" Falta SKYSCANNER_API_KEY en .env")
        return
    
    # Crear cliente y envolver en SheetManager (NUEVO)
    client = conectar_sheets()
    sm = SheetManager(client)
    
    ciclo_num = 0
    
    while True:
        ciclo_num += 1
        logging.info(f"\n{'#'*60}")
        logging.info(f" CICLO #{ciclo_num} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{'#'*60}")
        
        try:
            procesado = ejecutar_ciclo(sm)
            if not procesado:
                logging.info(f" Ninguna hoja en ON")
        except gspread.exceptions.APIError as e:
            if '429' in str(e):
                logging.warning(f"  Google Sheets saturado, esperando 60s extra...")
                time.sleep(60)
            else:
                logging.error(f" Error de API Sheets en ciclo: {e}")
                # Invalidar caché en errores de API no-429 (puede ser token expirado)
                sm.invalidar()
        except Exception as e:
            logging.error(f" Error inesperado en ciclo #{ciclo_num}: {e}")
            try:
                logging.info(" Reconectando SheetManager...")
                sm.reconectar()
            except Exception as e2:
                logging.error(f" Error reconectando: {e2}")
        
        # Modo una sola vez (para Task Scheduler externo)
        if not LOOP_ENABLED:
            logging.info(" Modo una sola vez. Finalizando.")
            break
        
        logging.info(f"  Esperando {LOOP_INTERVAL_SECONDS}s ({LOOP_INTERVAL_SECONDS // 60} min) hasta el siguiente ciclo...")
        time.sleep(LOOP_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()