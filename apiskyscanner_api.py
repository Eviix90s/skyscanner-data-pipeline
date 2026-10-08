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
- v3.3: detecta RESULT_STATUS_COMPLETE (status en raíz de la respuesta), búsquedas en
  paralelo (SS_PARALLEL_SEARCHES), escritura por lotes (SS_WRITE_BATCH_SIZE) y
  ampliación automática de la hoja de resultados
-v 3.3 skyscanner
"""

import os, requests, gspread, time, json, hashlib, logging, re, sys, threading
from concurrent.futures import ThreadPoolExecutor
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

# Rendimiento
PARALLEL_SEARCHES = get_env_int('SS_PARALLEL_SEARCHES', 3)   # búsquedas Skyscanner simultáneas por bot
WRITE_BATCH_SIZE = get_env_int('SS_WRITE_BATCH_SIZE', 5)     # filas por escritura a Sheets (1 = fila por fila)

# Escalas: con SS_SOLO_DIRECTOS=true, cheapest y best se calculan SOLO con itinerarios sin escalas
# (equivale al filtro "Directo" de la web). Si no hay directos para esa fecha, la fila se omite,
# salvo que SS_SIN_DIRECTO_USAR_ESCALAS=true, en cuyo caso se usan los vuelos con escala.
SOLO_DIRECTOS = get_env_bool('SS_SOLO_DIRECTOS', False)
SIN_DIRECTO_USAR_ESCALAS = get_env_bool('SS_SIN_DIRECTO_USAR_ESCALAS', False)

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
    switch_url: str = ""         # URL de spreadsheet externo para el switch (ej: Administración VALV)
    switch_sheet_name: str = ""  # Nombre de la hoja en el spreadsheet externo
    # --- Modo TRIANGULO (multi-city): todo lo lee de la propia hoja ---
    tipo: str = "REDONDO"        # REDONDO (default) | TRIANGULO
    leg1_origen_cell: str = "E2"
    leg1_destino_cell: str = "F2"
    leg2_origen_cell: str = "G2"
    leg2_destino_cell: str = "H2"
    personas_cell: str = ""      # adultos (ej. L3)
    cabina_cell: str = ""        # Economia / Business... (ej. I2)
    mercado_cell: str = ""       # MX (ej. I4)
    moneda_cell: str = ""        # MXN (ej. Q3)
    directo_cell: str = ""       # TRUE/FALSE preferir directo (ej. M5)
    limite_cell: str = ""        # límite de precio por persona (ej. N10)
    modo_cell: str = ""          # "Más barato" / "Recomendado" (ej. M4)
    precio_col: str = "N"        # columna donde se escribe el precio
    precio_fecha_col: str = "O"  # columna con la fecha de cada fila de precio
    precio_fila_inicio: int = 11
    precio_fila_fin: int = 130
    # Filtro de aerolíneas (aplica a cualquier TIPO): ['AM'] = solo Aeroméxico; vacío = todas
    aerolineas: List[str] = field(default_factory=list)


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
            off_switch_cell=get_env(f'{prefix}OFF_SWITCH_CELL', ''),
            stats_cell=get_env(f'{prefix}STATS_CELL', 'F83'),
            origen_cell=get_env(f'{prefix}ORIGEN_CELL', 'E2'),
            destino_cell=get_env(f'{prefix}DESTINO_CELL', 'F2'),
            origen_url=get_env(f'{prefix}ORIGEN_URL', ''),
            solo_extras=get_env_bool(f'{prefix}SOLO_EXTRAS', False),
            extras_filas=extras_filas,
            extras_col=get_env(f'{prefix}EXTRAS_COL', EXTRAS_COL),
            extras_limit_col=get_env(f'{prefix}EXTRAS_LIMIT_COL', ''),
            extras_check_col=get_env(f'{prefix}EXTRAS_CHECK_COL', ''),
            switch_url=get_env(f'{prefix}SWITCH_URL', ''),
            switch_sheet_name=get_env(f'{prefix}SWITCH_SHEET', ''),
            tipo=get_env(f'{prefix}TIPO', 'REDONDO').upper(),
            leg1_origen_cell=get_env(f'{prefix}LEG1_ORIGEN_CELL', 'E2'),
            leg1_destino_cell=get_env(f'{prefix}LEG1_DESTINO_CELL', 'F2'),
            leg2_origen_cell=get_env(f'{prefix}LEG2_ORIGEN_CELL', 'G2'),
            leg2_destino_cell=get_env(f'{prefix}LEG2_DESTINO_CELL', 'H2'),
            personas_cell=get_env(f'{prefix}PERSONAS_CELL', ''),
            cabina_cell=get_env(f'{prefix}CABINA_CELL', ''),
            mercado_cell=get_env(f'{prefix}MERCADO_CELL', ''),
            moneda_cell=get_env(f'{prefix}MONEDA_CELL', ''),
            directo_cell=get_env(f'{prefix}DIRECTO_CELL', ''),
            limite_cell=get_env(f'{prefix}LIMITE_CELL', ''),
            modo_cell=get_env(f'{prefix}MODO_CELL', ''),
            precio_col=get_env(f'{prefix}PRECIO_COL', 'N').upper(),
            precio_fecha_col=get_env(f'{prefix}PRECIO_FECHA_COL', 'O').upper(),
            precio_fila_inicio=get_env_int(f'{prefix}PRECIO_FILA_INICIO', 11),
            precio_fila_fin=get_env_int(f'{prefix}PRECIO_FILA_FIN', 130),
            aerolineas=[a.strip().upper() for a in get_env(f'{prefix}AEROLINEAS', '').split(',') if a.strip()],
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
        self._lock = threading.Lock()
        self.reset()

    def reset(self):
        with self._lock:
            self.total_calls = 0
            self.successful_calls = 0
            self.failed_calls = 0
            self.cache_hits = 0
            self.start_time = datetime.now()
            self.searches_completed = 0
            self.avg_poll_rounds = []

    def record_call(self, success=True, from_cache=False):
        with self._lock:
            self.total_calls += 1
            if from_cache: self.cache_hits += 1
            if success: self.successful_calls += 1
            else: self.failed_calls += 1

    def record_search(self, poll_rounds: int):
        with self._lock:
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
    """Limitador de llamadas por ventana de tiempo. Seguro para varios hilos:
    el lock garantiza que las búsquedas en paralelo comparten el mismo cupo."""
    def __init__(self, max_calls=None, time_window=None):
        self.max_calls = max_calls or MAX_CALLS_PER_MIN
        self.time_window = time_window or RATE_LIMIT_WINDOW
        self.calls = []
        self._lock = threading.Lock()

    def wait_if_needed(self):
        with self._lock:
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

def _get_switch_worksheet(sm: SheetManager, cfg: SheetConfig):
    """Retorna el worksheet donde está el switch.
    Si cfg.switch_url está configurado, usa el spreadsheet externo (ej: Administración VALV).
    Si no, usa el captura_sheet del propio bot."""
    if cfg.switch_url and cfg.switch_sheet_name:
        return sm.get_worksheet(cfg.switch_url, cfg.switch_sheet_name)
    return sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)


@sheets_retry
def is_enabled(sm: SheetManager, cfg: SheetConfig) -> bool:
    try:
        ws = _get_switch_worksheet(sm, cfg)
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
        ws = _get_switch_worksheet(sm, cfg)
        ws.update_acell(cfg.switch_cell, 'OFF')
        if cfg.off_switch_cell:
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
    """Escribe resultados en la hoja en lotes de WRITE_BATCH_SIZE filas
    (1 request por lote en vez de 1 por fila) y amplía la hoja si faltan filas
    (antes: error 'exceeds grid limits' y la fila se perdía)."""
    def __init__(self, ws_resultados, start_row: int = 2, batch_size: int = None, ultima_col: str = "J"):
        self.ws = ws_resultados
        self.current_row = start_row          # siguiente fila libre en la hoja
        self.rows_written = 0
        self.batch_buffer = []
        self.batch_size = max(1, batch_size or WRITE_BATCH_SIZE)
        self.ultima_col = ultima_col          # REDONDO escribe A:J; TRIANGULO A:L
        self._lock = threading.Lock()

    def _asegurar_filas(self, ultima_fila: int):
        row_count = self.ws.row_count
        if ultima_fila > row_count:
            faltan = ultima_fila - row_count + 50   # margen para no ampliar en cada lote
            self.ws.add_rows(faltan)
            logging.info(f"  Hoja ampliada +{faltan} filas (ahora {self.ws.row_count})")

    def write_row(self, data: List[Any]):
        with self._lock:
            self.batch_buffer.append(data)
            if len(self.batch_buffer) >= self.batch_size:
                self._flush_locked()

    def flush_buffer(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if not self.batch_buffer:
            return
        rows = list(self.batch_buffer)
        first = self.current_row
        last = first + len(rows) - 1
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._asegurar_filas(last)
                self.ws.update(range_name=f"A{first}:{self.ultima_col}{last}", values=rows, value_input_option="USER_ENTERED")
                self.current_row = last + 1
                self.rows_written += len(rows)
                self.batch_buffer = []
                return
            except gspread.exceptions.APIError as e:
                if '429' in str(e) and attempt < max_retries - 1:
                    wait = (attempt + 1) * 30
                    logging.warning(f"  Sheets 429 al escribir - esperando {wait}s")
                    time.sleep(wait)
                else:
                    # Se quedan en el buffer; se reintentan en el flush final
                    logging.error(f" Error escribiendo lote de {len(rows)} filas: {e}")
                    return
            except Exception as e:
                logging.error(f" Error escribiendo lote de {len(rows)} filas: {e}")
                return

    def get_rows_written(self) -> int:
        return self.rows_written

@sheets_retry
def limpiar_resultados_seguro(ws_resultados, ultima_col: str = "J"):
    try:
        frozen = ws_resultados._properties.get("gridProperties", {}).get("frozenRowCount", 0) or 0
        start_row = max(2, frozen + 1)
        last_row = ws_resultados.row_count
        if last_row >= start_row:
            ws_resultados.batch_clear([f"A{start_row}:{ultima_col}{last_row}"])
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


def _es_directo(it: dict, legs: dict) -> bool:
    """True si TODOS los tramos del itinerario (ida y vuelta) tienen 0 escalas."""
    for leg_id in it.get("legIds", []) or []:
        if (legs.get(leg_id, {}) or {}).get("stopCount", 0):
            return False
    return True


def _extraer_precios_de_respuesta(resp_json: dict, solo_directos: Optional[bool] = None) -> dict:
    """solo_directos=None usa el valor global del .env; una hoja puede pasar el suyo (True/False)."""
    _sd = SOLO_DIRECTOS if solo_directos is None else solo_directos
    out = {'best': None, 'cheapest': None, 'fastest': None, 'status': None, 'directos': None, 'total_itins': None}
    try:
        content = resp_json.get("content", {})
        results = content.get("results", {})
        itins = results.get("itineraries", {}) or {}
        legs = results.get("legs", {}) or {}
        sorting = content.get("sortingOptions", {}) or {}
        # La API v3 devuelve "status" en la RAÍZ de la respuesta, no dentro de "content".
        # Antes se leía content.status (siempre None) y nunca se detectaba COMPLETE.
        out['status'] = resp_json.get("status") or content.get("status")
        out['total_itins'] = len(itins)

        # Filtro de escalas: igual que marcar solo "Directo" en la web
        if _sd and itins:
            directos = {k: v for k, v in itins.items() if _es_directo(v, legs)}
            out['directos'] = len(directos)
            if directos or not SIN_DIRECTO_USAR_ESCALAS:
                itins = directos

        def get_price(itin_id: str) -> Optional[int]:
            it = itins.get(itin_id)
            if not it: return None
            price_obj = it.get("price") or (it.get("pricingOptions", [{}])[0].get("price"))
            if not price_obj: return None
            mxn = _price_to_mxn(price_obj.get("amount", "0"), price_obj.get("unit", "PRICE_UNIT_MICRO"))
            return int(round(mxn)) if mxn > 0 else None

        # Se toma el PRIMER itinerario de cada orden que pase el filtro (best = primer directo del orden "best")
        for api_key, out_key in [("best", "best"), ("cheapest", "cheapest"), ("fastest", "fastest")]:
            for entry in (sorting.get(api_key, []) or []):
                price = get_price(entry.get("itineraryId"))
                if price:
                    out[out_key] = price
                    break
        
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
def buscar_precios_skyscanner(entity_orig, entity_dest, ida, vuelta, iata_orig, iata_dest,
                              legs: List[dict] = None, adults: int = None, cabin: str = None,
                              market: str = None, currency: str = None, solo_directos: bool = None,
                              aerolineas: List[str] = None) -> dict:
    """Búsqueda ida y vuelta (comportamiento de siempre) o multi-city si se pasa `legs`:
        legs = [{'entity': id, 'iata': 'GDL', 'dest_entity': id, 'dest_iata': 'BOS', 'fecha': 'YYYY-MM-DD'}, ...]
    Los demás parámetros opcionales permiten que una hoja use sus propios valores (personas,
    cabina, mercado, moneda, directos) sin tocar el .env. Si no se pasan, se usa el .env.
    `aerolineas` (ej. ['AM']) limita la búsqueda a esas aerolíneas (includedCarriersIds de la API)."""
    _sd = SOLO_DIRECTOS if solo_directos is None else solo_directos
    if legs is None:
        ok_i, _ = validar_fecha(ida)
        ok_v, _ = validar_fecha(vuelta)
        if not ok_i or not ok_v: return {'best': None, 'cheapest': None}
        if datetime.strptime(vuelta, "%Y-%m-%d") < datetime.strptime(ida, "%Y-%m-%d"):
            logging.warning(" Vuelta anterior a ida")
            return {'best': None, 'cheapest': None}
        legs = [dict(entity=entity_orig, iata=iata_orig, dest_entity=entity_dest, dest_iata=iata_dest, fecha=ida),
                dict(entity=entity_dest, iata=iata_dest, dest_entity=entity_orig, dest_iata=iata_orig, fecha=vuelta)]
        route_log = (f"{iata_orig}({entity_orig}) → {iata_dest}({entity_dest})" if USE_ENTITY_ID
                     else f"{iata_orig} → {iata_dest}")
    else:
        fechas = [l['fecha'] for l in legs]
        for f in fechas:
            ok, _ = validar_fecha(f)
            if not ok: return {'best': None, 'cheapest': None}
        if fechas != sorted(fechas):
            logging.warning(f" Fechas de tramos fuera de orden: {fechas}")
            return {'best': None, 'cheapest': None}
        ida, vuelta = fechas[0], fechas[-1]
        route_log = " | ".join(f"{l['iata']}→{l['dest_iata']} {l['fecha']}" for l in legs)

    def _place(entity, iata):
        return {"entityId": entity} if (USE_ENTITY_ID and entity) else {"iata": iata}
    query_legs = [{"originPlaceId": _place(l['entity'], l['iata']),
                   "destinationPlaceId": _place(l['dest_entity'], l['dest_iata']),
                   "date": {"year": int(l['fecha'][:4]), "month": int(l['fecha'][5:7]), "day": int(l['fecha'][8:10])}}
                  for l in legs]

    rate_limiter.wait_if_needed()
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}
    payload = {
        "query": {
            "market": market or MARKET, "locale": LOCALE, "currency": currency or CURRENCY,
            "queryLegs": query_legs,
            "adults": adults or ADULTOS, "cabinClass": cabin or CABIN,
            "childrenAges": [] if CHILDREN == 0 else [8] * CHILDREN
        }
    }
    if aerolineas:
        payload["query"]["includedCarriersIds"] = list(aerolineas)
    try:
        logging.info(f" Búsqueda: {route_log} | {ida} → {vuelta}" + (f" | solo {','.join(aerolineas)}" if aerolineas else ""))
        search_start = time.time()
        r = requests.post(URL_LIVE_CREATE, json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        initial = _extraer_precios_de_respuesta(j, _sd)
        token = j.get("sessionToken")
        if not token:
            logging.warning(" Sin sessionToken, retornando resultado inicial")
            metrics.record_search(0)
            return initial

        poll_count = 0
        status = initial.get('status')
        # cheapest y best: se toman del ÚLTIMO poll con contenido (estado final = COMPLETE),
        # que es lo que muestra la página. Antes se guardaba el MÍNIMO visto en cualquier poll:
        # best quedaba igual a cheapest en el 70% de los casos, y cheapest podía conservar
        # una tarifa parcial que ya no existía en el resultado final.
        cheapest_ever = initial.get('cheapest')
        best_ever = initial.get('best')
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
                new_result = _extraer_precios_de_respuesta(poll_r.json(), _sd)
                status = new_result.get('status')
                new_cheap = new_result.get('cheapest')
                new_best = new_result.get('best')
                if new_cheap is not None:
                    if new_cheap != cheapest_ever:
                        logging.info(f"    Poll {poll_count} cheapest: ${cheapest_ever or '?'} → ${new_cheap}")
                    cheapest_ever = new_cheap
                if new_best is not None:
                    if new_best != best_ever:
                        logging.info(f"    Poll {poll_count} best:     ${best_ever or '?'} → ${new_best}")
                    best_ever = new_best
            except requests.RequestException as e:
                logging.warning(f"    Error poll {poll_count}: {e}")
                continue

        metrics.record_search(poll_count)
        final_result = {'cheapest': cheapest_ever, 'best': best_ever, 'status': status}
        if _sd and cheapest_ever is None:
            logging.info(f"    FINAL: SIN VUELO DIRECTO para {route_log} {ida}→{vuelta} (fila omitida) | Polls={poll_count}")
        else:
            logging.info(f"    FINAL: Cheapest=${cheapest_ever} | Best=${best_ever} | Polls={poll_count}"
                         + (" | solo directos" if _sd else ""))
        return final_result
    except requests.RequestException as e:
        logging.error(f" Error búsqueda: {e}")
        metrics.record_call(False)
        return {'best': None, 'cheapest': None}


def _buscar_job(job: dict) -> dict:
    """Ejecuta una búsqueda a partir de un job (dict con los datos de la ruta).
    Los jobs REDONDO solo traen los 6 datos básicos; los de TRIANGULO agregan legs/adults/etc."""
    extra = {k: job[k] for k in ('legs', 'adults', 'cabin', 'market', 'currency', 'solo_directos', 'aerolineas') if k in job}
    try:
        return buscar_precios_skyscanner(job['entity_orig'], job['entity_dest'], job['ida'], job['vuelta'],
                                         job['iata_orig'], job['iata_dest'], **extra)
    except Exception as e:
        logging.error(f" Error búsqueda {job['iata_orig']}→{job['iata_dest']} {job['ida']}: {e}")
        return {'best': None, 'cheapest': None}


def buscar_en_paralelo(jobs: List[dict], max_workers: int = None):
    """Ejecuta las búsquedas con hasta PARALLEL_SEARCHES hilos y entrega
    (job, precios) EN EL MISMO ORDEN de entrada, conforme van terminando.
    Cada búsqueda sigue siendo la misma función de siempre; solo corren varias a la vez."""
    workers = max(1, max_workers or PARALLEL_SEARCHES)
    if workers == 1 or len(jobs) <= 1:
        for job in jobs:
            yield job, _buscar_job(job)
        return
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="busqueda") as ex:
        for job, precios in zip(jobs, ex.map(_buscar_job, jobs)):
            yield job, precios


def _fila_resultado(job: dict, cheapest: int, best: Optional[int], texto_precio: str = "") -> List[Any]:
    """texto_precio: si se pasa (ej. 'Sobre límite $4,999'), va en lugar de los precios."""
    return [job['iata_orig'], job.get('nombre_orig') or job['iata_orig'], job['entity_orig'],
            job['iata_dest'], job.get('nombre_dest') or job['iata_dest'], job['entity_dest'],
            job['ida'], job['vuelta'],
            texto_precio or f"${cheapest:,} MXN",
            texto_precio or (f"${best:,} MXN" if best else "")]



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
        if cfg.tipo in ('TRIANGULO', 'IDA'):
            return _procesar_hoja_triangulo(sm, version, cfg)
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

        # Ruta principal: todas las fechas, en paralelo, resultados en orden
        logging.info(f"\n Ruta principal ({len(pares)} fechas, {PARALLEL_SEARCHES} en paralelo)...")
        jobs = [dict(entity_orig=entity_orig, entity_dest=entity_dest, ida=ida, vuelta=vuelta,
                     iata_orig=iata_origen, iata_dest=iata_destino,
                     nombre_orig=nombre_orig, nombre_dest=nombre_dest,
                     aerolineas=cfg.aerolineas) for ida, vuelta in pares]
        for idx, (job, precios) in enumerate(buscar_en_paralelo(jobs), 1):
            cheapest = precios.get('cheapest')
            best = precios.get('best')
            logging.info(f"[{idx}/{len(jobs)}] {job['iata_orig']}→{job['iata_dest']} | {job['ida']} - {job['vuelta']} "
                         f"→ Cheapest=${cheapest or 0:,} | Best=${best or 0:,}")
            if cheapest and WRITE_IMMEDIATELY:
                writer.write_row(_fila_resultado(job, cheapest, best))

        # Orígenes extra: se resuelven las entidades en serie (usan caché) y
        # luego TODAS las combinaciones origen×fecha se buscan en paralelo
        # Si la hoja configura EXTRAS_LIMIT_COL (ej. E), cada origen extra trae su límite por persona
        # (misma zona que SOLO_EXTRAS: IATA en D, límite en E). Sin esa variable, se buscan sin límite.
        if cfg.extras_limit_col:
            limites = {e['iata']: e['limite'] for e in obtener_origenes_extras_con_limite(sm, cfg)}
            extras = filtrar_extras_unicos(list(limites), iata_origen)
        else:
            limites = {}
            extras = filtrar_extras_unicos(obtener_origenes_extras(sm, cfg), iata_origen)
        if extras:
            logging.info(f"\n {len(extras)} orígenes extra")
            jobs_extra = []
            for iata_extra in extras:
                entity_ex, nombre_ex = obtener_entity_info(iata_extra)
                if not entity_ex: continue
                for ida, vuelta in pares:
                    jobs_extra.append(dict(entity_orig=entity_ex, entity_dest=entity_dest, ida=ida, vuelta=vuelta,
                                           iata_orig=iata_extra, iata_dest=iata_destino,
                                           nombre_orig=nombre_ex, nombre_dest=nombre_dest,
                                           aerolineas=cfg.aerolineas, limite=limites.get(iata_extra)))
            sobre_limite = 0
            for idx, (job, precios) in enumerate(buscar_en_paralelo(jobs_extra), 1):
                cheapest = precios.get('cheapest')
                best = precios.get('best')
                limite = job.get('limite')
                logging.info(f"[{job['iata_orig']}] [{idx}/{len(jobs_extra)}] {job['ida']} - {job['vuelta']} "
                             f"→ Cheapest=${cheapest or 0:,} | Best=${best or 0:,}")
                if cheapest and limite and cheapest / ADULTOS > limite:
                    sobre_limite += 1
                    logging.info(f"    ${cheapest:,} (${cheapest / ADULTOS:,.0f} pp) > límite ${limite:,} → Sobre límite")
                    if WRITE_IMMEDIATELY:
                        writer.write_row(_fila_resultado(job, cheapest, best, texto_precio=f"Sobre límite ${limite:,}"))
                    continue
                if cheapest and WRITE_IMMEDIATELY:
                    writer.write_row(_fila_resultado(job, cheapest, best))
            if limites:
                logging.info(f" Orígenes extra: {sobre_limite} de {len(jobs_extra)} sobre el límite")

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

        # Entidades en serie (caché), luego todas las combinaciones origen×fecha en paralelo
        jobs = []
        for extra_info in extras:
            iata_extra = extra_info['iata']
            limite = extra_info['limite']
            entity_ex, nombre_ex = obtener_entity_info(iata_extra)
            if not entity_ex:
                logging.warning(f" No se obtuvo EntityID para {iata_extra}, saltando")
                continue
            limite_str = f"${limite:,}" if limite else "sin límite"
            logging.info(f" {iata_extra} → {iata_destino} | Límite: {limite_str} | {len(pares)} fechas")
            for ida, vuelta in pares:
                jobs.append(dict(entity_orig=entity_ex, entity_dest=entity_dest, ida=ida, vuelta=vuelta,
                                 iata_orig=iata_extra, iata_dest=iata_destino,
                                 nombre_orig=nombre_ex, nombre_dest=nombre_dest, limite=limite,
                                 aerolineas=cfg.aerolineas))

        logging.info(f"\n {len(jobs)} búsquedas, {PARALLEL_SEARCHES} en paralelo...")
        for idx, (job, precios) in enumerate(buscar_en_paralelo(jobs), 1):
            cheapest = precios.get('cheapest')
            best = precios.get('best')
            limite = job['limite']
            total_buscados += 1
            logging.info(f"  [{idx}/{len(jobs)}] {job['iata_orig']}→{job['iata_dest']} | {job['ida']} - {job['vuelta']} "
                         f"→ Cheapest=${cheapest or 0:,} | Best=${best or 0:,}")
            if cheapest:
                if limite and cheapest > limite:
                    total_filtrados += 1
                    logging.info(f"    ${cheapest:,} > límite ${limite:,} → omitido")
                    continue
                if WRITE_IMMEDIATELY:
                    writer.write_row(_fila_resultado(job, cheapest, best))

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
# MODO TRIÁNGULO / MULTI-CITY (hojas "VUE TRI ...")
# Lee ruta, personas, cabina, mercado, moneda, directo, límite y modo de la
# propia hoja; busca tramo1 (fecha IDA) + tramo2 (fecha VUELTA) como multi-city
# y escribe UN precio por fecha en la columna de precio, alineado por la fecha.
# ============================================================================

_CABINAS = {'ECONOMIA': 'CABIN_CLASS_ECONOMY', 'ECONOMÍA': 'CABIN_CLASS_ECONOMY', 'ECONOMY': 'CABIN_CLASS_ECONOMY',
            'PREMIUM': 'CABIN_CLASS_PREMIUM_ECONOMY', 'PREMIUM ECONOMY': 'CABIN_CLASS_PREMIUM_ECONOMY',
            'BUSINESS': 'CABIN_CLASS_BUSINESS', 'EJECUTIVA': 'CABIN_CLASS_BUSINESS',
            'PRIMERA': 'CABIN_CLASS_FIRST', 'FIRST': 'CABIN_CLASS_FIRST'}


def _a1_a_rc(ref: str):
    """'N10' -> (10, 14)"""
    m = re.match(r'^([A-Za-z]+)(\d+)$', ref.strip())
    if not m: raise ValueError(f"Referencia inválida: {ref}")
    col = 0
    for ch in m.group(1).upper(): col = col * 26 + (ord(ch) - 64)
    return int(m.group(2)), col


def _col_a_indice(col: str) -> int:
    n = 0
    for ch in col.strip().upper(): n = n * 26 + (ord(ch) - 64)
    return n


def _a_entero(v, default=None):
    try:
        s = str(v).replace(',', '').replace('$', '').replace(' ', '').strip()
        return int(float(s)) if s else default
    except (ValueError, TypeError):
        return default


def _procesar_hoja_triangulo(sm: SheetManager, version: str, cfg: SheetConfig) -> bool:
    try:
        ws = sm.get_worksheet(get_sheet_url(cfg), cfg.captura_sheet)
        valores = ws.get_all_values()

        def celda(ref: str, default: str = "") -> str:
            if not ref: return default
            r, c = _a1_a_rc(ref)
            fila = valores[r - 1] if r - 1 < len(valores) else []
            v = fila[c - 1] if c - 1 < len(fila) else ""
            return v.strip() if isinstance(v, str) else (v if v is not None else default)

        # --- Configuración leída de la hoja ---
        # TIPO=IDA: un solo tramo (E2→F2 en la fecha IDA). TIPO=TRIANGULO: dos tramos.
        es_ida = cfg.tipo == 'IDA'
        o1, d1 = sanitizar_entrada(celda(cfg.leg1_origen_cell)), sanitizar_entrada(celda(cfg.leg1_destino_cell))
        if es_ida:
            o2 = d2 = ''
            if not all(validar_iata_code(x) for x in (o1, d1)):
                logging.error(f" {version}: IATA inválido en tramo: {o1}→{d1}")
                apagar_switch(sm, cfg)
                return False
        else:
            o2, d2 = sanitizar_entrada(celda(cfg.leg2_origen_cell)), sanitizar_entrada(celda(cfg.leg2_destino_cell))
            if not all(validar_iata_code(x) for x in (o1, d1, o2, d2)):
                logging.error(f" {version}: IATA inválido en tramos: {o1}→{d1}, {o2}→{d2}")
                apagar_switch(sm, cfg)
                return False
        personas = _a_entero(celda(cfg.personas_cell), 1) or 1
        cabina = _CABINAS.get(celda(cfg.cabina_cell).upper(), CABIN) if cfg.cabina_cell else CABIN
        mercado = (celda(cfg.mercado_cell).upper() or MARKET) if cfg.mercado_cell else MARKET
        moneda = (celda(cfg.moneda_cell).upper() or CURRENCY) if cfg.moneda_cell else CURRENCY
        directo = (celda(cfg.directo_cell).upper() in ('TRUE', '1', 'SI', 'SÍ', 'ON')) if cfg.directo_cell else SOLO_DIRECTOS
        limite = _a_entero(celda(cfg.limite_cell)) if cfg.limite_cell else None
        modo = celda(cfg.modo_cell).upper() if cfg.modo_cell else 'MÁS BARATO'
        usar_best = 'RECOMEND' in modo
        if es_ida:
            logging.info(f" Modo IDA (sencillo): {o1}→{d1} en la fecha IDA")
        else:
            logging.info(f" Modo TRIÁNGULO: {o1}→{d1} (ida) + {o2}→{d2} (vuelta)")
        logging.info(f"   Personas={personas} | Cabina={cabina} | {mercado}/{moneda} | Directo={directo} | "
                     f"Límite pp={limite} | Escribe={'best (Recomendado)' if usar_best else 'cheapest (Más barato)'} "
                     f"en {cfg.precio_col}{cfg.precio_fila_inicio}:{cfg.precio_col}{cfg.precio_fila_fin}")

        # --- Entidades (caché) ---
        ent = {}
        for iata in dict.fromkeys(x for x in (o1, d1, o2, d2) if x):
            e, _ = obtener_entity_info(iata)
            if not e:
                logging.error(f" No se obtuvo EntityID para {iata}")
                apagar_switch(sm, cfg)
                return False
            ent[iata] = e

        if es_ida:
            # Solo la columna IDA (B, fila 12 en adelante); la columna VUELTA se ignora
            pares = []
            for fila in valores[11:]:
                ida = (fila[1] if len(fila) > 1 else "").strip()
                if re.match(r"\d{4}-\d{2}-\d{2}$", ida) and validar_fecha(ida)[0]:
                    pares.append((ida, ida))
            logging.info(f" {cfg.captura_sheet}: {len(pares)} fechas de ida")
        else:
            pares = leer_parametros_y_pares(sm, cfg)
        if not pares:
            logging.error(f" Sin fechas válidas")
            apagar_switch(sm, cfg)
            return False

        # --- Fila de precio por fecha (columna de fechas de la zona de resultados) ---
        ci = _col_a_indice(cfg.precio_fecha_col)
        fila_por_fecha = {}
        for r in range(cfg.precio_fila_inicio, cfg.precio_fila_fin + 1):
            fila = valores[r - 1] if r - 1 < len(valores) else []
            f = (fila[ci - 1] if ci - 1 < len(fila) else "").strip()
            if f: fila_por_fecha.setdefault(f, r)

        rango_precio = f"{cfg.precio_col}{cfg.precio_fila_inicio}:{cfg.precio_col}{cfg.precio_fila_fin}"
        ws.batch_clear([rango_precio])
        logging.info(f" Limpiado {rango_precio} | {len(pares)} fechas | {PARALLEL_SEARCHES} en paralelo")

        # --- Pestaña de resultados (opcional): una fila por fecha con cheapest, best, tramo 2 y estado ---
        writer = None
        if get_env(f'{version}_RESULTADO_SHEET'):
            ws_res = sm.get_worksheet(SHEET_DESTINO_URL, cfg.resultado_sheet)
            limpiar_resultados_seguro(ws_res, ultima_col="L")
            try:
                if not (ws_res.acell('K1').value or '').strip():
                    ws_res.update(range_name='K1:L1', values=[['Tramo 2', 'Estado']])
            except Exception as e:
                logging.warning(f" No se pudieron escribir encabezados K1:L1: {e}")
            writer = IncrementalWriter(ws_res, start_row=2, ultima_col="L")
            logging.info(f" Resultados también en '{cfg.resultado_sheet}' (A:L)")
        nombres = {i: obtener_entity_info(i)[1] for i in dict.fromkeys(x for x in (o1, d1, o2, d2) if x)}

        def tramos(ida, vuelta):
            t = [dict(entity=ent[o1], iata=o1, dest_entity=ent[d1], dest_iata=d1, fecha=ida)]
            if not es_ida:
                t.append(dict(entity=ent[o2], iata=o2, dest_entity=ent[d2], dest_iata=d2, fecha=vuelta))
            return t
        jobs = [dict(entity_orig=ent[o1], entity_dest=ent[d1], ida=ida, vuelta=vuelta, iata_orig=o1, iata_dest=d1,
                     legs=tramos(ida, vuelta),
                     adults=personas, cabin=cabina, market=mercado, currency=moneda, solo_directos=directo,
                     aerolineas=cfg.aerolineas)
                for ida, vuelta in pares]

        pendientes: List[tuple] = []
        escritas = omitidas_limite = sin_precio = sin_fila = 0

        def flush():
            nonlocal pendientes
            if not pendientes: return
            data = [{'range': c, 'values': [[v]]} for c, v in pendientes]
            for intento in range(3):
                try:
                    ws.batch_update(data, value_input_option='USER_ENTERED')
                    pendientes = []
                    return
                except gspread.exceptions.APIError as e:
                    if '429' in str(e) and intento < 2:
                        logging.warning(f"  Sheets 429 al escribir - esperando {(intento + 1) * 30}s")
                        time.sleep((intento + 1) * 30)
                    else:
                        logging.error(f" Error escribiendo {len(data)} precios: {e}")
                        return

        def fila_resultado(job, cheapest, best, estado):
            return [o1, nombres.get(o1) or o1, ent[o1], d1, nombres.get(d1) or d1, ent[d1],
                    job['ida'], "" if es_ida else job['vuelta'],
                    f"${cheapest:,} MXN" if cheapest else "", f"${best:,} MXN" if best else "",
                    "" if es_ida else f"{o2}→{d2}", estado]

        for idx, (job, precios) in enumerate(buscar_en_paralelo(jobs), 1):
            cheapest, best = precios.get('cheapest'), precios.get('best')
            precio = best if usar_best else cheapest
            etiqueta = (f"[{idx}/{len(jobs)}] {o1}→{d1} {job['ida']}" if es_ida
                        else f"[{idx}/{len(jobs)}] {o1}→{d1} {job['ida']} | {o2}→{d2} {job['vuelta']}")
            if not precio:
                sin_precio += 1
                logging.info(f"{etiqueta} → sin precio (celda vacía)")
                if writer: writer.write_row(fila_resultado(job, cheapest, best, "Sin precio"))
                continue
            por_persona = precio / personas
            if limite and por_persona > limite:
                omitidas_limite += 1
                logging.info(f"{etiqueta} → ${precio:,} (${por_persona:,.0f} pp) > límite ${limite:,} → celda vacía")
                if writer: writer.write_row(fila_resultado(job, cheapest, best, f"Sobre límite ${limite:,}"))
                continue
            fila = fila_por_fecha.get(job['ida'])
            if not fila:
                sin_fila += 1
                logging.warning(f"{etiqueta} → ${precio:,} pero la fecha {job['ida']} no está en {cfg.precio_fecha_col}")
                if writer: writer.write_row(fila_resultado(job, cheapest, best, "Fecha sin fila"))
                continue
            pendientes.append((f"{cfg.precio_col}{fila}", int(precio)))
            escritas += 1
            logging.info(f"{etiqueta} → ${precio:,} → {cfg.precio_col}{fila}")
            if writer: writer.write_row(fila_resultado(job, cheapest, best, "OK"))
            if len(pendientes) >= WRITE_BATCH_SIZE: flush()
        flush()
        if writer:
            writer.flush_buffer()
            logging.info(f" '{cfg.resultado_sheet}': {writer.get_rows_written()} filas escritas")

        logging.info(f"\n {version} {'IDA' if es_ida else 'TRIÁNGULO'} COMPLETADO: {escritas} precios escritos | "
                     f"{omitidas_limite} sobre el límite | {sin_precio} sin precio | {sin_fila} sin fila de fecha")
        if get_env(f'{version}_STATS_CELL'):
            actualizar_fecha(sm, cfg)
        apagar_switch(sm, cfg)
        return True
    except Exception as e:
        logging.error(f" Error en {version} (triángulo): {e}")
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
    logging.info(f"   Paralelo: {PARALLEL_SEARCHES} búsquedas | Lote escritura: {WRITE_BATCH_SIZE} filas")
    logging.info(f"   Solo directos: {SOLO_DIRECTOS} | Sin directo usar escalas: {SIN_DIRECTO_USAR_ESCALAS} | Rate limit: {MAX_CALLS_PER_MIN}/min")
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