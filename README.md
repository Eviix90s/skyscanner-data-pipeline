# ✈️ Skyscanner Data Pipeline

![Build & Push](https://github.com/Eviix90s/skyscanner-data-pipeline/actions/workflows/docker-build-push.yml/badge.svg)

Automatización de extracción de precios de vuelos en tiempo real mediante la API de Skyscanner v3, con escritura incremental a Google Sheets y despliegue distribuido en Docker con 4 contenedores trabajando en paralelo.

---

## Tabla de contenido

- [Descripción del proyecto](#descripción-del-proyecto)
- [Evolución de la arquitectura](#evolución-de-la-arquitectura)
- [Arquitectura final](#arquitectura-final)
- [Stack tecnológico](#stack-tecnológico)
- [Componentes clave](#componentes-clave)
- [Seguridad](#seguridad)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Despliegue](#despliegue)

---

## Descripción del proyecto

Este proyecto nació como una solución interna para automatizar la búsqueda y registro de precios de vuelos desde múltiples orígenes hacia distintos destinos. El sistema lee rutas configuradas en Google Sheets, consulta la API de Skyscanner v3 con polling inteligente, y escribe los resultados de vuelta a la hoja en tiempo real.

Lo que comenzó como un script local evolucionó hasta convertirse en una arquitectura distribuida de microservicios en Docker, con cuatro contenedores corriendo en paralelo, cada uno responsable de una hoja de trabajo diferente, sin interferencias entre sí y con recuperación automática ante fallos.

---

## Evolución de la arquitectura

El proyecto pasó por varias fases antes de llegar a la arquitectura actual. Cada etapa resolvió problemas reales encontrados en producción.

### Fase 1 — Desarrollo local

El primer objetivo fue hacer funcionar la integración con la API de Skyscanner v3 y con Google Sheets en entorno local. En esta etapa se definieron los flujos principales: leer rutas desde una hoja de origen, construir la búsqueda, hacer polling hasta obtener resultados completos, y escribir los precios en la hoja destino.

Se validaron los conceptos de **EntityID** (identificador preciso de aeropuertos y ciudades), el flujo de creación y polling de búsquedas live, y la autenticación con Google mediante cuenta de servicio.

---

### Fase 2 — Servidor local con Task Scheduler (Windows)

Con el script funcionando correctamente, se desplegó en una laptop encendida 24/7 usando **Windows Task Scheduler** para ejecutarlo cada 5 minutos. Esta fase permitió validar el comportamiento del sistema en condiciones reales de producción continua: errores de red, expiración de tokens, reintentos, y rate limits.

Se identificaron los primeros cuellos de botella y se añadieron mecanismos de manejo de errores más robustos.

---

### Fase 3 — Primer despliegue en Docker (ejecución secuencial)

Se containerizó el script y se configuró un pipeline CI/CD básico. En esta versión, el programa procesaba las hojas de forma **secuencial**: verificaba el switch ON/OFF de cada hoja, ejecutaba la búsqueda completa, y volvía al inicio del ciclo cada 5 minutos.

El problema de esta arquitectura era el **cuello de botella temporal**: si una hoja tardaba en completar su búsqueda, todas las demás esperaban. Con múltiples hojas activas simultáneamente, el tiempo de ciclo crecía de forma lineal.

---

### Fase 4 — Análisis con Postman y optimización de polling

Para entender los límites reales de la API, se realizaron pruebas con **Postman** directamente sobre los endpoints de Skyscanner v3. Los hallazgos fueron importantes:

- La API soportó más de **500 polls consecutivos con 0 segundos de delay** sin errores, lo que demostró que el límite real era mucho más permisivo de lo documentado.
- Se identificó que el verdadero cuello de botella no era Skyscanner sino la **API de Google Sheets**, que tiene límites estrictos de escritura por minuto por cuenta de servicio.
- Se realizó un cálculo preciso de la frecuencia de escritura para asegurar que, con múltiples contenedores escribiendo simultáneamente, nunca se superara el límite de Google Sheets (error 429).

Esto llevó a implementar un sistema de **retry inteligente con backoff** para los errores 429 de Google Sheets, y a ajustar el `PAUSE_BETWEEN_SHEETS` para distribuir la carga de escritura en el tiempo.

---

### Fase 5 — Arquitectura paralela con 4 contenedores (versión actual)

Con el análisis de la fase anterior, se rediseñó la arquitectura para eliminar el cuello de botella secuencial. La solución fue separar cada hoja en su **propio contenedor independiente**, todos corriendo simultáneamente con la misma imagen Docker pero con configuración diferente vía variable de entorno.

Cada contenedor:
- Tiene su propio directorio de datos (`/app/data`) — sin conflictos de lockfile ni caché
- Tiene sus propios logs (`/app/logs`) — trazabilidad independiente por hoja
- Se reinicia automáticamente si falla (`restart: always`)
- Se diferencia del resto únicamente por `PRIORIDAD_PROCESO`

El resultado es un sistema donde las 4 hojas se actualizan en paralelo, el tiempo de ciclo es constante independientemente de cuántas hojas estén activas, y un fallo en un contenedor no afecta a los demás.

---

### Fase 6 — CI/CD automatizado (en curso)

La siguiente etapa es configurar un pipeline de CI/CD completo en GitHub Actions para automatizar el build, push a Docker Hub y despliegue al servidor en cada push a `main`.

---

## Arquitectura final

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Host                          │
│                                                         │
│  ┌───────────────┐  ┌───────────────┐                  │
│  │  bot-v1       │  │  bot-v2       │                  │
│  │  PRIORIDAD=V1 │  │  PRIORIDAD=V2 │                  │
│  │  /data/v1     │  │  /data/v2     │                  │
│  │  /logs/v1     │  │  /logs/v2     │                  │
│  └───────┬───────┘  └───────┬───────┘                  │
│          │                  │                           │
│  ┌───────┴───────┐  ┌───────┴───────┐                  │
│  │  bot-v3       │  │  bot-puebla   │                  │
│  │  PRIORIDAD=V3 │  │  PRIORIDAD=   │                  │
│  │  /data/v3     │  │  PUEBLA       │                  │
│  │  /logs/v3     │  │  /data/puebla │                  │
│  └───────────────┘  └───────────────┘                  │
│                                                         │
│  credentials/ (read-only, compartido)                  │
└─────────────────────────────────────────────────────────┘
          │                        │
          ▼                        ▼
  Skyscanner API v3        Google Sheets API
  (búsqueda de vuelos)     (lectura y escritura)
```

Los 4 contenedores usan la **misma imagen** (`alexn90s/skyscanner-bot:latest`) y se diferencian únicamente por la variable de entorno `PRIORIDAD_PROCESO`. Las credenciales se montan como volumen **read-only** compartido. Los datos y logs son completamente aislados por contenedor.

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.11 |
| Containerización | Docker + Docker Compose |
| API de vuelos | Skyscanner Partners API v3 |
| Hoja de cálculo | Google Sheets API v4 (via gspread) |
| Autenticación Google | OAuth2 / Service Account |
| Configuración | Variables de entorno (.env) |
| Logging | RotatingFileHandler (10MB, 5 backups) |
| CI/CD | GitHub Actions (en configuración) |

---

## Componentes clave

### SheetManager — Caché de conexiones a Google Sheets

Uno de los problemas encontrados en producción fue el alto número de llamadas `GetSpreadsheet` que hacía `gspread` al reconectar en cada ciclo. Se implementó un `SheetManager` que cachea las conexiones a spreadsheets y worksheets en memoria, reduciendo las llamadas a la API de Google Sheets en aproximadamente un **99%** entre ciclos del mismo contenedor.

### RateLimiter — Control de llamadas a Skyscanner

Implementación de sliding window para controlar la frecuencia de llamadas a la API de Skyscanner. Configurable via `.env` (`SS_MAX_CALLS_PER_MIN`, `SS_RATE_LIMIT_WINDOW`). Evita errores 429 y respeta los límites del tier de la API.

### Polling inteligente

La API de Skyscanner v3 devuelve resultados parciales en el primer response y requiere polling hasta que el status sea `RESULT_STATUS_COMPLETE`. El sistema implementa:
- Mínimo de polls garantizados (`SS_MIN_GUARANTEED_POLLS`) para evitar cortes prematuros
- Deadline configurable en segundos (`SS_POLL_DEADLINE_SECONDS`)
- Espera entre polls ajustable (`SS_POLL_SLEEP_SECONDS`)
- Opción de esperar el estado `COMPLETE` o cortar antes con los mejores precios disponibles

### EntityID Cache

Los EntityIDs de aeropuertos y ciudades se resuelven una vez y se persisten en disco (`entity_cache.json`). Las búsquedas posteriores usan el cache local, eliminando llamadas al endpoint de autosuggest de Skyscanner.

### APIMetrics

Sistema de métricas en memoria que registra por ciclo: total de llamadas, tasa de éxito, cache hits, tiempo de ejecución y promedio de rounds de polling. Los datos se escriben de vuelta a la hoja de Google Sheets en una celda de estadísticas.

### Soporte PUEBLA B

Modo especial para una hoja con lógica diferente: en lugar de buscar por rutas completas, procesa únicamente filas "extras" que tengan checkbox activo y precio dentro de un límite definido por columna. Demuestra la extensibilidad de la configuración por contenedor.

---

## Seguridad

### Qué entra al repositorio

```
apiskyscanner_api.py     ✅ Código fuente
docker-compose.yml       ✅ Orquestación
Dockerfile               ✅ Build instructions
requirements.txt         ✅ Dependencias
.gitignore               ✅ Reglas de exclusión
```

### Qué nunca entra al repositorio ni a la imagen Docker

```
.env                     ❌ API keys, URLs de hojas, configuración
credentials/             ❌ service-account.json (Google)
data/                    ❌ Caché y lockfiles generados en runtime
logs/                    ❌ Logs de producción
```

El `.dockerignore` excluye los mismos archivos sensibles del contexto de build. Las credenciales llegan al contenedor únicamente como **volumen montado en el servidor**, nunca embebidas en la imagen.

---

## Estructura del repositorio

```
skyscanner-data-pipeline/
├── apiskyscanner_api.py       ← Script principal
├── docker-compose.yml         ← Orquestación de los 4 contenedores
├── Dockerfile                 ← Build de la imagen
├── requirements.txt           ← Dependencias Python
├── .gitignore                 ← Excluye secretos y archivos runtime
├── dockerignore               ← Excluye secretos del build de Docker
└── README.md
```

En el servidor de producción (fuera del repositorio):

```
~/skyscanner-bot/
├── .env                       ← Configuración privada
├── docker-compose.yml
├── credentials/
│   └── service-account.json   ← Credenciales Google (privado)
├── data/
│   ├── v1/                    ← Caché del contenedor V1
│   ├── v2/
│   ├── v3/
│   └── puebla/
└── logs/
    ├── v1/
    ├── v2/
    ├── v3/
    └── puebla/
```

---

## Despliegue

### Requisitos previos

- Docker y Docker Compose instalados en el servidor
- Archivo `.env` con las variables de configuración
- `credentials/service-account.json` con acceso a las hojas de Google Sheets
- Cuenta en Docker Hub con la imagen publicada

### Levantar todos los contenedores

```bash
docker compose pull
docker compose up -d
```

### Ver logs en tiempo real

```bash
# Ver todos los contenedores
docker compose logs -f

# Ver solo uno
docker compose logs -f bot-v1
```

### Detener

```bash
docker compose down
```

### Actualizar a nueva versión

Desde el equipo de desarrollo:

```bash
docker build -t alexn90s/skyscanner-bot:latest .
docker push alexn90s/skyscanner-bot:latest
```

En el servidor:

```bash
docker compose pull
docker compose up -d
```
