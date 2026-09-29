# Skyscanner VALV Bot

Bot que lee hojas de Google Sheets, consulta la API de Skyscanner y escribe resultados.
Corre como varios contenedores (uno por hoja), cada uno con su propia service account.

## Estructura

| Archivo | Que es |
|---|---|
| `apiskyscanner_api.py` | Todo el codigo del bot |
| `requirements.txt` | Dependencias Python |
| `Dockerfile` | Construye la imagen `alexn90s/skyscanner-bot` |
| `docker-compose.yml` | Produccion: 7 bots + Watchtower (usa la imagen de Docker Hub) |
| `docker-compose.dev.yml` | Desarrollo: 1 bot construido desde el codigo local |
| `.env` | Configuracion y API keys (NO se sube a Git) |
| `.env.example` | Plantilla del .env sin valores |
| `credentials/` | Service accounts de Google (NO se sube a Git) |

## Flujo de trabajo

1. Editar `apiskyscanner_api.py`.
2. Probar localmente con un solo bot:
   ```
   docker compose -f docker-compose.dev.yml up --build
   ```
   (Ctrl+C para detener. Mientras pruebas, apaga el bot de produccion de esa misma hoja
   para que no se pisen: `docker compose -f C:\Skyscanner-valv-bot\docker-compose.yml stop bot-v1`)
3. Cuando funcione, construir y publicar la imagen:
   ```
   docker build -t alexn90s/skyscanner-bot:latest .
   docker push alexn90s/skyscanner-bot:latest
   ```
4. Watchtower en produccion detecta la nueva imagen en 5 min y reinicia los bots.
   Para no esperar: `docker compose -f C:\Skyscanner-valv-bot\docker-compose.yml pull; docker compose -f C:\Skyscanner-valv-bot\docker-compose.yml up -d`

## Variables nuevas en `.env` (v3.3)

| Variable | Valor | Que hace |
|---|---|---|
| `SS_PARALLEL_SEARCHES` | 3 | Busquedas Skyscanner simultaneas por bot. Subir de a 1 vigilando 429 |
| `SS_WRITE_BATCH_SIZE` | 5 | Filas por request de escritura a Sheets (1 = fila por fila como antes) |
| `LOOP_INTERVAL_SECONDS` | 30 | Cada cuanto se revisa el switch ON (antes 300) |
| `SHEETS_CHECK_DELAY` | 0 | Pausa entre switches (un solo switch por contenedor, no aplica) |
| `PAUSE_BETWEEN_SHEETS` | 0 | Pausa entre hojas (una sola hoja por contenedor, no aplica) |

## Pruebas

```
# Comparar precios v3.2 (original) vs v3.3 (nueva), solo API Skyscanner, sin tocar Sheets
docker run --rm --dns 8.8.8.8 --env-file .env -e SS_ENTITY_CACHE=//work/data/entity_cache.json -e SS_LOG_FILE=/tmp/t.log -v "C:/Skyscanner-valv-dev://work" -w //work --entrypoint python alexn90s/skyscanner-bot:dev tests/comparar_precios.py

# Probar escritura por lotes y ampliacion de filas (crea y borra una pestana temporal en Resultados)
docker run --rm --dns 8.8.8.8 --env-file .env -e GOOGLE_KEYFILE=//work/credentials/service-account.json -e SS_LOG_FILE=/tmp/t.log -v "C:/Skyscanner-valv-dev://work" -w //work --entrypoint python alexn90s/skyscanner-bot:dev tests/test_writer_sheets.py
```

Ultimo resultado (2026-09-25): 6/6 busquedas con 0 MXN de diferencia, 7.1x mas rapido, 6 polls promedio vs 13.7.

Comparacion contra skyscanner.com.mx (misma hora, 1 adulto, economy, MXN), tolerancia 500 MXN:

| Ruta | Fechas | Bot cheapest | Web cheapest | Bot best | Web best |
|---|---|---|---|---|---|
| MEX-CCS | 15 oct - 25 oct | 13,102 | 13,456 | 23,428 | 23,348 |
| MEX-CUN | 4 nov - 14 nov | 3,011 | 3,000 | 3,011 | 3,121 |
| MEX-CUN | 24 nov - 4 dic | 2,758 | 2,755 | 2,821 | 2,755 |

Diferencia maxima: 354 MXN (cheapest), 110 MXN (best). Cheapest y best se toman del estado final (COMPLETE), igual que la pagina. Con el minimo entre polls (logica anterior) la tercera ruta daba cheapest=2,562 y best=2,562.

## Produccion en esta maquina

La instalacion que corre 24/7 esta en `C:\Skyscanner-valv-bot`. Esta carpeta (`C:\Skyscanner-valv-dev`)
es solo para desarrollo. Si cambias el `.env` o el `docker-compose.yml`, copialos tambien a produccion.

## v3.5: modo TRIANGULO (multi-city)

Una hoja con `<PREFIJO>_TIPO=TRIANGULO` se procesa con `_procesar_hoja_triangulo`, que lee TODO de la propia hoja
(ruta de dos tramos, personas, cabina, mercado, moneda, preferir directo, limite por persona y modo
"Mas barato"/"Recomendado") y escribe UN precio por fecha en la columna de precio, alineado por la columna de fechas.
Si el precio por persona supera el limite, la celda queda vacia. REDONDO y SOLO_EXTRAS no cambian.

Tramos: `LEG1_ORIGEN->LEG1_DESTINO` en la fecha IDA y `LEG2_ORIGEN->LEG2_DESTINO` en la fecha VUELTA (queryLegs de 2 tramos).
La busqueda acepta `legs=[...]` opcional (hasta 6 tramos segun la API); sin `legs` arma ida y vuelta como siempre.

Variables por hoja (ver bloque `TRIJAZ_*` en `.env.example`): `TIPO, ORIGEN_URL, CAPTURA_SHEET, SWITCH_CELL,
OFF_SWITCH_CELL, LEG1/LEG2_ORIGEN/DESTINO_CELL, PERSONAS_CELL, CABINA_CELL, MERCADO_CELL, MONEDA_CELL, DIRECTO_CELL,
LIMITE_CELL, MODO_CELL, PRECIO_COL, PRECIO_FECHA_COL, PRECIO_FILA_INICIO, PRECIO_FILA_FIN`.

Prueba: `tests/test_triangulo.py` duplica la pestaña, corre el bot sobre la copia, muestra N/P y borra la copia.

## v3.6: modo IDA (sencillo)

`<PREFIJO>_TIPO=IDA` usa la misma funcion que TRIANGULO con UN solo tramo (`LEG1_ORIGEN_CELL`->`LEG1_DESTINO_CELL`
en la fecha IDA de la columna B). La columna VUELTA se ignora. Todo lo demas es igual: personas, cabina, mercado,
moneda, preferir directo, limite por persona, modo Mas barato/Recomendado, precio en N alineado por O, switches.
Probar con `tests/test_triangulo.py` y `TEST_HOJA=VUEIDA`. Contenedor `bot-vueida` en el compose.
