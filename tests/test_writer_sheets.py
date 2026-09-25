# -*- coding: utf-8 -*-
"""
Prueba la escritura por lotes y la ampliacion automatica de filas contra una
hoja TEMPORAL creada por la service account (no toca las hojas reales).
Uso (dentro del contenedor): python tests/test_writer_sheets.py
"""
import importlib.util, logging, os, sys
base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("bot", os.path.join(base, "apiskyscanner_api.py"))
bot = importlib.util.module_from_spec(spec); spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.INFO)

client = bot.conectar_sheets()
# Las service accounts ya no tienen Drive propio: se usa una PESTANA temporal
# en el spreadsheet de Resultados y se borra al final. No toca otras pestanas.
ss = client.open_by_url(bot.SHEET_DESTINO_URL)
ws = ss.add_worksheet(title="TEST-writer (borrar)", rows=5, cols=10)   # 5 filas a proposito: 12 NO caben
try:
    print(f"Pestana temporal creada con {ws.row_count} filas")
    writer = bot.IncrementalWriter(ws, start_row=2, batch_size=5)
    filas = [[f"O{i}", f"Origen {i}", f"E{i}", "CUN", "Cancun", "E999", "2026-11-01", "2026-11-08", f"${1000+i:,} MXN", f"${1100+i:,} MXN"] for i in range(1, 13)]
    for f in filas:
        writer.write_row(f)
    writer.flush_buffer()
    leidas = ws.get_all_values()[1:13]
    ok_contenido = [r[:10] for r in leidas] == filas
    ok_filas = ws.row_count >= 13
    print(f"Filas escritas: {writer.get_rows_written()} | row_count final: {ws.row_count} | contenido igual: {ok_contenido} | hoja ampliada: {ok_filas}")
    print("RESULTADO:", "OK" if (ok_contenido and ok_filas and writer.get_rows_written() == 12) else "FALLO")
    sys.exit(0 if (ok_contenido and ok_filas) else 1)
finally:
    ss.del_worksheet(ws)
    print("Pestana temporal eliminada")
