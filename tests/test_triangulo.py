# -*- coding: utf-8 -*-
"""Prueba el modo TRIANGULO / IDA contra COPIAS temporales de la pestaña de captura y,
si la hoja tiene pestaña de resultados configurada, de esa pestaña. Las copias se borran al final.
No toca las pestañas reales.

Uso (en Docker):  -e PRIORIDAD_PROCESO=TRIJAZ -e TEST_HOJA=TRIJAZ  (o VUEIDA)
Requiere GOOGLE_KEYFILE=credentials_multicity.json."""
import importlib.util, logging, os, time

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("bot", os.path.join(base, "apiskyscanner_api.py"))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.INFO)
for h in logging.getLogger().handlers:
    h.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%H:%M:%S'))

HOJA = os.getenv('TEST_HOJA', 'TRIJAZ')
client = bot.conectar_sheets()
sm = bot.SheetManager(client)
cfg = bot.SHEET_CONFIGS[HOJA]
print(f"Hoja {HOJA} | tipo {cfg.tipo} | captura '{cfg.captura_sheet}' | switch {cfg.switch_cell}/{cfg.off_switch_cell}")

# Copia temporal de la pestaña de captura
ss = client.open_by_url(bot.get_sheet_url(cfg))
orig = ss.worksheet(cfg.captura_sheet)
nombre = f"TEST-{HOJA} (borrar)"
for w in ss.worksheets():
    if w.title == nombre:
        ss.del_worksheet(w)
copia = orig.duplicate(new_sheet_name=nombre, insert_sheet_index=len(ss.worksheets()))
print("Copia creada:", nombre)

# Copia temporal de la pestaña de resultados (solo si la hoja la tiene configurada)
dst = client.open_by_url(bot.SHEET_DESTINO_URL)
nombre_res = f"TEST-{HOJA}-RES (borrar)"
copia_res = None
if os.getenv(f'{HOJA}_RESULTADO_SHEET'):
    for w in dst.worksheets():
        if w.title == nombre_res:
            dst.del_worksheet(w)
    copia_res = dst.worksheet(cfg.resultado_sheet).duplicate(new_sheet_name=nombre_res, insert_sheet_index=len(dst.worksheets()))
    print("Copia de resultados creada:", nombre_res)

try:
    copia.update_acell(cfg.switch_cell, "ON")
    cfg.captura_sheet = nombre          # el bot trabaja SOLO sobre las copias
    if copia_res:
        cfg.resultado_sheet = nombre_res
    t = time.time()
    ok = bot.procesar_hoja(sm, HOJA, cfg)
    print(f"\nprocesar_hoja -> {ok} en {time.time() - t:.0f}s")
    time.sleep(2)
    print(f"\nSwitch {cfg.switch_cell}={copia.acell(cfg.switch_cell).value} | {cfg.off_switch_cell}={copia.acell(cfg.off_switch_cell).value}")
    filas = copia.get('L11:P60', value_render_option='FORMATTED_VALUE')
    print(f"\n{'#':>3} {'Fecha (O)':<12} {'Precio N':>10} {'por Persona P':>14}")
    for r in filas:
        r = r + [''] * (5 - len(r))
        if r[3]:
            print(f"{r[0]:>3} {r[3]:<12} {r[2]:>10} {r[4]:>14}")
    print("\nFecha más barata (F23/G23):", copia.acell('F23').value, copia.acell('G23').value)
    if copia_res:
        print("\n=== Pestaña de resultados (copia), primeras filas A:L ===")
        for r in copia_res.get('A1:L6'):
            print("  ", r)
        print("   total filas con datos:", len([r for r in copia_res.get('A2:A200') if r and r[0]]))
finally:
    ss.del_worksheet(copia)
    if copia_res:
        dst.del_worksheet(copia_res)
    print("\nCopias eliminadas")
