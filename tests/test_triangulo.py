# -*- coding: utf-8 -*-
"""Prueba el modo TRIANGULO contra una COPIA temporal de la pestaña (se borra al final).
Requiere PRIORIDAD_PROCESO=TRIJAZ y GOOGLE_KEYFILE=credentials_multicity.json."""
import importlib.util, logging, os, sys, time
base=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec=importlib.util.spec_from_file_location("bot", os.path.join(base,"apiskyscanner_api.py"))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.INFO)
for h in logging.getLogger().handlers: h.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%H:%M:%S'))
client=bot.conectar_sheets(); sm=bot.SheetManager(client)
cfg=bot.SHEET_CONFIGS['TRIJAZ']
ss=client.open_by_url(bot.get_sheet_url(cfg))
orig=ss.worksheet(cfg.captura_sheet)
nombre="TEST-TRI (borrar)"
for w in ss.worksheets():
    if w.title==nombre: ss.del_worksheet(w)
copia=orig.duplicate(new_sheet_name=nombre, insert_sheet_index=len(ss.worksheets()))
print("Copia creada:", nombre)
try:
    copia.update_acell(cfg.switch_cell, "ON")
    cfg.captura_sheet=nombre           # el bot trabaja SOLO sobre la copia
    t=time.time()
    ok=bot.procesar_hoja(sm, 'TRIJAZ', cfg)
    print(f"\nprocesar_hoja -> {ok} en {time.time()-t:.0f}s")
    time.sleep(2)
    print(f"\nSwitch {cfg.switch_cell}={copia.acell(cfg.switch_cell).value} | {cfg.off_switch_cell}={copia.acell(cfg.off_switch_cell).value}")
    filas=copia.get('L11:P30', value_render_option='FORMATTED_VALUE')
    print(f"\n{'#':>3} {'Fecha (O)':<12} {'Precio N':>10} {'por Persona P':>14}")
    for r in filas:
        r=r+['']*(5-len(r))
        if r[3]: print(f"{r[0]:>3} {r[3]:<12} {r[2]:>10} {r[4]:>14}")
    print("\nFecha más barata (F23/G23):", copia.acell('F23').value, copia.acell('G23').value)
finally:
    ss.del_worksheet(copia); print("\nCopia eliminada")
