# -*- coding: utf-8 -*-
"""Lee la hoja Resultados de V1 (lo que escribio el bot en la ultima corrida) y vuelve a buscar
cada fila con la version actual del bot para ver si los valores son estables o si algo se desvia."""
import importlib.util, logging, os, re, time
from concurrent.futures import ThreadPoolExecutor
base=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec=importlib.util.spec_from_file_location("bot", os.path.join(base,"apiskyscanner_api.py"))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.WARNING)
client=bot.conectar_sheets(); sm=bot.SheetManager(client)
cfg=bot.SHEET_CONFIGS["V1"]
ws=sm.get_worksheet(bot.SHEET_DESTINO_URL, cfg.resultado_sheet)
filas=[r for r in ws.get_all_values()[1:] if len(r)>=9 and r[0] and r[6]]
print(f"Hoja '{cfg.resultado_sheet}': {len(filas)} filas escritas por el bot\n")
def num(s):
    m=re.search(r'[\d,]+', s or ''); return int(m.group().replace(',','')) if m else None
def run(r):
    o,eo,d,ed,ida,vuelta=r[0],r[2],r[3],r[5],r[6],r[7]
    t=time.time(); res=bot.buscar_precios_skyscanner(eo,ed,ida,vuelta,o,d)
    return r,res,time.time()-t
print(f"{'Ruta':<9}{'Ida':<12}{'Vuelta':<12}{'Hoja cheap':>11}{'API cheap':>10}{'Dif':>7}{'Hoja best':>10}{'API best':>9}{'Dif':>7}{'polls':>6}")
print('-'*94)
with ThreadPoolExecutor(3) as ex:
    for r,res,t in ex.map(run, filas):
        hc,hb=num(r[8]),num(r[9] if len(r)>9 else ''); ac,ab=res.get('cheapest'),res.get('best')
        dc=(ac-hc) if (ac and hc) else None; db=(ab-hb) if (ab and hb) else None
        print(f"{r[0]}-{r[3]:<5}{r[6]:<12}{r[7]:<12}{hc or 0:>11,}{ac or 0:>10,}{(dc or 0):>+7,}{hb or 0:>10,}{ab or 0:>9,}{(db or 0):>+7,}{bot.metrics.avg_poll_rounds[-1] if bot.metrics.avg_poll_rounds else 0:>6}")
