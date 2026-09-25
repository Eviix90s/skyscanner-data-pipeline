# -*- coding: utf-8 -*-
"""Busca 3 rutas con la version NUEVA e imprime cheapest/best para comparar a mano con skyscanner.com.mx"""
import importlib.util, logging, os, time
from concurrent.futures import ThreadPoolExecutor
base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("bot", os.path.join(base, "apiskyscanner_api.py"))
bot = importlib.util.module_from_spec(spec); spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.WARNING)
casos = [("MEX","CCS","2026-10-15","2026-10-25"),("MEX","CUN","2026-11-04","2026-11-14"),("MEX","CUN","2026-11-24","2026-12-04")]
ent = {i: bot.obtener_entity_info(i)[0] for i in {"MEX","CCS","CUN"}}
def run(c):
    o,d,ida,vuelta = c; t=time.time()
    r = bot.buscar_precios_skyscanner(ent[o], ent[d], ida, vuelta, o, d)
    return c, r, time.time()-t
with ThreadPoolExecutor(3) as ex:
    for (o,d,ida,vuelta), r, t in ex.map(run, casos):
        print(f"{o}-{d} {ida} -> {vuelta} | cheapest={r.get('cheapest'):,} | best={r.get('best'):,} | status={r.get('status')} | {t:.1f}s")
print("hora:", time.strftime("%H:%M:%S"))
