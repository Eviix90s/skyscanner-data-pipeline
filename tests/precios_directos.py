# -*- coding: utf-8 -*-
"""Busca rutas con la version actual e imprime cheapest/best. Respeta SS_SOLO_DIRECTOS del entorno."""
import importlib.util, logging, os, time
from concurrent.futures import ThreadPoolExecutor
base=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec=importlib.util.spec_from_file_location("bot", os.path.join(base,"apiskyscanner_api.py"))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot)
logging.getLogger().setLevel(logging.WARNING)
casos=[("PBC","IAH","2027-01-29","2027-02-03"),("PBC","IAH","2026-11-04","2026-11-09"),("QRO","IAH","2026-11-11","2026-11-16"),("MEX","CUN","2026-11-24","2026-12-04")]
ent={i: bot.obtener_entity_info(i)[0] for i in {"PBC","QRO","IAH","MEX","CUN"}}
def run(c):
    o,d,ida,vuelta=c; t=time.time(); r=bot.buscar_precios_skyscanner(ent[o],ent[d],ida,vuelta,o,d); return c,r,time.time()-t
print(f"SS_SOLO_DIRECTOS={bot.SOLO_DIRECTOS}")
with ThreadPoolExecutor(4) as ex:
    for (o,d,ida,vuelta),r,t in ex.map(run,casos):
        c,b=r.get('cheapest'),r.get('best')
        print(f"  {o}-{d} {ida} -> {vuelta} | cheapest={c if c is None else f'{c:,}'} | best={b if b is None else f'{b:,}'} | {t:.1f}s")
