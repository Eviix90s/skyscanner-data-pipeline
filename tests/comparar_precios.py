# -*- coding: utf-8 -*-
"""
Compara la version ORIGINAL (v3.2) contra la NUEVA (v3.3) buscando las mismas rutas/fechas.
No toca Google Sheets: solo API Skyscanner.
Reporta: precio por version, diferencia, tiempo por busqueda, polls, y tiempo total.
Criterio: diferencia de cheapest <= 500 MXN.
Uso (dentro del contenedor): python tests/comparar_precios.py
"""
import importlib.util, logging, os, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

TOLERANCIA = int(os.getenv("TOLERANCIA_MXN", "500"))
PARALELO_NUEVO = int(os.getenv("SS_PARALLEL_SEARCHES", "3"))

def cargar(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod

base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
old = cargar("bot_old", os.path.join(base, "tests", "bot_v32_original.py"))
new = cargar("bot_new", os.path.join(base, "apiskyscanner_api.py"))
logging.getLogger().setLevel(logging.WARNING)   # silenciar los logs de los bots

# Rutas de prueba: la ruta real de V1 (MEX-CCS) y una nacional (MEX-CUN)
hoy = date.today()
def par(d_ida, dur=10):
    ida = hoy + timedelta(days=d_ida); return (ida.isoformat(), (ida + timedelta(days=dur)).isoformat())
casos = []
for iata_o, iata_d in [("MEX", "CCS"), ("MEX", "CUN")]:
    e_o, n_o = new.obtener_entity_info(iata_o); e_d, n_d = new.obtener_entity_info(iata_d)
    for d in (20, 40, 60):
        ida, vuelta = par(d)
        casos.append(dict(entity_orig=e_o, entity_dest=e_d, ida=ida, vuelta=vuelta, iata_orig=iata_o, iata_dest=iata_d))

def correr(mod, job):
    t = time.time()
    r = mod.buscar_precios_skyscanner(job['entity_orig'], job['entity_dest'], job['ida'], job['vuelta'], job['iata_orig'], job['iata_dest'])
    return r, time.time() - t

print(f"\n{len(casos)} busquedas por version | tolerancia {TOLERANCIA} MXN\n")

# 1) ORIGINAL v3.2: secuencial (como corre hoy en produccion)
old.metrics.reset(); t0 = time.time()
res_old = [correr(old, j) for j in casos]
t_old = time.time() - t0; polls_old = list(old.metrics.avg_poll_rounds)

# 2) NUEVA v3.3: en paralelo
new.metrics.reset(); t0 = time.time()
with ThreadPoolExecutor(max_workers=PARALELO_NUEVO) as ex:
    res_new = list(ex.map(lambda j: correr(new, j), casos))
t_new = time.time() - t0; polls_new = list(new.metrics.avg_poll_rounds)

print(f"{'Ruta':<9}{'Ida':<12}{'Vuelta':<12}{'v3.2 cheap':>11}{'v3.3 cheap':>11}{'Dif':>7}{'v3.2 best':>10}{'v3.3 best':>10}{'t v3.2':>8}{'t v3.3':>8}  OK")
print("-" * 106)
fallos = 0; difs = []
for j, (ro, to), (rn, tn) in zip(casos, res_old, res_new):
    co, cn = ro.get('cheapest'), rn.get('cheapest'); bo, bn = ro.get('best'), rn.get('best')
    dif = (cn - co) if (co and cn) else None
    ok = dif is not None and abs(dif) <= TOLERANCIA
    if dif is not None: difs.append(abs(dif))
    if not ok: fallos += 1
    print(f"{j['iata_orig']}-{j['iata_dest']:<5}{j['ida']:<12}{j['vuelta']:<12}{co or 0:>11,}{cn or 0:>11,}{(dif if dif is not None else 0):>+7,}{bo or 0:>10,}{bn or 0:>10,}{to:>7.1f}s{tn:>7.1f}s  {'SI' if ok else 'NO'}")

print("-" * 106)
print(f"Tiempo total v3.2 (secuencial):      {t_old:6.1f}s  | promedio por busqueda {t_old/len(casos):5.1f}s | polls prom {sum(polls_old)/max(len(polls_old),1):.1f}")
print(f"Tiempo total v3.3 ({PARALELO_NUEVO} en paralelo):   {t_new:6.1f}s  | promedio por busqueda {sum(t for _,t in res_new)/len(casos):5.1f}s | polls prom {sum(polls_new)/max(len(polls_new),1):.1f}")
print(f"Aceleracion total: {t_old/t_new:.1f}x")
print(f"Diferencia de precio: max {max(difs) if difs else 0:,} MXN | promedio {sum(difs)/len(difs) if difs else 0:,.0f} MXN | fuera de tolerancia: {fallos}/{len(casos)}")
print("\nRESULTADO:", "EXACTO (todas dentro de tolerancia)" if fallos == 0 else f"REVISAR: {fallos} fuera de tolerancia")
sys.exit(0 if fallos == 0 else 1)
